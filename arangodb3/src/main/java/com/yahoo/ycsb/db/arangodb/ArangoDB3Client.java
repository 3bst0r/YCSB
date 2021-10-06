/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db.arangodb;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.DocumentUpdateOptions;
import com.arangodb.model.TransactionOptions;
import com.arangodb.util.MapBuilder;
import com.arangodb.velocypack.VPackBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.ValueType;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.soe.Generator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ArangoDB binding for YCSB framework using the ArangoDB Inc. <a
 * href="https://github.com/arangodb/arangodb-java-driver">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 *
 * @see <a href="https://github.com/arangodb/arangodb-java-driver">ArangoDB Inc.
 * driver</a>
 */
public class ArangoDB3Client extends DB {

  private static Logger logger = LoggerFactory.getLogger(ArangoDB3Client.class);

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** ArangoDB Driver related, Singleton. */
  private ArangoDB arangoDB;
  private String databaseName = "ycsb";
  private String collectionName;
  private Boolean dropDBBeforeRun;
  private Boolean waitForSync = false;
  private Boolean transactionUpdate = false;

  // Jackson ObjectMapper
  private ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   *
   * Actually, one client process will share one DB instance here.(Coincide to
   * mongoDB driver)
   */
  @Override
  public void init() throws DBException {
    synchronized (ArangoDB3Client.class) {
      Properties props = getProperties();

      collectionName = props.getProperty("table", "usertable");

      // Set the DB address
      String ip = props.getProperty("arangodb.ip", "localhost");
      String portStr = props.getProperty("arangodb.port", "8529");
      int port = Integer.parseInt(portStr);

      // If clear db before run
      String dropDBBeforeRunStr = props.getProperty("arangodb.dropDBBeforeRun", "false");
      dropDBBeforeRun = Boolean.parseBoolean(dropDBBeforeRunStr);

      // Set the sync mode
      String waitForSyncStr = props.getProperty("arangodb.waitForSync", "false");
      waitForSync = Boolean.parseBoolean(waitForSyncStr);

      // Set if transaction for update
      String transactionUpdateStr = props.getProperty("arangodb.transactionUpdate", "false");
      transactionUpdate = Boolean.parseBoolean(transactionUpdateStr);

      // Init ArangoDB connection
      try {
        arangoDB = new ArangoDB.Builder().host(ip).port(port).build();
      } catch (Exception e) {
        logger.error("Failed to initialize ArangoDB", e);
        System.exit(-1);
      }

      if (INIT_COUNT.getAndIncrement() == 0) {
        // Init the database
        if (dropDBBeforeRun) {
          // Try delete first
          try {
            arangoDB.db(databaseName).drop();
          } catch (ArangoDBException e) {
            logger.info("Fail to delete DB: {}", databaseName);
          }
        }
        try {
          arangoDB.createDatabase(databaseName);
          logger.info("Database created: " + databaseName);
        } catch (ArangoDBException e) {
          logger.error("Failed to create database: {} with ex: {}", databaseName, e.toString());
        }
        try {
          arangoDB.db(databaseName).createCollection(collectionName);
          logger.info("Collection created: " + collectionName);
        } catch (ArangoDBException e) {
          logger.error("Failed to create collection: {} with ex: {}", collectionName, e.toString());
        }
        logger.info("ArangoDB client connection created to {}:{}", ip, port);

        // Log the configuration
        logger.info("Arango Configuration: dropDBBeforeRun: {}; address: {}:{}; databaseName: {};"
                + " waitForSync: {}; transactionUpdate: {};",
            dropDBBeforeRun, ip, port, databaseName, waitForSync, transactionUpdate);
      }
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   *
   * Actually, one client process will share one DB instance here.(Coincide to
   * mongoDB driver)
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      arangoDB.shutdown();
      arangoDB = null;
      logger.info("Local cleaned up.");
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to insert.
   * @param values
   *      A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      BaseDocument toInsert = new BaseDocument(key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.addAttribute(entry.getKey(), byteIteratorToString(entry.getValue()));
      }
      DocumentCreateOptions options = new DocumentCreateOptions().waitForSync(waitForSync);
      arangoDB.db(databaseName).collection(table).insertDocument(toInsert, options);
      return Status.OK;
    } catch (ArangoDBException e) {
      logger.error("Exception while trying insert {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to read.
   * @param fields
   *      The list of fields to read, or null for all of them
   * @param result
   *      A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      VPackSlice document = arangoDB.db(databaseName).collection(table).getDocument(key, VPackSlice.class, null);
      if (!this.fillMap(result, document, fields)) {
        return Status.ERROR;
      }
      return Status.OK;
    } catch (ArangoDBException e) {
      logger.error("Exception while trying read {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to write.
   * @param values
   *      A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *     description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      if (!transactionUpdate) {
        BaseDocument updateDoc = new BaseDocument();
        for (Entry<String, ByteIterator> field : values.entrySet()) {
          updateDoc.addAttribute(field.getKey(), byteIteratorToString(field.getValue()));
        }
        arangoDB.db(databaseName).collection(table).updateDocument(key, updateDoc);
        return Status.OK;
      } else {
        // id for documentHandle
        String transactionAction = "function (id) {"
               // use internal database functions
            + "var db = require('internal').db;"
              // collection.update(document, data, overwrite, keepNull, waitForSync)
            + String.format("db._update(id, %s, true, false, %s);}",
                mapToJson(values), Boolean.toString(waitForSync).toLowerCase());
        TransactionOptions options = new TransactionOptions();
        options.writeCollections(table);
        options.params(createDocumentHandle(table, key));
        arangoDB.db(databaseName).transaction(transactionAction, Void.class, options);
        return Status.OK;
      }
    } catch (ArangoDBException e) {
      logger.error("Exception while trying update {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *      The name of the table
   * @param key
   *      The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      arangoDB.db(databaseName).collection(table).deleteDocument(key);
      return Status.OK;
    } catch (ArangoDBException e) {
      logger.error("Exception while trying delete {} {} with ex {}", table, key, e.toString());
    }
    return Status.ERROR;
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table
   *      The name of the table
   * @param startkey
   *      The record key of the first record to read.
   * @param recordcount
   *      The number of records to read
   * @param fields
   *      The list of fields to read, or null for all of them
   * @param result
   *      A Vector of HashMaps, where each HashMap is a set field/value
   *      pairs for one record
   * @return Zero on success, a non-zero error code on error. See the
   *     {@link DB} class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    ArangoCursor<VPackSlice> cursor = null;
    try {
      String aqlQuery = String.format(
          "FOR target IN %s FILTER target._key >= @key SORT target._key ASC LIMIT %d RETURN %s ", table,
          recordcount, constructReturnForAQL(fields, "target"));

      Map<String, Object> bindVars = new MapBuilder().put("key", startkey).get();
      cursor = arangoDB.db(databaseName).query(aqlQuery, bindVars, null, VPackSlice.class);
      while (cursor.hasNext()) {
        VPackSlice aDocument = cursor.next();
        HashMap<String, ByteIterator> aMap = new HashMap<String, ByteIterator>(aDocument.size());
        if (!this.fillMap(aMap, aDocument)) {
          return Status.ERROR;
        }
        result.add(aMap);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Exception while trying scan {} {} {} with ex {}", table, startkey, recordcount, e.toString());
    } finally {
      if (cursor != null) {
        try {
          cursor.close();
        } catch (IOException e) {

          logger.error("Fail to close cursor", e);
        }
      }
    }
    return Status.ERROR;
  }


  /*
      =================== BEGIN SOE operations  ===================
  */

  @Override
  public Status soeLoad(String table, Generator generator) {
    try {

      // find random customer (why not one after the other?)
      String key = generator.getCustomerIdRandom();
      ArangoCollection collection = arangoDB.db(databaseName).collection(table);
      // supplying ObjectNode.class to getDocument would return null as result
      VPackSlice customerDoc = collection.getDocument(key, VPackSlice.class);
      if (customerDoc == null) {
        System.out.println("Empty return");
        return Status.OK;
      }

      // put customer document to generator
      generator.putCustomerDocument(key, customerDoc.toString());

      // get orders from doc
      Iterator<VPackSlice> ordersIt = customerDoc
          .get(Generator.SOE_FIELD_CUSTOMER_ORDER_LIST).arrayIterator();
      while (ordersIt.hasNext()) {
        String orderKey = ordersIt.next().getAsString();
        VPackSlice orderDoc = collection.getDocument(orderKey, VPackSlice.class);
        if (orderDoc == null) {
          return Status.ERROR;
        }
        generator.putOrderDocument(orderKey, orderDoc.toString());
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status soeInsert(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      String key = gen.getPredicate().getDocid();
      String value = gen.getPredicate().getValueA();
      BaseDocument toInsert = new BaseDocument(key);
      TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {
      };
      Map<String, Object> properties = objectMapper.readValue(value, typeRef);
      // remove key and id to not confuse the db (generator doc id is different from ids in generator json doc)
      properties.remove("_key");
      properties.remove("_id");
      toInsert.setProperties(properties);
      DocumentCreateOptions options = new DocumentCreateOptions().waitForSync(waitForSync);
      arangoDB.db(databaseName).collection(table).insertDocument(toInsert, options);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status soeUpdate(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      String key = gen.getCustomerIdWithDistribution();
      String updateFieldName = gen.getPredicate().getNestedPredicateA().getName();
      String updateFieldValue = gen.getPredicate().getNestedPredicateA().getValueA();

      BaseDocument toInsert = new BaseDocument(key);
      Map<String, Object> properties = new HashMap<>();
      properties.put(updateFieldName, updateFieldValue);
      toInsert.setProperties(properties);
      DocumentUpdateOptions options = new DocumentUpdateOptions().waitForSync(waitForSync);
      arangoDB.db(databaseName).collection(table).updateDocument(key, toInsert, options);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status soeRead(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      String key = gen.getCustomerIdWithDistribution();
      VPackSlice queryResult = arangoDB.db(databaseName).collection(table).getDocument(key, VPackSlice.class, null);
      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status soeScan(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    String startkey = gen.getCustomerIdWithDistribution();
    int recordcount = gen.getRandomLimit();

    try {
      String aqlQuery = String.format(
          "FOR target IN %s FILTER target._key >= @key SORT target._key ASC LIMIT %d RETURN target ", table,
          recordcount);

      Map<String, Object> bindVars = new MapBuilder().put("key", startkey).get();
      return soeQueryAndFillMap(result, aqlQuery, bindVars);
    } catch (Exception e) {
      logger.error("Exception while trying scan {} {} {} with ex {}", table, startkey, recordcount, e.toString());
    }
    return Status.ERROR;
  }

  // TODO with the current setup there are mostly 0 results, because there would have to be at least 11 customers
  // with the same zip so that the query would return something. maybe solvable by larger data
  @Override
  public Status soePage(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    try {
      String aqlQuery = String.format(
          "FOR target IN %s " +
              "FILTER target.%s.%s == @val " +
              "LIMIT @offset, @limit " +
              "RETURN target ",
          table,
          gen.getPredicate().getName(),
          gen.getPredicate().getNestedPredicateA().getName());

      Map<String, Object> bindVars = new MapBuilder()
          .put("val", '"' + gen.getPredicate().getNestedPredicateA().getValueA() + '"')
          .put("offset", offset)
          .put("limit", recordcount)
          .get();

      return soeQueryAndFillMap(result, aqlQuery, bindVars);
    } catch (Exception e) {
      logger.error("Exception while trying page {} {} {} with ex {}", table,
          gen.getPredicate().getNestedPredicateA().getValueA(), recordcount, e.toString());
    }
    return Status.ERROR;
  }

  // TODO with the current setup there are mostly 0 results, because there would have to be at least 11 customers
  // with the same zip so that the query would return something. maybe solvable by larger data
  @Override
  public Status soeSearch(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    try {
      final String predicate1Name = gen.getPredicatesSequence().get(0).getName() + "." +
          gen.getPredicatesSequence().get(0).getNestedPredicateA().getName();
      String predicate1Val = gen.getPredicatesSequence().get(0).getNestedPredicateA().getValueA();
      final String predicate2Name = gen.getPredicatesSequence().get(1).getName();
      final String predicate2Val = gen.getPredicatesSequence().get(1).getValueA();
      final String predicate3Name = gen.getPredicatesSequence().get(2).getName();
      final String predicate3Val = gen.getPredicatesSequence().get(2).getValueA();

      String aqlQuery = String.format("FOR target IN %s " +
              "FILTER target.%s == @val1 " +
              "AND target.%s == @val2 " +
              "AND DATE_YEAR(target.%s) == @val3 " +
              "SORT target.%s " +
              "LIMIT @offset, @limit " +
              "RETURN target",
          table,
          predicate1Name,
          predicate2Name,
          predicate3Name,
          predicate1Name
      );
      Map<String, Object> bindVars = new MapBuilder()
          .put("val1", predicate1Val)
          .put("val2", predicate2Val)
          .put("val3", predicate3Val)
          .put("offset", offset)
          .put("limit", recordcount)
          .get();

      return soeQueryAndFillMap(result, aqlQuery, bindVars);
    } catch (Exception e) {
      logger.error("Exception while trying page {} {} {} with ex {}", table,
          gen.getPredicate().getNestedPredicateA().getValueA(), recordcount, e.toString());
    }
    return Status.ERROR;
  }

  @Override
  public Status soeNestScan(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    try {
      final String predicateName = gen.getPredicate().getName() + '.'
          + gen.getPredicate().getNestedPredicateA().getName() + '.'
          + gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getName();
      final String predicateVal = gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getValueA();

      String aqlQuery = String.format("FOR target IN %s " +
              "FILTER target.%s == @val " +
              "LIMIT @limit " +
              "RETURN target",
          table,
          predicateName
      );
      Map<String, Object> bindVars = new MapBuilder()
          .put("val", predicateVal)
          .put("limit", recordcount)
          .get();

      return soeQueryAndFillMap(result, aqlQuery, bindVars);
    } catch (Exception e) {
      logger.error("Exception while trying page {} {} {} with ex {}", table,
          gen.getPredicate().getNestedPredicateA().getValueA(), recordcount, e.toString());
    }
    return Status.ERROR;
  }

  @Override
  public Status soeArrayScan(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    try {
      final String predicateName = gen.getPredicate().getName();
      final String predicateVal = gen.getPredicate().getValueA();

      String aqlQuery = String.format("FOR target IN %s " +
              "FILTER @val IN target.%s " +
              "LIMIT @limit " +
              "RETURN target",
          table,
          predicateName
      );
      Map<String, Object> bindVars = new MapBuilder()
          .put("val", predicateVal)
          .put("limit", recordcount)
          .get();

      return soeQueryAndFillMap(result, aqlQuery, bindVars);
    } catch (Exception e) {
      logger.error("Exception while trying page {} {} {} with ex {}", table,
          gen.getPredicate().getNestedPredicateA().getValueA(), recordcount, e.toString());
    }
    return Status.ERROR;
  }

  @Override
  public Status soeArrayDeepScan(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    try {
      final String visitedPlacesFieldName = gen.getPredicate().getName();
      final String countryFieldName = gen.getPredicate().getNestedPredicateA().getName();
      final String cityFieldName = gen.getPredicate().getNestedPredicateB().getName();

      final String countryValue = gen.getPredicate().getNestedPredicateA().getValueA();
      final String cityValue = gen.getPredicate().getNestedPredicateB().getValueA();

      final String aqlQuery = String.format("FOR target IN %s " +
              "FILTER [] != target.%s[* FILTER CURRENT.%s == @country AND CONTAINS(CURRENT.%s, @city)] " +
              "LIMIT @limit " +
              "RETURN target",
          table,
          visitedPlacesFieldName,
          countryFieldName,
          cityFieldName
      );
      Map<String, Object> bindVars = new MapBuilder()
          .put("country", countryValue)
          .put("city", cityValue)
          .put("limit", recordcount)
          .get();

      return soeQueryAndFillMap(result, aqlQuery, bindVars);
    } catch (Exception e) {
      logger.error("Exception while trying page {} {} {} with ex {}", table,
          gen.getPredicate().getNestedPredicateA().getValueA(), recordcount, e.toString());
    }
    return Status.ERROR;
  }

  @Override
  public Status soeReport(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      final String orderList = gen.getPredicatesSequence().get(0).getName();
      final String addressZip = gen.getPredicatesSequence().get(1).getName() + '.'
          + gen.getPredicatesSequence().get(1).getNestedPredicateA().getName();
      final String addressZipValue = gen.getPredicatesSequence().get(1).getNestedPredicateA().getValueA();

      final String aqlQuery = String.format(
          "FOR c1 in %s " +
              "FILTER c1.%s == @zip " +
              "FOR o2 IN %s " +
              "   FILTER o2._key IN c1.%s " +
              "   RETURN {c1, o2} ",
          table,
          addressZip,
          table,
          orderList
      );
      Map<String, Object> bindVars = new MapBuilder()

          .put("zip", addressZipValue)
          .get();

      return soeQueryAndFillMap(result, aqlQuery, bindVars);
    } catch (Exception e) {
      logger.error("Exception while trying page {} {} with ex {}", table,
          gen.getPredicate().getNestedPredicateA().getValueA(),  e.toString());
    }
    return Status.ERROR;
  }

  private Status soeQueryAndFillMap(Vector<HashMap<String, ByteIterator>> result,
                                    String aqlQuery,
                                    Map<String, Object> bindVars) {
    ArangoCursor<VPackSlice> cursor = arangoDB.db(databaseName).query(aqlQuery, bindVars, null, VPackSlice.class);
    while (cursor.hasNext()) {
      VPackSlice aDocument = cursor.next();
      HashMap<String, ByteIterator> aMap = new HashMap<>(aDocument.size());
      if (!this.soeFillMap(aMap, aDocument)) {
        return Status.ERROR;
      }
      result.add(aMap);
    }
    return Status.OK;
  }

  /*
      ===================  END SOE operations  ===================
  */


  private String createDocumentHandle(String collection, String documentKey) throws ArangoDBException {
    validateCollectionName(collection);
    return collection + "/" + documentKey;
  }

  private void validateCollectionName(String name) throws ArangoDBException {
    if (name.indexOf('/') != -1) {
      throw new ArangoDBException("does not allow '/' in name.");
    }
  }


  private String constructReturnForAQL(Set<String> fields, String targetName) {
    // Construct the AQL query string.
    String resultDes = targetName;
    if (fields != null && fields.size() != 0) {
      StringBuilder builder = new StringBuilder("{");
      for (String field : fields) {
        builder.append(String.format("\n\"%s\" : %s.%s,", field, targetName, field));
      }
      //Replace last ',' to newline.
      builder.setCharAt(builder.length() - 1, '\n');
      builder.append("}");
      resultDes = builder.toString();
    }
    return resultDes;
  }

  private boolean fillMap(Map<String, ByteIterator> resultMap, VPackSlice document) {
    return fillMap(resultMap, document, null);
  }

  /**
   * Fills the map with the properties from the BaseDocument.
   *
   * @param resultMap
   *      The map to fill/
   * @param document
   *      The record to read from
   * @param fields
   *      The list of fields to read, or null for all of them
   * @return isSuccess
   */
  private boolean fillMap(Map<String, ByteIterator> resultMap, VPackSlice document, Set<String> fields) {
    if (fields == null || fields.size() == 0) {
      Iterator<Entry<String, VPackSlice>> iterator = document.objectIterator();
      while (iterator.hasNext()) {
        Entry<String, VPackSlice> next = iterator.next();
        VPackSlice value = next.getValue();
        if (value.isString()) {
          resultMap.put(next.getKey(), stringToByteIterator(value.getAsString()));
        } else if (!value.isCustom()) {
          logger.error("Error! Not the format expected! Actually is {}",
              value.getClass().getName());
          return false;
        }
      }
    } else {
      for (String field : fields) {
        VPackSlice value = document.get(field);
        if (value.isString()) {
          resultMap.put(field, stringToByteIterator(value.getAsString()));
        } else if (!value.isCustom()) {
          logger.error("Error! Not the format expected! Actually is {}",
              value.getClass().getName());
          return false;
        }
      }
    }
    return true;
  }

  private boolean soeFillMap(Map<String, ByteIterator> resultMap, VPackSlice document) {
    Iterator<Entry<String, VPackSlice>> iterator = document.objectIterator();
    while (iterator.hasNext()) {
      Entry<String, VPackSlice> next = iterator.next();
      resultMap.put(next.getKey(), stringToByteIterator(next.toString()));
    }
    return true;
  }

  private String byteIteratorToString(ByteIterator byteIter) {
    return new String(byteIter.toArray());
  }

  private ByteIterator stringToByteIterator(String content) {
    return new StringByteIterator(content);
  }

  private String mapToJson(HashMap<String, ByteIterator> values) {
    VPackBuilder builder = new VPackBuilder().add(ValueType.OBJECT);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      builder.add(entry.getKey(), byteIteratorToString(entry.getValue()));
    }
    builder.close();
    return arangoDB.util().deserialize(builder.slice(), String.class);
  }

}

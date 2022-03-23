/*
 * Copyright 2017 YCSB Contributors. All Rights Reserved.
 *
 * CODE IS BASED ON the jdbc-binding JdbcDBClient class.
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
package com.yahoo.ycsb.db.postgrenosql;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.soe.Generator;
import com.yahoo.ycsb.workloads.soe.SoeQueryPredicate;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.yahoo.ycsb.db.postgrenosql.StatementType.Type.*;
import static java.lang.String.format;

// TODO there is a bug in one of the writing operations,
// storing {"ballance_current": {"ballance_current": ...}}

/**
 * PostgreNoSQL client for YCSB-JSON framework.
 */
public class PostgreNoSQLDBClient extends PostgreNoSQLBaseClient {

  public static final String JSONB = "jsonb";
  // Jackson ObjectMapper
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public void init() throws DBException {
    super.init();
  }

  @Override
  public Status soeLoad(String table, Generator generator) {
    try {
      StatementType type = new StatementType(SOE_LOAD, table, null);
      PreparedStatement soeLoadStatement = cachedStatements.get(type);
      if (soeLoadStatement == null) {
        soeLoadStatement = createAndCacheSoeLoadStatement(type);
      }

      // find random customer
      final String customerId = generator.getCustomerIdRandom();
      soeLoadStatement.setString(1, customerId);

      final Optional<String> customerDocOpt = getFirstColumnFromFirstRowAsString(soeLoadStatement);
      if (!customerDocOpt.isPresent()) {
        return Status.ERROR;
      }

      // store customer in generator
      final String customerDoc = customerDocOpt.get();
      generator.putCustomerDocument(customerId, customerDoc);

      // get orders from customer
      final JsonNode customerDocJson = MAPPER.readTree(customerDoc);
      for (JsonNode jsonNode : customerDocJson
          .get(Generator.SOE_FIELD_CUSTOMER_ORDER_LIST)) {
        String orderId = jsonNode.textValue();
        soeLoadStatement.setString(1, orderId);
        final Optional<String> orderDocOpt = getFirstColumnFromFirstRowAsString(soeLoadStatement);
        if (!orderDocOpt.isPresent()) {
          return Status.ERROR;
        }
        final String orderDoc = orderDocOpt.get();
        generator.putOrderDocument(orderId, orderDoc);
      }
      return Status.OK;

    } catch (SQLException e) {
      LOG.error("Error in processing soeLoad of table " + table + ": " + e);
      return Status.ERROR;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status soeInsert(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      StatementType type = new StatementType(SOE_INSERT, table, null);
      PreparedStatement soeInsertStatement = cachedStatements.get(type);
      if (soeInsertStatement == null) {
        soeInsertStatement = createAndCacheSoeInsertStatement(type);
      }
      String key = gen.getPredicate().getDocid();
      String value = gen.getPredicate().getValueA();
      PGobject object = new PGobject();
      object.setType(JSONB);
      object.setValue(value);

      soeInsertStatement.setString(1, key);
      soeInsertStatement.setObject(2, object);

      int sqlStatus = soeInsertStatement.executeUpdate();
      if (sqlStatus == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing insert to table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status soeUpdate(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      StatementType type = new StatementType(SOE_UPDATE, table, null);
      PreparedStatement soeUpdateStatement = cachedStatements.get(type);
      if (soeUpdateStatement == null) {
        soeUpdateStatement = createAndCacheSoeUpdateStatement(type, gen);
      }
      String key = gen.getPredicate().getNestedPredicateA().getName();
      String value = getPredicateValue(gen.getPredicate(), 1);
      ObjectNode newValue = MAPPER.createObjectNode();
      newValue.put(key, value);
      PGobject object = new PGobject();
      object.setType(JSONB);
      object.setValue(newValue.toString());

      soeUpdateStatement.setObject(1, object);
      final String id = gen.getCustomerIdWithDistribution();
      soeUpdateStatement.setString(2, id);

      int sqlStatus = soeUpdateStatement.executeUpdate();
      if (sqlStatus == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing update to table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status soeRead(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      StatementType type = new StatementType(SOE_READ, table, gen.getAllFields());
      PreparedStatement soeReadStatement = cachedStatements.get(type);
      if (soeReadStatement == null) {
        soeReadStatement = createAndCacheSoeReadStatement(type);
      }
      String key = gen.getCustomerIdWithDistribution();

      soeReadStatement.setString(1, key);

      return executeQuery(result, gen, soeReadStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing read to table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status soeSearch(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      StatementType type = new StatementType(SOE_SEARCH, table, gen.getAllFields());
      PreparedStatement soeSearchStatement = cachedStatements.get(type);
      if (soeSearchStatement == null) {
        soeSearchStatement = createAndCacheSoeSearchStatement(type, gen);
      }

      final String countryVal = getPredicateValue(gen.getPredicatesSequence().get(0), 1);
      soeSearchStatement.setString(1, countryVal);
      final String ageGroupVal = gen.getPredicatesSequence().get(1).getValueA();
      soeSearchStatement.setString(2, ageGroupVal);
      final String dateOfBirthVal = gen.getPredicatesSequence().get(2).getValueA();
      soeSearchStatement.setString(3, dateOfBirthVal);
      soeSearchStatement.setInt(4, gen.getRandomOffset());
      soeSearchStatement.setInt(5, gen.getRandomLimit());

      return executeQuery(result, gen, soeSearchStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing soe search in table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status soeScan(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      String startkey = gen.getCustomerIdWithDistribution();
      int recordcount = gen.getRandomLimit();
      result.ensureCapacity(recordcount);
      StatementType type = new StatementType(SOE_SCAN, table, gen.getAllFields());
      PreparedStatement soeScanStatement = cachedStatements.get(type);
      if (soeScanStatement == null) {
        soeScanStatement = createAndCacheSoeScanStatement(type);
      }

      soeScanStatement.setString(1, startkey);
      soeScanStatement.setString(2, PRIMARY_KEY);
      soeScanStatement.setInt(3, recordcount);

      return executeQuery(result, gen, soeScanStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing soe scan in table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status soeArrayScan(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      int recordcount = gen.getRandomLimit();
      result.ensureCapacity(recordcount);
      StatementType type = new StatementType(SOE_ARRAY_SCAN, table, gen.getAllFields());
      PreparedStatement soeArrayScanStatement = cachedStatements.get(type);
      if (soeArrayScanStatement == null) {
        soeArrayScanStatement = createAndCacheSoeArrayScanStatement(type, gen);
      }

      PGobject devicesValue = new PGobject();
      devicesValue.setType(JSONB);
      devicesValue.setValue('"' + gen.getPredicate().getValueA() + '"');
      soeArrayScanStatement.setObject(1, devicesValue);
      soeArrayScanStatement.setInt(2, recordcount);

      return executeQuery(result, gen, soeArrayScanStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing soe array scan in table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status soeNestScan(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      int recordcount = gen.getRandomLimit();
      result.ensureCapacity(recordcount);
      StatementType type = new StatementType(SOE_NEST_SCAN, table, gen.getAllFields());
      PreparedStatement soeNestScanStatement = cachedStatements.get(type);
      if (soeNestScanStatement == null) {
        soeNestScanStatement = createAndCacheSoeNestScanStatement(type, gen);
      }

      String nestedFieldValue = getPredicateValue(gen.getPredicate(), 2);
      soeNestScanStatement.setString(1, nestedFieldValue);
      soeNestScanStatement.setInt(2, recordcount);

      return executeQuery(result, gen, soeNestScanStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing soe nest scan in table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  // TODO with the current setup there are mostly 0 results, because there would have to be at least 11 customers
  // with the same zip so that the query would return something. maybe solvable by larger data
  @Override
  public Status soePage(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      int recordcount = gen.getRandomLimit();
      result.ensureCapacity(recordcount);
      int offset = gen.getRandomOffset();
      StatementType type = new StatementType(SOE_PAGE, table, gen.getAllFields());
      PreparedStatement soePageStatement = cachedStatements.get(type);
      if (soePageStatement == null) {
        soePageStatement = createAndCacheSoePageStatement(type, gen);
      }

      String nestedFieldValue = getPredicateValue(gen.getPredicate(), 1);
      soePageStatement.setString(1, nestedFieldValue);
      soePageStatement.setInt(2, recordcount);
      soePageStatement.setInt(3, offset);

      return executeQuery(result, gen, soePageStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing soe page in table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  // TODO predicates for country and city are sometimes null
  @Override
  public Status soeArrayDeepScan(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      int recordcount = gen.getRandomLimit();
      result.ensureCapacity(recordcount);
      StatementType type = new StatementType(SOE_ARRAY_DEEP_SCAN, table, gen.getAllFields());
      PreparedStatement preparedStatement = cachedStatements.get(type);
      if (preparedStatement == null) {
        preparedStatement = createAndCacheSoeArrayDeepScanStatement(type, gen);
      }

      preparedStatement.setString(1, gen.getPredicate().getNestedPredicateA().getValueA());
      preparedStatement.setString(2, gen.getPredicate().getNestedPredicateB().getValueA());
      preparedStatement.setInt(3, recordcount);

      return executeQuery(result, gen, preparedStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing soe array deep scan in table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  // TODO handle null values for predicate...empty list or parse null correctly? query returns no results for
  //  null values currently...
  @Override
  public Status soeLiteralArray(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      int recordcount = gen.getRandomLimit();
      result.ensureCapacity(recordcount);
      StatementType type = new StatementType(SOE_LITERAL_ARRAY, table, gen.getAllFields());
      PreparedStatement preparedStatement = cachedStatements.get(type);
      if (preparedStatement == null) {
        preparedStatement = createAndCacheLiteralArrayStatement(type, gen);
      }

      preparedStatement.setString(1, gen.getPredicate().getValueA());
      preparedStatement.setInt(2, recordcount);

      return executeQuery(result, gen, preparedStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing soe literal array in table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  // TODO uses indexes but still a sequence scan, so it's quite slow. maybe needs a different join syntax
  @Override
  public Status soeReport(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      int recordcount = gen.getRandomLimit();
      result.ensureCapacity(recordcount);
      StatementType type = new StatementType(SOE_REPORT, table, gen.getAllFields());
      PreparedStatement preparedStatement = cachedStatements.get(type);
      if (preparedStatement == null) {
        preparedStatement = createAndCacheSoeReportStatement(type, gen);
      }

      final String addressZipValue = getPredicateValue(gen.getPredicatesSequence().get(1), 1);
      preparedStatement.setString(1, addressZipValue);

      return executeQuery(result, gen, preparedStatement);
    } catch (SQLException | JsonProcessingException e) {
      LOG.error("Error in processing soe search in table: " + table + ": " + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createAndCacheSoeReportStatement(StatementType type, Generator gen)
      throws SQLException {
    PreparedStatement soeReportStatement = connection.prepareStatement(createSoeReportStatement(type, gen));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, soeReportStatement);
    if (statement == null) {
      return soeReportStatement;
    }
    return statement;
  }

  private String createSoeReportStatement(StatementType type, Generator gen) {
    final String orderList = gen.getPredicatesSequence().get(0).getName();
    final SoeQueryPredicate addressZip = gen.getPredicatesSequence().get(1);
    final String address = addressZip.getName();
    final String zip = addressZip.getNestedPredicateA().getName();
    return format("SELECT jsonb_build_object('c', c.%s, 'o', o.%s) ", COLUMN_NAME, COLUMN_NAME) +
        format(" FROM %s c ", type.getTableName()) +
        format(" JOIN %s o ON ", type.getTableName()) +
        format("c.%s->'%s' @> to_jsonb(o.%s) ", COLUMN_NAME, orderList, PRIMARY_KEY) +
        format("WHERE c.%s->'%s'->>'%s' = ?", COLUMN_NAME, address, zip); // param 1
  }

  private PreparedStatement createAndCacheLiteralArrayStatement(StatementType type, Generator gen)
      throws SQLException {
    PreparedStatement literalArrayStatement = connection.prepareStatement(createLiteralArrayStatement(type, gen));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, literalArrayStatement);
    if (statement == null) {
      return literalArrayStatement;
    }
    return statement;
  }

  private String createLiteralArrayStatement(StatementType type, Generator gen) {
    return selectJsonColumnFromTable(type) +
        // cast supplied array to jsonb and then to text to normalize its representation and thus enable exact matching
        format(" WHERE %s->>'%s' = ?::jsonb::text ", COLUMN_NAME, gen.getPredicate().getName()) + // param 1
        " ORDER BY " + PRIMARY_KEY +
        " LIMIT ? "; // param 2
  }

  private PreparedStatement createAndCacheSoeArrayDeepScanStatement(StatementType type, Generator gen)
      throws SQLException {
    PreparedStatement arrayDeepScanStatement = connection.prepareStatement(createSoeArrayDeepScanStatement(type, gen));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, arrayDeepScanStatement);
    if (statement == null) {
      return arrayDeepScanStatement;
    }
    return statement;
  }

  private String createSoeArrayDeepScanStatement(StatementType type, Generator gen) {
    final SoeQueryPredicate predicate = gen.getPredicate();
    final String visitedPlaces = predicate.getName();
    final String country = predicate.getNestedPredicateA().getName();
    final String cities = predicate.getNestedPredicateB().getName();

    return selectJsonColumnFromTable(type) +
        format(" WHERE %s->'%s' @> ", COLUMN_NAME, visitedPlaces) +
        format(" jsonb_build_array(" +
            "     jsonb_build_object(" +
            "       '%s', ?, " +                    // param 1
            "       '%s', jsonb_build_array(?)" +   // param 2
            "     )" +
            "    )", country, cities) +
        " ORDER BY " + PRIMARY_KEY +
        " LIMIT ? "; // param 3
  }

  private PreparedStatement createAndCacheSoePageStatement(StatementType type, Generator gen) throws SQLException {
    PreparedStatement soePageStatement = connection.prepareStatement(createSoePageStatement(type, gen));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, soePageStatement);
    if (statement == null) {
      return soePageStatement;
    }
    return statement;
  }

  private String createSoePageStatement(StatementType type, Generator gen) {
    final String pathPredicateAsTextInArrowSyntax = getPathPredicateAsTextInArrowSyntax(gen.getPredicate(), 1);
    return selectJsonColumnFromTable(type) +
        format(" WHERE " + COLUMN_NAME + " %s = ? ", pathPredicateAsTextInArrowSyntax) + // param 1
        format(" ORDER BY " + COLUMN_NAME + "%s ", pathPredicateAsTextInArrowSyntax) +
        " LIMIT ? OFFSET ? "; // param 2 // param 3
  }

  private String getPathPredicateAsTextInArrowSyntax(SoeQueryPredicate predicate, int nestingLevel) {
    return getPathPredicateInArrowSyntax(predicate, nestingLevel, true);
  }

  private String getPathPredicateAsJsonbInArrowSyntax(SoeQueryPredicate predicate, int nestingLevel) {
    return getPathPredicateInArrowSyntax(predicate, nestingLevel, false);
  }


  private String getPathPredicateInArrowSyntax(SoeQueryPredicate predicate, int nestingLevel, boolean asText) {
    if (nestingLevel == 0) {
      return asText ? "->>" : "->" + predicate.getName();
    }
    StringBuilder path = new StringBuilder("->" + enquote(predicate.getName()));
    predicate = predicate.getNestedPredicateA();
    for (int i = 0; i < nestingLevel; i++) {
      if (asText && i == nestingLevel - 1) {
        path.append("->>");
      } else {
        path.append("->");
      }
      path.append(enquote(predicate.getName()));
      predicate = predicate.getNestedPredicateA();
    }
    return path.toString();
  }


  private PreparedStatement createAndCacheSoeNestScanStatement(StatementType type, Generator gen)
      throws SQLException {
    PreparedStatement nestScanStatement = connection.prepareStatement(createSoeNestScanStatement(type, gen));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, nestScanStatement);
    if (statement == null) {
      return nestScanStatement;
    }
    return statement;
  }

  private String createSoeNestScanStatement(StatementType type, Generator gen) {
    final String pathPredicateAsTextInArrowSyntax = getPathPredicateAsTextInArrowSyntax(gen.getPredicate(), 2);
    return selectJsonColumnFromTable(type) +
        " WHERE " + COLUMN_NAME + pathPredicateAsTextInArrowSyntax + " = ? " + // param 1
        " LIMIT ?"; // param 2
  }

  private PreparedStatement createAndCacheSoeArrayScanStatement(StatementType type, Generator gen) throws SQLException {
    PreparedStatement scanStatement = connection.prepareStatement(createSoeArrayScanStatement(type, gen));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, scanStatement);
    if (statement == null) {
      return scanStatement;
    }
    return statement;
  }

  private String createSoeArrayScanStatement(StatementType type, Generator gen) {
    return selectJsonColumnFromTable(type) +
        " WHERE " + COLUMN_NAME + "->" + enquote(gen.getPredicate().getName()) +
        " @> ?" +
        " ORDER BY " + PRIMARY_KEY +
        " LIMIT ?";
  }

  private PreparedStatement createAndCacheSoeScanStatement(StatementType type) throws SQLException {
    PreparedStatement scanStatement = connection.prepareStatement(createSoeScanStatement(type));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, scanStatement);
    if (statement == null) {
      return scanStatement;
    }
    return statement;
  }

  private String createSoeScanStatement(StatementType type) {
    return selectJsonColumnFromTable(type) +
        " WHERE " + PRIMARY_KEY + " >= ? " +
        "ORDER BY ? " +
        "LIMIT ?";
  }

  private PreparedStatement createAndCacheSoeSearchStatement(StatementType type, Generator gen) throws SQLException {
    PreparedStatement readStatement = connection.prepareStatement(createSoeSearchStatement(type, gen));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, readStatement);
    if (statement == null) {
      return readStatement;
    }
    return statement;
  }

  private String createSoeSearchStatement(StatementType type, Generator gen) {
    String address = gen.getPredicatesSequence().get(0).getName();
    address = enquote(address);
    String country = gen.getPredicatesSequence().get(0).getNestedPredicateA().getName();
    country = enquote(country);
    String ageGroup = gen.getPredicatesSequence().get(1).getName();
    ageGroup = enquote(ageGroup);
    String dateOfBirth = gen.getPredicatesSequence().get(2).getName();
    dateOfBirth = enquote(dateOfBirth);
    return selectJsonColumnFromTable(type) +
        " WHERE " +
        COLUMN_NAME + "->" + address + "->>" + country + " = " + "? " + // param 1
        " AND " +
        COLUMN_NAME + "->>" + ageGroup + " = ?" + // param 2
        " AND " +
        " date_trunc('year', date(" + COLUMN_NAME + "->>" + dateOfBirth + ")) = to_date(?, 'YYYY') " + // param 3
        " ORDER BY " + COLUMN_NAME + "->" + address + "->>" + country +
        " OFFSET ? LIMIT ?"; // param 4 // param 5
  }

  private PreparedStatement createAndCacheSoeUpdateStatement(StatementType type, Generator gen) throws SQLException {
    PreparedStatement updateStatement = connection.prepareStatement(createSoeUpdateStatement(type, gen));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, updateStatement);
    if (statement == null) {
      return updateStatement;
    }
    return statement;
  }

  private String createSoeUpdateStatement(StatementType type, Generator gen) {
    String updatePath = format("{%s}",
        gen.getPredicate().getNestedPredicateA().getName());
    return "UPDATE " + type.getTableName() + " " +
        " SET " + COLUMN_NAME + " = " +
        format("jsonb_set(%s, '%s', ?) ", COLUMN_NAME, updatePath) +
        "WHERE " + PRIMARY_KEY + " = ?";
  }

  private PreparedStatement createAndCacheSoeReadStatement(StatementType type) throws SQLException {
    PreparedStatement readStatement = connection.prepareStatement(createSoeReadStatement(type));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, readStatement);
    if (statement == null) {
      return readStatement;
    }
    return statement;
  }

  private String createSoeReadStatement(StatementType type) {
    return selectJsonColumnFromTable(type) +
        " WHERE " +
        PRIMARY_KEY +
        " = ?";
  }

  private PreparedStatement createAndCacheSoeInsertStatement(StatementType type) throws SQLException {
    PreparedStatement loadStatement = connection.prepareStatement(createInsertStatement(type));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, loadStatement);
    if (statement == null) {
      return loadStatement;
    }
    return statement;
  }

  private PreparedStatement createAndCacheSoeLoadStatement(StatementType type) throws SQLException {
    PreparedStatement loadStatement = connection.prepareStatement(createSoeLoadStatement(type));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, loadStatement);
    if (statement == null) {
      return loadStatement;
    }
    return statement;
  }

  private String createSoeLoadStatement(StatementType type) {
    return "SELECT " + COLUMN_NAME + " " +
        "FROM " + type.getTableName() + " " +
        "WHERE " + PRIMARY_KEY + " = ?";
  }

  private String selectJsonColumnFromTable(StatementType type) {
    return format("SELECT %s FROM %s ", COLUMN_NAME, type.getTableName());
  }

  /**
   * execute query that returns results as json objects.
   */
  private Status executeQuery(Vector<HashMap<String, ByteIterator>> result,
                              Generator gen,
                              PreparedStatement statement) throws SQLException, JsonProcessingException {
    try (ResultSet resultSet = statement.executeQuery()) {
      if (!resultSet.next()) {
        return Status.NOT_FOUND;
      }
      do {
        HashMap<String, ByteIterator> row = new HashMap<>(gen.getAllFields().size());
        soeDecode(row, resultSet);
        result.add(row);
      } while (resultSet.next());
      return Status.OK;
    }
  }

  /**
   * execute query that returns results as json objects.
   */
  private Status executeQuery(HashMap<String, ByteIterator> result,
                              Generator gen,
                              PreparedStatement statement) throws SQLException, JsonProcessingException {
    try (ResultSet resultSet = statement.executeQuery()) {
      if (!resultSet.next()) {
        return Status.NOT_FOUND;
      }

      soeDecode(result, resultSet);

      if (resultSet.next()) {
        LOG.error("Got more than one result for point query");
        return Status.UNEXPECTED_STATE;
      }
      return Status.OK;
    }
  }

  private void soeDecode(HashMap<String, ByteIterator> result, ResultSet resultSet)
      throws JsonProcessingException, SQLException {
    JsonNode jsonNode = MAPPER.readTree(resultSet.getString(1));
    Iterator<Map.Entry<String, JsonNode>> jsonFields = jsonNode.fields();
    while (jsonFields.hasNext()) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.toString()));
      }
    }
  }

  private Optional<String> getFirstColumnFromFirstRowAsString(PreparedStatement statement)
      throws SQLException {
    String columnAsString;
    try (ResultSet resultSet = statement.executeQuery()) {
      if (!resultSet.next()) {
        return Optional.empty();
      }
      columnAsString = resultSet.getString(1);
    }
    return Optional.of(columnAsString);
  }

  private static String enquote(String string) {
    return "'" + string + "'";
  }

  private String getPredicateValue(SoeQueryPredicate predicate, int nestingLevel) {
    for (int i = nestingLevel; i > 0; i--) {
      predicate = predicate.getNestedPredicateA();
    }
    return predicate.getValueA();
  }
}

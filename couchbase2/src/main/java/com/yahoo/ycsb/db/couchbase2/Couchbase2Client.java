package com.yahoo.ycsb.db.couchbase2;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.transcoder.JacksonTransformers;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.soe.Generator;
import com.yahoo.ycsb.workloads.soe.SoeQueryPredicate;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.*;

/**
 * A client that extends Couchbase2DB with YCSB-JSON operations.
 */
public class Couchbase2Client extends Couchbase2DB {

  private String soeQuerySelectIDClause;
  private String soeQuerySelectAllClause;
  private String soeScanN1qlQuery;
  private String soeScanKVQuery;
  private String soeInsertN1qlQuery;
  private String soeReadN1qlQuery;
  private Boolean isSOETest;

  @Override
  public void init() throws DBException {
    super.init();
    Properties props = getProperties();
    isSOETest = props.getProperty("couchbase.soe", "false").equals("true");

    soeQuerySelectIDClause = "SELECT RAW meta().id FROM";
    soeQuerySelectAllClause = "SELECT RAW `" + bucketName + "` FROM ";
    //soeQuerySelectAllClause = "SELECT * FROM ";

    soeReadN1qlQuery = soeQuerySelectAllClause + " `" + bucketName + "` USE KEYS [$1]";

    soeInsertN1qlQuery = "INSERT INTO `" + bucketName
        + "`(KEY,VALUE) VALUES ($1,$2)";

    soeScanN1qlQuery = soeQuerySelectAllClause + " `" + bucketName +
        "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";

    soeScanKVQuery = soeQuerySelectIDClause + " `" + bucketName +
        "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";
  }

  @Override
  public Status soeLoad(String table, Generator generator) {

    try {
      String docId = generator.getCustomerIdRandom();
      RawJsonDocument doc = bucket.get(docId, RawJsonDocument.class);
      if (doc != null) {
        generator.putCustomerDocument(docId, doc.content().toString());

        try {
          JsonNode json = JacksonTransformers.MAPPER.readTree(doc.content());
          Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields();
          while (jsonFields.hasNext()) {
            Map.Entry<String, JsonNode> jsonField = jsonFields.next();
            String name = jsonField.getKey();
            if (name.equals(Generator.SOE_FIELD_CUSTOMER_ORDER_LIST)) {
              JsonNode jsonValue = jsonField.getValue();
              ArrayList<String> orders = new ArrayList<>();
              for (final JsonNode objNode : jsonValue) {
                orders.add(objNode.asText());
              }
              if (orders.size() > 0) {
                String pickedOrder;
                if (orders.size() > 1) {
                  Collections.shuffle(orders);
                }
                pickedOrder = orders.get(0);
                RawJsonDocument orderDoc = bucket.get(pickedOrder, RawJsonDocument.class);
                generator.putOrderDocument(pickedOrder, orderDoc.content().toString());
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Could not decode JSON");
        }

      } else {
        System.err.println("Error getting document from DB: " + docId);
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }


  // *********************  SOE Insert ********************************

  @Override
  public Status soeInsert(String table, HashMap<String, ByteIterator> result, Generator gen) {

    try {
      //Pair<String, String> inserDocPair = gen.getInsertDocument();

      if (kv) {
        return soeInsertKv(gen);
      } else {
        return soeInsertN1ql(gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeInsertKv(Generator gen) {
    int tries = 60; // roughly 60 seconds with the 1 second sleep, not 100% accurate.
    for (int i = 0; i < tries; i++) {
      try {
        waitForMutationResponse(bucket.async().insert(
            RawJsonDocument.create(gen.getPredicate().getDocid(), documentExpiry, gen.getPredicate().getValueA()),
            persistTo,
            replicateTo
        ));
        return Status.OK;
      } catch (TemporaryFailureException ex) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while sleeping on TMPFAIL backoff.", ex);
        }
      }
    }
    throw new RuntimeException("Still receiving TMPFAIL from the server after trying " + tries + " times. " +
        "Check your server.");
  }

  private Status soeInsertN1ql(Generator gen)
      throws Exception {

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeInsertN1qlQuery,
        JsonArray.from(gen.getPredicate().getDocid(), JsonObject.fromJson(gen.getPredicate().getValueA())),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException("Error while parsing N1QL Result. Query: " + soeInsertN1qlQuery
          + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }


  // *********************  SOE Update ********************************

  @Override
  public Status soeUpdate(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      if (kv) {
        return soeUpdateKv(gen);
      } else {
        return soeUpdateN1ql(gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeUpdateKv(Generator gen) {

    waitForMutationResponse(bucket.async().replace(
        RawJsonDocument.create(gen.getCustomerIdWithDistribution(), documentExpiry, gen.getPredicate().getValueA()),
        persistTo,
        replicateTo
    ));

    return Status.OK;
  }

  private Status soeUpdateN1ql(Generator gen) {
    String updateQuery = "UPDATE `" + bucketName + "` USE KEYS [$1] SET " +
        gen.getPredicate().getNestedPredicateA().getName() + " = $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        updateQuery,
        JsonArray.from(gen.getCustomerIdWithDistribution(), gen.getPredicate().getNestedPredicateA().getValueA()),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      return Status.ERROR;
    }
    return Status.OK;
  }


  // *********************  SOE Read ********************************
  @Override
  public Status soeRead(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      if (kv) {
        return soeReadKv(result, gen);
      } else {
        return soeReadN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeReadKv(HashMap<String, ByteIterator> result, Generator gen)
      throws Exception {
    RawJsonDocument loaded = bucket.get(gen.getCustomerIdWithDistribution(), RawJsonDocument.class);
    if (loaded == null) {
      return Status.NOT_FOUND;
    }
    soeDecode(loaded.content(), null, result);
    return Status.OK;
  }

  private Status soeReadN1ql(HashMap<String, ByteIterator> result, Generator gen)
      throws Exception {
    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeReadN1qlQuery,
        JsonArray.from(gen.getCustomerIdWithDistribution()),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException("Error while parsing N1QL Result. Query: " + soeReadN1qlQuery
          + ", Errors: " + queryResult.errors());
    }

    N1qlQueryRow row;
    try {
      row = queryResult.rows().next();
    } catch (NoSuchElementException ex) {
      return Status.NOT_FOUND;
    }

    JsonObject content = row.value();
    Set<String> fields = gen.getAllFields();
    if (fields == null) {
      content = content.getObject(bucketName); // n1ql result set scoped under *.bucketName
      fields = content.getNames();
    }

    for (String field : fields) {
      Object value = content.get(field);
      result.put(field, new StringByteIterator(value != null ? value.toString() : ""));
    }
    return Status.OK;
  }


  // *********************  SOE Scan ********************************

  @Override
  public Status soeScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeScanKv(result, gen);
      } else {
        return soeScanN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeScanKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String key = gen.getCustomerIdWithDistribution();
    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    bucket.async()
        .query(N1qlQuery.parameterized(
            soeScanKVQuery,
            JsonArray.from(key, recordcount),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
        ))
        .doOnNext(new Action1<AsyncN1qlQueryResult>() {
          @Override
          public void call(AsyncN1qlQueryResult result) {
            if (!result.parseSuccess()) {
              throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeScanKVQuery
                  + ", Errors: " + result.errors());
            }
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryResult, rx.Observable<AsyncN1qlQueryRow>>() {
          @Override
          public rx.Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
            return result.rows();
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryRow, rx.Observable<RawJsonDocument>>() {
          @Override
          public rx.Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
            String id = new String(row.byteValue()).trim();
            return bucket.async().get(id.substring(1, id.length() - 1), RawJsonDocument.class);
          }
        })
        .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
          @Override
          public HashMap<String, ByteIterator> call(RawJsonDocument document) {
            HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
            soeDecode(document.content(), null, tuple);
            return tuple;
          }
        })
        .toBlocking()
        .forEach(new Action1<HashMap<String, ByteIterator>>() {
          @Override
          public void call(HashMap<String, ByteIterator> tuple) {
            data.add(tuple);
          }
        });

    result.addAll(data);
    return Status.OK;
  }

  private List<N1qlQueryRow> explainPlan(String queryStatement) {
    return bucket.query(N1qlQuery.simple("EXPLAIN " + queryStatement)).allRows();
  }

  private Status soeScanN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    int recordcount = gen.getRandomLimit();

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeScanN1qlQuery,
        JsonArray.from(gen.getCustomerIdWithDistribution(), recordcount),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeScanN1qlQuery
          + ", Errors: " + queryResult.errors());
    }

    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }

// *********************  SOE search ********************************

  @Override
  public Status soeSearch(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeSearchKv(result, gen);
      } else {
        return soeSearchN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeSearchKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String soeSearchKvQuery = soeQuerySelectIDClause + " `" + bucketName + "` WHERE " +
        gen.getPredicatesSequence().get(0).getName() + "." +
        gen.getPredicatesSequence().get(0).getNestedPredicateA().getName() + "= $1 AND " +
        gen.getPredicatesSequence().get(1).getName() + " = $2 AND DATE_PART_STR(" +
        gen.getPredicatesSequence().get(2).getName() + ", \"year\") = $3 ORDER BY " +
        gen.getPredicatesSequence().get(0).getName() + "." +
        gen.getPredicatesSequence().get(0).getNestedPredicateA().getName() + ", " +
        gen.getPredicatesSequence().get(1).getName() + ", DATE_PART_STR(" +
        gen.getPredicatesSequence().get(2).getName() + ", \"year\") OFFSET $4 LIMIT $5";

    bucket.async()
        .query(N1qlQuery.parameterized(
            soeSearchKvQuery,
            JsonArray.from(gen.getPredicatesSequence().get(0).getNestedPredicateA().getValueA(),
                gen.getPredicatesSequence().get(1).getValueA(),
                Integer.parseInt(gen.getPredicatesSequence().get(2).getValueA()), offset, recordcount),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
        ))
        .doOnNext(new Action1<AsyncN1qlQueryResult>() {
          @Override
          public void call(AsyncN1qlQueryResult result) {
            if (!result.parseSuccess()) {
              throw new RuntimeException("Error while parsing N1QL Result. Query: soeSearchKv(), " +
                  "Errors: " + result.errors());
            }
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryResult, rx.Observable<AsyncN1qlQueryRow>>() {
          @Override
          public rx.Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
            return result.rows();
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryRow, rx.Observable<RawJsonDocument>>() {
          @Override
          public rx.Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
            String id = new String(row.byteValue()).trim();
            return bucket.async().get(id.substring(1, id.length() - 1), RawJsonDocument.class);
          }
        })
        .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
          @Override
          public HashMap<String, ByteIterator> call(RawJsonDocument document) {
            HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
            soeDecode(document.content(), null, tuple);
            return tuple;
          }
        })
        .toBlocking()
        .forEach(new Action1<HashMap<String, ByteIterator>>() {
          @Override
          public void call(HashMap<String, ByteIterator> tuple) {
            data.add(tuple);
          }
        });
    result.addAll(data);
    return Status.OK;
  }


  private Status soeSearchN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    String soeSearchN1qlQuery = soeQuerySelectAllClause + " `" + bucketName + "` WHERE " +
        gen.getPredicatesSequence().get(0).getName() + "." +
        gen.getPredicatesSequence().get(0).getNestedPredicateA().getName() + "= $1 AND " +
        gen.getPredicatesSequence().get(1).getName() + " = $2 AND DATE_PART_STR(" +
        gen.getPredicatesSequence().get(2).getName() + ", \"year\") = $3 ORDER BY " +
        gen.getPredicatesSequence().get(0).getName() + "." +
        gen.getPredicatesSequence().get(0).getNestedPredicateA().getName() + ", " +
        gen.getPredicatesSequence().get(1).getName() + ", DATE_PART_STR(" +
        gen.getPredicatesSequence().get(2).getName() + ", \"year\") OFFSET $4 LIMIT $5";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeSearchN1qlQuery,
        JsonArray.from(gen.getPredicatesSequence().get(0).getNestedPredicateA().getValueA(),
            gen.getPredicatesSequence().get(1).getValueA(),
            Integer.parseInt(gen.getPredicatesSequence().get(2).getValueA()), offset, recordcount),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeSearchN1qlQuery
          + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }

    return Status.OK;
  }


  // *********************  SOE Page ********************************

  @Override
  public Status soePage(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soePageKv(result, gen);
      } else {
        return soePageN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soePageKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String soePageKvQuery = soeQuerySelectIDClause + " `" + bucketName + "` WHERE " + gen.getPredicate().getName() +
        "." + gen.getPredicate().getNestedPredicateA().getName() + " = $1 OFFSET $2 LIMIT $3";

    bucket.async()
        .query(N1qlQuery.parameterized(
            soePageKvQuery,
            JsonArray.from(gen.getPredicate().getNestedPredicateA().getValueA(), offset, recordcount),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
        ))
        .doOnNext(new Action1<AsyncN1qlQueryResult>() {
          @Override
          public void call(AsyncN1qlQueryResult result) {
            if (!result.parseSuccess()) {
              throw new RuntimeException("Error while parsing N1QL Result. Query: soePageKv(), " +
                  "Errors: " + result.errors());
            }
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryResult, rx.Observable<AsyncN1qlQueryRow>>() {
          @Override
          public rx.Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
            return result.rows();
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryRow, rx.Observable<RawJsonDocument>>() {
          @Override
          public rx.Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
            String id = new String(row.byteValue()).trim();
            return bucket.async().get(id.substring(1, id.length() - 1), RawJsonDocument.class);
          }
        })
        .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
          @Override
          public HashMap<String, ByteIterator> call(RawJsonDocument document) {
            HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
            soeDecode(document.content(), null, tuple);
            return tuple;
          }
        })
        .toBlocking()
        .forEach(new Action1<HashMap<String, ByteIterator>>() {
          @Override
          public void call(HashMap<String, ByteIterator> tuple) {
            data.add(tuple);
          }
        });

    result.addAll(data);

    return Status.OK;
  }


  private Status soePageN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    String soePageN1qlQuery = soeQuerySelectAllClause + " `" + bucketName + "` WHERE " +
        gen.getPredicate().getName() + "." + gen.getPredicate().getNestedPredicateA().getName() +
        " = $1 OFFSET $2 LIMIT $3";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soePageN1qlQuery,
        JsonArray.from(gen.getPredicate().getNestedPredicateA().getValueA(), offset, recordcount),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soePageN1qlQuery
          + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }


  // *********************  SOE NestScan ********************************

  @Override
  public Status soeNestScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeNestScanKv(result, gen);
      } else {
        return soeNestScanN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeNestScanKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String soeNestScanKvQuery = soeQuerySelectIDClause + " `" + bucketName + "` WHERE " +
        gen.getPredicate().getName() + "." +
        gen.getPredicate().getNestedPredicateA().getName() + "." +
        gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getName() + " = $1  LIMIT $2";

    bucket.async()
        .query(N1qlQuery.parameterized(
            soeNestScanKvQuery,
            JsonArray.from(gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getValueA(), recordcount),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
        ))
        .doOnNext(new Action1<AsyncN1qlQueryResult>() {
          @Override
          public void call(AsyncN1qlQueryResult result) {
            if (!result.parseSuccess()) {
              throw new RuntimeException("Error while parsing N1QL Result. Query: soeNestedScanKv(), " +
                  "Errors: " + result.errors());
            }
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryResult, rx.Observable<AsyncN1qlQueryRow>>() {
          @Override
          public rx.Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
            return result.rows();
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryRow, rx.Observable<RawJsonDocument>>() {
          @Override
          public rx.Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
            String id = new String(row.byteValue()).trim();
            return bucket.async().get(id.substring(1, id.length() - 1), RawJsonDocument.class);
          }
        })
        .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
          @Override
          public HashMap<String, ByteIterator> call(RawJsonDocument document) {
            HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
            soeDecode(document.content(), null, tuple);
            return tuple;
          }
        })
        .toBlocking()
        .forEach(new Action1<HashMap<String, ByteIterator>>() {
          @Override
          public void call(HashMap<String, ByteIterator> tuple) {
            data.add(tuple);
          }
        });

    result.addAll(data);
    return Status.OK;
  }


  private Status soeNestScanN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String soeNestScanN1qlQuery = soeQuerySelectAllClause + " `" + bucketName + "` WHERE " +
        gen.getPredicate().getName() + "." +
        gen.getPredicate().getNestedPredicateA().getName() + "." +
        gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getName() + " = $1  LIMIT $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeNestScanN1qlQuery,
        JsonArray.from(gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getValueA(), recordcount),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeNestScanN1qlQuery
          + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }

    return Status.OK;
  }


  // *********************  SOE ArrayScan ********************************

  @Override
  public Status soeArrayScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeArrayScanKv(result, gen);
      } else {
        return soeArrayScanN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeArrayScanKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String soeArrayScanKvQuery = soeQuerySelectIDClause + " `" + bucketName + "` WHERE ANY v IN " +
        gen.getPredicate().getName() + " SATISFIES v = $1 END ORDER BY meta().id LIMIT $2";

    bucket.async()
        .query(N1qlQuery.parameterized(
            soeArrayScanKvQuery,
            JsonArray.from(gen.getPredicate().getValueA(), recordcount),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
        ))
        .doOnNext(new Action1<AsyncN1qlQueryResult>() {
          @Override
          public void call(AsyncN1qlQueryResult result) {
            if (!result.parseSuccess()) {
              throw new RuntimeException("Error while parsing N1QL Result. Query: soeArrayScanKv(), " +
                  "Errors: " + result.errors());
            }
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryResult, rx.Observable<AsyncN1qlQueryRow>>() {
          @Override
          public rx.Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
            return result.rows();
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryRow, rx.Observable<RawJsonDocument>>() {
          @Override
          public rx.Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
            String id = new String(row.byteValue()).trim();
            return bucket.async().get(id.substring(1, id.length() - 1), RawJsonDocument.class);
          }
        })
        .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
          @Override
          public HashMap<String, ByteIterator> call(RawJsonDocument document) {
            HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
            soeDecode(document.content(), null, tuple);
            return tuple;
          }
        })
        .toBlocking()
        .forEach(new Action1<HashMap<String, ByteIterator>>() {
          @Override
          public void call(HashMap<String, ByteIterator> tuple) {
            data.add(tuple);
          }
        });

    result.addAll(data);
    return Status.OK;
  }


  private Status soeArrayScanN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    String soeArrayScanN1qlQuery = soeQuerySelectAllClause + "`" + bucketName + "` WHERE ANY v IN " +
        gen.getPredicate().getName() + " SATISFIES v = $1 END ORDER BY meta().id LIMIT $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeArrayScanN1qlQuery,
        JsonArray.from(gen.getPredicate().getValueA(), recordcount),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeArrayScanN1qlQuery
          + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }

    return Status.OK;
  }


  // *********************  SOE ArrayDeepScan ********************************

  @Override
  public Status soeArrayDeepScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeArrayDeepScanKv(result, gen);
      } else {
        return soeArrayDeepScanN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeArrayDeepScanKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String visitedPlacesFieldName = gen.getPredicate().getName();
    String countryFieldName = gen.getPredicate().getNestedPredicateA().getName();
    String cityFieldName = gen.getPredicate().getNestedPredicateB().getName();

    String cityCountryValue = gen.getPredicate().getNestedPredicateA().getValueA() + "." +
        gen.getPredicate().getNestedPredicateB().getValueA();

    String soeArrayDeepScanKvQuery = soeQuerySelectIDClause + " `" + bucketName + "` WHERE ANY v IN "
        + visitedPlacesFieldName + " SATISFIES  ANY c IN v." + cityFieldName + " SATISFIES (v."
        + countryFieldName + " || \".\" || c) = $1  END END  ORDER BY META().id LIMIT $2";

    bucket.async()
        .query(N1qlQuery.parameterized(
            soeArrayDeepScanKvQuery,
            JsonArray.from(cityCountryValue, recordcount),
            N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
        ))
        .doOnNext(new Action1<AsyncN1qlQueryResult>() {
          @Override
          public void call(AsyncN1qlQueryResult result) {
            if (!result.parseSuccess()) {
              throw new RuntimeException("Error while parsing N1QL Result. Query: soeArrayDeepScanKv(), " +
                  "Errors: " + result.errors());
            }
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryResult, rx.Observable<AsyncN1qlQueryRow>>() {
          @Override
          public rx.Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
            return result.rows();
          }
        })
        .flatMap(new Func1<AsyncN1qlQueryRow, rx.Observable<RawJsonDocument>>() {
          @Override
          public Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
            String id = new String(row.byteValue()).trim();
            return bucket.async().get(id.substring(1, id.length() - 1), RawJsonDocument.class);
          }
        })
        .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
          @Override
          public HashMap<String, ByteIterator> call(RawJsonDocument document) {
            HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
            soeDecode(document.content(), null, tuple);
            return tuple;
          }
        })
        .toBlocking()
        .forEach(new Action1<HashMap<String, ByteIterator>>() {
          @Override
          public void call(HashMap<String, ByteIterator> tuple) {
            data.add(tuple);
          }
        });

    result.addAll(data);
    return Status.OK;
  }


  private Status soeArrayDeepScanN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    String visitedPlacesFieldName = gen.getPredicate().getName();
    String countryFieldName = gen.getPredicate().getNestedPredicateA().getName();
    String cityFieldName = gen.getPredicate().getNestedPredicateB().getName();

    String cityCountryValue = gen.getPredicate().getNestedPredicateA().getValueA() + "." +
        gen.getPredicate().getNestedPredicateB().getValueA();

    String soeArrayDeepScanN1qlQuery = soeQuerySelectAllClause + " `" + bucketName + "` WHERE ANY v IN "
        + visitedPlacesFieldName + " SATISFIES  ANY c IN v." + cityFieldName + " SATISFIES (v."
        + countryFieldName + " || \".\" || c) = $1  END END  ORDER BY META().id LIMIT $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeArrayDeepScanN1qlQuery,
        JsonArray.from(cityCountryValue, recordcount),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeArrayDeepScanN1qlQuery
          + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }


  // *********************  SOE Report  ********************************

  @Override
  public Status soeReport(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeReport1Kv(result, gen);
      } else {
        return soeReport1N1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeReport1Kv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    return soeReport1N1ql(result, gen);
  }


  private Status soeReport1N1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    String soeReport1N1qlQuery = "SELECT * FROM `" + bucketName + "` c1 INNER JOIN `" +
        bucketName + "` o1 ON KEYS c1." + gen.getPredicatesSequence().get(0).getName() + " WHERE c1." +
        gen.getPredicatesSequence().get(1).getName() + "." +
        gen.getPredicatesSequence().get(1).getNestedPredicateA().getName() + " = $1 ";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeReport1N1qlQuery,
        JsonArray.from(gen.getPredicatesSequence().get(1).getNestedPredicateA().getValueA()),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));
    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeReport1N1qlQuery
          + ", Errors: " + queryResult.errors());
    }

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }

    return Status.OK;
  }


  // *********************  SOE Report 2  ********************************

  @Override
  public Status soeReport2(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeReport2Kv(result, gen);
      } else {
        return soeReport2N1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeReport2Kv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    return soeReport2N1ql(result, gen);
  }

  private Status soeReport2N1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    String nameOrderMonth = gen.getPredicatesSequence().get(0).getName();
    String nameOrderSaleprice = gen.getPredicatesSequence().get(1).getName();
    String nameAddress = gen.getPredicatesSequence().get(2).getName();
    String nameAddressZip = gen.getPredicatesSequence().get(2).getNestedPredicateA().getName();
    String nameOrderlist = gen.getPredicatesSequence().get(3).getName();
    String valueOrderMonth = gen.getPredicatesSequence().get(0).getValueA();
    String valueAddressZip = gen.getPredicatesSequence().get(2).getNestedPredicateA().getValueA();

    String soeReport2N1qlQuery = "SELECT o2." + nameOrderMonth + ", c2." + nameAddress + "." + nameAddressZip +
        ", SUM(o2." + nameOrderSaleprice + ") FROM `" + bucketName + "` c2 INNER JOIN `" + bucketName +
        "` o2 ON KEYS c2." + nameOrderlist + " WHERE c2." + nameAddress + "." + nameAddressZip +
        " = $1 AND o2." + nameOrderMonth + " = $2 GROUP BY o2." + nameOrderMonth + ", c2." + nameAddress +
        "." + nameAddressZip + " ORDER BY SUM(o2." + nameOrderSaleprice + ")";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeReport2N1qlQuery,
        JsonArray.from(valueAddressZip, valueOrderMonth),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));
    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeReport2N1qlQuery
          + ", Errors: " + queryResult.errors());
    }

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }

  // ************************************************************************************************

  // *********************  SOE Compound Multiple Array  ********************************

  @Override
  public Status soeCompoundMultipleArray(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeCompoundMultipleArrayKv(result, gen);
      } else {
        return soeCompoundMultipleArrayN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeCompoundMultipleArrayKv(Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    return soeCompoundMultipleArrayN1ql(result, gen);
  }

  private Status soeCompoundMultipleArrayN1ql(Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    final SoeQueryPredicate devicesPredicate = gen.getPredicatesSequence().get(0);
    final String devicesFieldName = devicesPredicate.getName();
    final String devicesValue = devicesPredicate.getValueA();
    final SoeQueryPredicate childrenPredicate = gen.getPredicatesSequence().get(1);
    final String childrenFieldName = childrenPredicate.getName();
    final String childrenAgeFieldName = childrenPredicate.getNestedPredicateA().getName();
    final Integer childrenAgeValue = Integer.valueOf(childrenPredicate.getNestedPredicateA().getValueA());

    String soeCompoundMultipleArrayN1QLQuery = "SELECT * FROM `" + bucketName + "` " +
        "WHERE ANY d IN " + devicesFieldName + " SATISFIES d == $1 END " +
        "AND ANY c IN " + childrenFieldName + " SATISFIES c." + childrenAgeFieldName + " == $2 END " +
        "ORDER BY META().id LIMIT $3";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeCompoundMultipleArrayN1QLQuery,
        JsonArray.from(devicesValue, childrenAgeValue, recordcount),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeCompoundMultipleArrayN1QLQuery
          + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }

  @Override
  public Status soeLiteralArray(String table, Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeLiteralArrayKv(result, gen);
      } else {
        return soeLiteralArrayN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeLiteralArrayKv(Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    return soeLiteralArrayN1ql(result, gen);
  }

  private Status soeLiteralArrayN1ql(Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    final SoeQueryPredicate devicesPredicate = gen.getPredicate();
    final String devicesFieldName = devicesPredicate.getName();
    final String devicesValue = devicesPredicate.getValueA();
    final JsonArray devicesJsonArrayValue = JsonArray.fromJson(devicesValue);
    String soeLiteralArrayN1QLQuery = "SELECT * FROM `" + bucketName + "` " +
        "WHERE " + devicesFieldName + " = $1 " +
        "ORDER BY META().id LIMIT $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        soeLiteralArrayN1QLQuery,
        JsonArray.from(devicesJsonArrayValue, recordcount),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeLiteralArrayN1QLQuery
          + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }

  /**
   * handling rich JSON types by converting Json arrays and Json objects into String.
   *
   * @param source
   * @param fields
   * @param dest
   */
  private void soeDecode(final String source, final Set<String> fields,
                         final HashMap<String, ByteIterator> dest) {
    try {
      JsonNode json = JacksonTransformers.MAPPER.readTree(source);
      boolean checkFields = fields != null && !fields.isEmpty();
      Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields();
      while (jsonFields.hasNext()) {
        Map.Entry<String, JsonNode> jsonField = jsonFields.next();
        String name = jsonField.getKey();
        if (checkFields && !fields.contains(name)) {
          continue;
        }
        JsonNode jsonValue = jsonField.getValue();
        if (jsonValue != null && !jsonValue.isNull()) {
          dest.put(name, new StringByteIterator(jsonValue.toString()));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not soe-decode JSON");
    }
  }

  // ************************************************************************************************
}

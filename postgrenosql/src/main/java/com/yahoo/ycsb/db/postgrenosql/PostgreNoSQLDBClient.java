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

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.generator.soe.Generator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class PostgreNoSQLDBClient extends PostgreNoSQLBaseClient {

  public static final int YCSB_VALUE_COLUMN_INDEX = 2;
  public static final String JSONB = "jsonb";
  // Jackson ObjectMapper
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void init() throws DBException {
    super.init();
    // TODO whatever needs to be done here
  }

  @Override
  public Status soeLoad(String table, Generator generator) {
    try {
      StatementType type = new StatementType(StatementType.Type.SOE_LOAD, table, null);
      PreparedStatement soeLoadStatement = cachedStatements.get(type);
      if (soeLoadStatement == null) {
        soeLoadStatement = createAndCacheSoeLoadStatement(type);
      }

      // find random customer
      final String customerId = generator.getCustomerIdRandom();
      soeLoadStatement.setString(1, customerId);

      final Optional<String> customerDocOpt = getColumnAsStringFromFirstResult(soeLoadStatement,
          YCSB_VALUE_COLUMN_INDEX);
      if (!customerDocOpt.isPresent()) {
        return Status.ERROR;
      }

      // store customer in generator
      final String customerDoc = customerDocOpt.get();
      generator.putCustomerDocument(customerId, customerDoc);

      // get orders from customer
      final JsonNode customerDocJson = objectMapper.readTree(customerDoc);
      final Iterator<JsonNode> ordersIt = customerDocJson
          .get(Generator.SOE_FIELD_CUSTOMER_ORDER_LIST)
          .getElements();
      while (ordersIt.hasNext()) {
        String orderId = ordersIt.next().getTextValue();
        soeLoadStatement.setString(1, orderId);
        final Optional<String> orderDocOpt = getColumnAsStringFromFirstResult(soeLoadStatement,
            YCSB_VALUE_COLUMN_INDEX);
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
      StatementType type = new StatementType(StatementType.Type.SOE_INSERT, table, null);
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

  private PreparedStatement createAndCacheSoeInsertStatement(StatementType type) throws SQLException {
    PreparedStatement loadStatement = connection.prepareStatement(createInsertStatement(type));
    PreparedStatement statement = cachedStatements.putIfAbsent(type, loadStatement);
    if (statement == null) {
      return loadStatement;
    }
    return statement;
  }

  private Optional<String> getColumnAsStringFromFirstResult(PreparedStatement statement, int columnIndex)
      throws SQLException {
    ResultSet resultSet = statement.executeQuery();
    if (!resultSet.next()) {
      return Optional.empty();
    }
    String columnAsString = resultSet.getString(columnIndex);
    resultSet.close();
    return Optional.of(columnAsString);
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
    StringBuilder soeLoad = new StringBuilder("SELECT " + PRIMARY_KEY + ", " + COLUMN_NAME + " ");
    soeLoad.append("FROM " + type.getTableName() + " ");
    soeLoad.append("WHERE " + PRIMARY_KEY + " = ?");
    return soeLoad.toString();
  }
}

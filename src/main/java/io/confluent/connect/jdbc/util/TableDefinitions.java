/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;

/**
 * A simple cache of {@link TableDefinition} keyed.
 */
public class TableDefinitions {

  private static final Logger log = LoggerFactory.getLogger(TableDefinitions.class);

  private final Map<TableId, TableDefinition> cache = new HashMap<>();
  private final DatabaseDialect dialect;

  /**
   * Create an instance that uses the specified database dialect.
   *
   * @param dialect the database dialect; may not be null
   */
  public TableDefinitions(DatabaseDialect dialect) {
    this.dialect = dialect;
  }

  /**
   * Get the {@link TableDefinition} for the given table.
   *
   * @param connection the JDBC connection to use; may not be null
   * @param tableId    the table identifier; may not be null
   * @return the cached {@link TableDefinition}, or null if there is no such table
   * @throws SQLException if there is any problem using the connection
   */
  public TableDefinition get(
      Connection connection,
      final TableId tableId
  ) throws SQLException {
    log.info("RCCA-12335: TableDefinitions#get: tableId=" + tableId);
    TableDefinition dbTable = cache.get(tableId);
    if (dbTable == null) {
      log.info("RCCA-12335: tableId is not in cache!");
      log.info("RCCA-12335: Calling dialect.tableExists(connection, tableId)");
      if (dialect.tableExists(connection, tableId)) {
        log.info("RCCA-12335: dialect.tableExists(connection, tableId) returned true");
        log.info("RCCA-12335: Calling dialect.describeTable(connection, tableId)");
        dbTable = dialect.describeTable(connection, tableId);
        if (dbTable != null) {
          log.info("RCCA-12335: Setting metadata for table {} to {}", tableId, dbTable);
          cache.put(tableId, dbTable);
        } else {
          log.info("RCCA-12335: dialect.describeTable(connection, tableId) returned null");
        }
      } else {
        log.info("RCCA-12335: dialect.tableExists(connection, tableId) returned false");
      }
    } else {
      log.info("RCCA-12335: Found tableId in cache!");
    }
    return dbTable;
  }

  /**
   * Refresh the cached {@link TableDefinition} for the given table.
   *
   * @param connection the JDBC connection to use; may not be null
   * @param tableId    the table identifier; may not be null
   * @return the refreshed {@link TableDefinition}, or null if there is no such table
   * @throws SQLException if there is any problem using the connection
   */
  public TableDefinition refresh(
      Connection connection,
      TableId tableId
  ) throws SQLException {
    TableDefinition dbTable = dialect.describeTable(connection, tableId);
    if (dbTable != null) {
      log.info("Refreshing metadata for table {} to {}", tableId, dbTable);
      cache.put(dbTable.id(), dbTable);
    } else {
      log.warn("Failed to refresh metadata for table {}", tableId);
    }
    return dbTable;
  }
}

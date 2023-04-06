/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client;

import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.util.tables.TableNameUtil;

/**
 * Thrown when the table specified doesn't exist when it was expected to
 */
public class TableNotFoundException extends Exception {
  /**
   * Exception to throw if an operation is attempted on a table that doesn't exist.
   */
  private static final long serialVersionUID = 1L;

  private String tableName;

  /**
   * @param tableId the internal id of the table that was sought
   * @param tableName the visible name of the table that was sought
   * @param description the specific reason why it failed
   */
  public TableNotFoundException(String tableId, String tableName, String description) {
    super("Table" + (tableName != null && !tableName.isEmpty() ? " " + tableName : "")
        + (tableId != null && !tableId.isEmpty() ? " (Id=" + tableId + ")" : "") + " does not exist"
        + (description != null && !description.isEmpty() ? " (" + description + ")" : ""));
    this.tableName = tableName;
  }

  /**
   * @param tableId the internal id of the table that was sought
   * @param tableName the visible name of the table that was sought
   * @param description the specific reason why it failed
   * @param cause the exception that caused this failure
   */
  public TableNotFoundException(String tableId, String tableName, String description,
      Throwable cause) {
    this(tableId, tableName, description);
    super.initCause(cause);
  }

  /**
   * @param e constructs an exception from a thrift exception
   */
  public TableNotFoundException(ThriftTableOperationException e) {
    this(e.getTableId(), e.getTableName(), e.getDescription(), e);
  }

  /**
   * @param tableName the original specified table
   * @param e indicates that a table wasn't found because the namespace specified in the table name
   *        wasn't found
   */
  public TableNotFoundException(String tableName, NamespaceNotFoundException e) {
    this(null, tableName,
        "Namespace " + TableNameUtil.qualify(tableName).getFirst() + " does not exist.", e);
  }

  /**
   * @return the name of the table sought
   */
  public String getTableName() {
    return tableName;
  }
}

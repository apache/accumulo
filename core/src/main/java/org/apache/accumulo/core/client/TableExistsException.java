/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client;

import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;

/**
 * Thrown when the table specified already exists, and it was expected that it didn't
 */
public class TableExistsException extends Exception {
  /**
   * Exception to throw if an operation is attempted on a table that already exists.
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * @param tableId
   *          the internal id of the table that exists
   * @param tableName
   *          the visible name of the table that exists
   * @param description
   *          the specific reason why it failed
   */
  public TableExistsException(String tableId, String tableName, String description) {
    super("Table" + (tableName != null && !tableName.isEmpty() ? " " + tableName : "") + (tableId != null && !tableId.isEmpty() ? " (Id=" + tableId + ")" : "")
        + " exists" + (description != null && !description.isEmpty() ? " (" + description + ")" : ""));
  }

  /**
   * @param tableId
   *          the internal id of the table that exists
   * @param tableName
   *          the visible name of the table that exists
   * @param description
   *          the specific reason why it failed
   * @param cause
   *          the exception that caused this failure
   */
  public TableExistsException(String tableId, String tableName, String description, Throwable cause) {
    this(tableId, tableName, description);
    super.initCause(cause);
  }

  /**
   * @param e
   *          constructs an exception from a thrift exception
   */
  public TableExistsException(ThriftTableOperationException e) {
    this(e.getTableId(), e.getTableName(), e.getDescription(), e);
  }
}

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

/**
 * This exception is thrown if a table is deleted after an operation starts.
 *
 * For example if table A exist when a scan is started, but is deleted during the scan then this
 * exception is thrown.
 */
public class TableDeletedException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  private String tableId;

  public TableDeletedException(String tableId) {
    super("Table ID " + tableId + " was deleted");
    this.tableId = tableId;
  }

  /**
   * @since 2.0.0
   */
  public TableDeletedException(String tableId, Exception cause) {
    super("Table ID " + tableId + " was deleted", cause);
    this.tableId = tableId;
  }

  public String getTableId() {
    return tableId;
  }
}

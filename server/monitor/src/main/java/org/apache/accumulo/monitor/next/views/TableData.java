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
package org.apache.accumulo.monitor.next.views;

import java.util.List;
import java.util.Map;

/**
 * Generic Data Transfer Object (DTO) for a set of rows that have the same columns. Each row will
 * typically represent a server and the columns would typically include the server's address,
 * resource group, and type, and a set of metrics.
 *
 * The response object contains several fields:
 *
 * <pre>
 * columns - contains an array of column definitions that can be used to create the table headers
 *           and DataTable columns
 * data    - an array of objects that can be used for the DataTable data definition
 * </pre>
 *
 */
public record TableData(List<Column> columns, List<Map<String,Object>> data, long timestamp) {

  /**
   * Definition of a column to be rendered in the UI
   */
  public record Column(String key, String label, String description, String uiClass) {
  }

}

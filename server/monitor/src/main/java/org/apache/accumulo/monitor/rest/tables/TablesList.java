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
package org.apache.accumulo.monitor.rest.tables;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Generates a list of tables as a JSON object
 *
 * @since 2.0.0
 *
 */
public class TablesList {

  // Variable names become JSON keys
  public List<TableNamespace> tables = new ArrayList<>();

  /**
   * Initializes array list
   */
  public TablesList() {}

  /**
   * Add a table to the list
   *
   * @param table
   *          Table to add
   */
  public void addTable(TableNamespace table) {
    tables.add(table);
  }
}

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
package org.apache.accumulo.monitor.rest.tables;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates a list with table information
 *
 * @since 2.0.0
 */
public class TableInformationList {

  // Variable names become JSON keys
  public List<TableInformation> table = new ArrayList<>();

  /**
   * Adds a new table to the list
   *
   * @param table Table information to add
   */
  public void addTable(TableInformation table) {
    this.table.add(table);
  }
}

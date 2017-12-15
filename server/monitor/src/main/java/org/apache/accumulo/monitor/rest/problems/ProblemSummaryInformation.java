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
package org.apache.accumulo.monitor.rest.problems;

import org.apache.accumulo.core.client.impl.Table;

/**
 *
 * Generates a problem summary object
 *
 * @since 2.0.0
 *
 */
public class ProblemSummaryInformation {

  // Variable names become JSON keys
  public String tableName;
  public Table.ID tableID;

  public Integer fileRead;
  public Integer fileWrite;
  public Integer tableLoad;

  public ProblemSummaryInformation() {}

  /**
   * Stores a single problem summary object
   *
   * @param tableName
   *          Name of the table with a problem
   * @param tableId
   *          ID of the table with a problem
   * @param fileRead
   *          Number of files read
   * @param fileWrite
   *          Number of files written
   * @param tableLoad
   *          Number of table loads
   */
  public ProblemSummaryInformation(String tableName, Table.ID tableId, Integer fileRead, Integer fileWrite, Integer tableLoad) {
    this.tableName = tableName;
    this.tableID = tableId;
    this.fileRead = fileRead;
    this.fileWrite = fileWrite;
    this.tableLoad = tableLoad;
  }
}

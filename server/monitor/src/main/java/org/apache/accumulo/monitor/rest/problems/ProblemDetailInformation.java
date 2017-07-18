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
 * Generates a problem detail as a JSON object
 *
 * @since 2.0.0
 *
 */
public class ProblemDetailInformation {

  // Variable names become JSON keys
  public String tableName;
  public Table.ID tableID;
  public String type;
  public String server;

  public Long time;
  public String resource;
  public String exception;

  public ProblemDetailInformation() {}

  /**
   * Stores a problem detail
   *
   * @param tableName
   *          Table name of the problem
   * @param tableID
   *          Table ID of the problem
   * @param type
   *          Type of problem
   * @param server
   *          Location of the problem
   * @param time
   *          Time of the problem
   * @param resource
   *          Resource with the problem
   * @param exception
   *          Exception of the problem
   */
  public ProblemDetailInformation(String tableName, Table.ID tableID, String type, String server, Long time, String resource, String exception) {
    this.tableName = tableName;
    this.tableID = tableID;
    this.type = type;
    this.server = server;
    this.time = time;
    this.resource = resource;
    this.exception = exception;
  }
}

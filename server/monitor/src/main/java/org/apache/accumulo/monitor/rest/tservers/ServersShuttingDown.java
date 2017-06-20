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
package org.apache.accumulo.monitor.rest.tservers;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Generates a list of servers shutting down
 *
 * @since 2.0.0
 *
 */
public class ServersShuttingDown {

  // Variable names become JSON keys
  public List<ServerShuttingDownInformation> server = new ArrayList<>();

  /**
   * Initalizes tservers list
   */
  public ServersShuttingDown() {}

  /**
   * Adds a new tserver to the list
   *
   * @param server
   *          TServer to add
   */
  public void addServerShuttingDown(ServerShuttingDownInformation server) {
    this.server.add(server);
  }
}

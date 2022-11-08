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
package org.apache.accumulo.monitor.rest.tservers;

import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;

/**
 * To use for XML Resource
 *
 * @since 2.0.0
 */
public class TabletServer {

  // Variable names become JSON keys
  public final TabletServerInformation server;

  public TabletServer() {
    server = new TabletServerInformation();
  }

  public TabletServer(TabletServerInformation server) {
    this.server = server;
  }

  public TabletServer(Monitor monitor, TabletServerStatus status) {
    server = new TabletServerInformation(monitor, status);
  }

}

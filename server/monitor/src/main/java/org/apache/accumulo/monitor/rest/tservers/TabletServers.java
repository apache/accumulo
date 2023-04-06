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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.monitor.rest.manager.ManagerInformation;

/**
 * Generates a list of servers, bad servers, and dead servers
 *
 * @since 2.0.0
 */
public class TabletServers {

  // Variable names become JSON keys
  public List<TabletServerInformation> servers = new ArrayList<>();
  public List<BadTabletServerInformation> badServers = new ArrayList<>();
  public List<DeadServerInformation> deadServers = new ArrayList<>();

  public TabletServers() {}

  public TabletServers(int size) {
    servers = new ArrayList<>(size);
  }

  /**
   * Adds bad and dead servers to the list
   *
   * @param info Manager information to get bad and dead server information
   */
  public void addBadTabletServer(ManagerInformation info) {
    badServers = info.badTabletServers.badTabletServer;
    deadServers = info.deadTabletServers.deadTabletServer;
  }

  /**
   * Adds new tservers to the list
   *
   * @param tablet New tserver
   */
  public void addTablet(TabletServer tablet) {
    servers.add(tablet.server);
  }
}

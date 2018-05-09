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
package org.apache.accumulo.monitor.rest;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.accumulo.monitor.rest.logs.DeadLoggerList;
import org.apache.accumulo.monitor.rest.master.MasterInformation;
import org.apache.accumulo.monitor.rest.tables.TableInformationList;
import org.apache.accumulo.monitor.rest.tservers.BadTabletServers;
import org.apache.accumulo.monitor.rest.tservers.DeadServerList;
import org.apache.accumulo.monitor.rest.tservers.ServersShuttingDown;
import org.apache.accumulo.monitor.rest.tservers.TabletServer;

/**
 * Generate summary of Monitor
 *
 * @since 2.0.0
 */
@XmlRootElement(name = "stats")
public class SummaryInformation {

  // Variable names become JSON keys
  public List<TabletServer> servers = new ArrayList<>();

  public String masterGoalState, masterState;

  public BadTabletServers badTabletServers;
  public ServersShuttingDown tabletServersShuttingDown;
  public Integer unassignedTablets;
  public DeadServerList deadTabletServers;

  public DeadLoggerList deadLoggers;

  public TableInformationList tables;

  public Totals totals;

  public SummaryInformation() {}

  /**
   * Stores Monitor information as XML or JSON
   *
   * @param size
   *          Number of tservers
   * @param info
   *          Master information
   * @param tablesList
   *          Table list
   */
  public SummaryInformation(int size, MasterInformation info, TableInformationList tablesList) {
    this.servers = new ArrayList<>(size);

    this.masterGoalState = info.masterGoalState;
    this.masterState = info.masterState;

    this.badTabletServers = info.badTabletServers;
    this.tabletServersShuttingDown = info.tabletServersShuttingDown;
    this.unassignedTablets = info.unassignedTablets;
    this.deadTabletServers = info.deadTabletServers;
    this.deadLoggers = info.deadLoggers;

    this.tables = tablesList;

    this.totals = new Totals(info.ingestrate, info.queryrate, info.numentries);
  }

  /**
   * Adds a new tablet
   *
   * @param tablet
   *          Tablet to add
   */
  public void addTabletServer(TabletServer tablet) {
    servers.add(tablet);
  }
}

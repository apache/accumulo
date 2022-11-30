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
package org.apache.accumulo.monitor.rest.manager;

import org.apache.accumulo.monitor.rest.logs.DeadLoggerList;
import org.apache.accumulo.monitor.rest.tservers.BadTabletServers;
import org.apache.accumulo.monitor.rest.tservers.DeadServerList;
import org.apache.accumulo.monitor.rest.tservers.ServersShuttingDown;

/**
 * Responsible for storing manager information as a JSON object
 *
 * @since 2.0.0
 */
public class ManagerInformation {

  // Variable names become JSON keys
  public String manager = "No Managers running";
  public String lastGC = "0";
  public String gcStatus;
  public String managerGoalState;
  public String managerState;

  public Integer onlineTabletServers = 0;
  public Integer totalTabletServers = 0;
  public Integer tablets = 0;
  public Integer unassignedTablets = 0;

  public long numentries = 0L;
  public double osload = 0L;
  public double ingestrate = 0d;
  public double entriesRead = 0d;
  public double queryrate = 0d; // entriesReturned same as queryrate

  public long holdTime = 0L;

  public int tables;
  public int deadTabletServersCount;
  public long lookups;
  public long uptime;

  public BadTabletServers badTabletServers;
  public ServersShuttingDown tabletServersShuttingDown;
  public DeadServerList deadTabletServers;
  public DeadLoggerList deadLoggers;

  /**
   * Creates an empty manager JSON object
   */
  public ManagerInformation() {}

  public ManagerInformation(String manager) {
    this.manager = manager;
  }

  /**
   * Stores a new manager JSON object
   *
   * @param manager Manager location
   * @param onlineTabletServers Number of online tservers
   * @param totalTabletServers Total number of tservers
   * @param lastGC Time of the last gc
   * @param tablets Number of tablet
   * @param unassignedTablets Number of unassigned tablets
   * @param entries Number of entries
   * @param ingest Number of ingest
   * @param entriesRead Number of queries
   * @param entriesReturned Number of returned queries
   * @param holdTime Amount of hold time
   * @param osLoad Amount of load to the OS
   * @param tables Number of tables
   * @param deadTabletServersCount Number of dead tservers
   * @param lookups Number of lookups
   * @param uptime Time the Monitor has been running
   * @param gcStatus Status of the garbage collector
   * @param managerGoalState Goal state of the manager
   * @param managerState Current state of the manager
   * @param badTabletServers Number of bad tservers
   * @param tabletServersShuttingDown Number of tservers shutting down
   * @param deadTabletServers Number of dead tservers
   * @param deadLoggers Number of dead loggers
   */
  public ManagerInformation(String manager, int onlineTabletServers, int totalTabletServers,
      String lastGC, int tablets, int unassignedTablets, long entries, double ingest,
      double entriesRead, double entriesReturned, long holdTime, double osLoad, int tables,
      int deadTabletServersCount, long lookups, long uptime, String gcStatus,
      String managerGoalState, String managerState, BadTabletServers badTabletServers,
      ServersShuttingDown tabletServersShuttingDown, DeadServerList deadTabletServers,
      DeadLoggerList deadLoggers) {

    this.manager = manager;
    this.onlineTabletServers = onlineTabletServers;
    this.totalTabletServers = totalTabletServers;
    this.lastGC = lastGC;
    this.tablets = tablets;
    this.unassignedTablets = unassignedTablets;
    this.numentries = entries;
    this.ingestrate = ingest;
    this.entriesRead = entriesRead;
    this.queryrate = entriesReturned;
    this.holdTime = holdTime;
    this.osload = osLoad;
    this.tables = tables;
    this.deadTabletServersCount = deadTabletServersCount;
    this.lookups = lookups;
    this.uptime = uptime;
    this.gcStatus = gcStatus;
    this.managerGoalState = managerGoalState;
    this.managerState = managerState;
    this.badTabletServers = badTabletServers;
    this.tabletServersShuttingDown = tabletServersShuttingDown;
    this.deadTabletServers = deadTabletServers;
    this.deadLoggers = deadLoggers;
  }
}

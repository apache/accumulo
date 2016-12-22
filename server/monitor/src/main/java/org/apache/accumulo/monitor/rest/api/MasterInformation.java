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
package org.apache.accumulo.monitor.rest.api;

public class MasterInformation {

  public String master, lastGC, gcStatus, masterGoalState, masterState;

  public Integer onlineTabletServers, totalTabletServers, tablets, unassignedTablets;
  public long numentries;
  public double osload, ingestrate, entriesRead, queryrate; // entriesReturned same as queryrate
  public long holdTime;

  public int tables, deadTabletServersCount;
  public long lookups, uptime;

  public BadTabletServers badTabletServers;
  public ServersShuttingDown tabletServersShuttingDown;
  public DeadServerList deadTabletServers;
  public DeadLoggerList deadLoggers;

  public MasterInformation() {
    this.master = "No Masters running";
    this.onlineTabletServers = 0;
    this.totalTabletServers = 0;
    this.lastGC = "0";
    this.tablets = 0;
    this.unassignedTablets = 0;
    this.numentries = 0l;
    this.ingestrate = 0d;
    this.entriesRead = 0d;
    this.queryrate = 0d;
    this.holdTime = 0;
    this.osload = 0l;
  }

  public MasterInformation(String master) {
    this.master = master;
  }

  public MasterInformation(String master, int onlineTabletServers, int totalTabletServers, String lastGC, int tablets, int unassignedTablets, long entries,
      double ingest, double entriesRead, double entriesReturned, long holdTime, double osLoad, int tables, int deadTabletServersCount, long lookups,
      long uptime, String gcStatus, String masterGoalState, String masterState, BadTabletServers badTabletServers,
      ServersShuttingDown tabletServersShuttingDown, DeadServerList deadTabletServers, DeadLoggerList deadLoggers) {

    this.master = master;
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
    this.masterGoalState = masterGoalState;
    this.masterState = masterState;
    this.badTabletServers = badTabletServers;
    this.tabletServersShuttingDown = tabletServersShuttingDown;
    this.deadTabletServers = deadTabletServers;
    this.deadLoggers = deadLoggers;
  }
}

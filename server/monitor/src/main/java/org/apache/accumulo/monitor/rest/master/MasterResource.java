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
package org.apache.accumulo.monitor.rest.master;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.logs.DeadLoggerInformation;
import org.apache.accumulo.monitor.rest.logs.DeadLoggerList;
import org.apache.accumulo.monitor.rest.tservers.BadTabletServerInformation;
import org.apache.accumulo.monitor.rest.tservers.BadTabletServers;
import org.apache.accumulo.monitor.rest.tservers.DeadServerInformation;
import org.apache.accumulo.monitor.rest.tservers.DeadServerList;
import org.apache.accumulo.monitor.rest.tservers.ServerShuttingDownInformation;
import org.apache.accumulo.monitor.rest.tservers.ServersShuttingDown;
import org.apache.accumulo.server.master.state.TabletServerState;

/**
 *
 * Responsible for generating a new Master information JSON object
 *
 * @since 2.0.0
 *
 */
@Path("/master")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class MasterResource {
  public static final String NO_MASTERS = "No Masters running";

  /**
   * Gets the MasterMonitorInfo, allowing for mocking frameworks for testability
   */
  protected static MasterMonitorInfo getMmi() {
    return Monitor.getMmi();
  }

  /**
   * Generates a master information JSON object
   *
   * @return master JSON object
   */
  @GET
  public static MasterInformation getTables() {

    MasterInformation masterInformation;
    MasterMonitorInfo mmi = Monitor.getMmi();

    if (mmi != null) {
      GCStatus gcStatusObj = Monitor.getGcStatus();
      String gcStatus = "Waiting";
      String label = "";
      if (gcStatusObj != null) {
        long start = 0;
        if (gcStatusObj.current.started != 0 || gcStatusObj.currentLog.started != 0) {
          start = Math.max(gcStatusObj.current.started, gcStatusObj.currentLog.started);
          label = "Running";
        } else if (gcStatusObj.lastLog.finished != 0) {
          start = gcStatusObj.lastLog.finished;
        }
        if (start != 0) {
          gcStatus = String.valueOf(start);
        }
      } else {
        gcStatus = "Down";
      }

      List<String> tservers = new ArrayList<>();
      for (TabletServerStatus up : mmi.tServerInfo) {
        tservers.add(up.name);
      }
      for (DeadServer down : mmi.deadTabletServers) {
        tservers.add(down.server);
      }
      List<String> masters = Monitor.getContext().getInstance().getMasterLocations();

      String master = masters.size() == 0 ? "Down" : AddressUtil.parseAddress(masters.get(0), false).getHost();
      Integer onlineTabletServers = mmi.tServerInfo.size();
      Integer totalTabletServers = tservers.size();
      Integer tablets = Monitor.getTotalTabletCount();
      Integer unassignedTablets = mmi.unassignedTablets;
      long entries = Monitor.getTotalEntries();
      double ingest = Monitor.getTotalIngestRate();
      double entriesRead = Monitor.getTotalScanRate();
      double entriesReturned = Monitor.getTotalQueryRate();
      long holdTime = Monitor.getTotalHoldTime();
      double osLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();

      int tables = Monitor.getTotalTables();
      int deadTabletServers = mmi.deadTabletServers.size();
      long lookups = Monitor.getTotalLookups();
      long uptime = System.currentTimeMillis() - Monitor.getStartTime();

      masterInformation = new MasterInformation(master, onlineTabletServers, totalTabletServers, gcStatus, tablets, unassignedTablets, entries, ingest,
          entriesRead, entriesReturned, holdTime, osLoad, tables, deadTabletServers, lookups, uptime, label, getGoalState(), getState(), getNumBadTservers(),
          getServersShuttingDown(), getDeadTservers(), getDeadLoggers());
    } else {
      masterInformation = new MasterInformation();
    }
    return masterInformation;
  }

  /**
   * Returns the current state of the master
   *
   * @return master state
   */
  public static String getState() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return NO_MASTERS;
    }
    return mmi.state.toString();
  }

  /**
   * Returns the goal state of the master
   *
   * @return master goal state
   */
  public static String getGoalState() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return NO_MASTERS;
    }
    return mmi.goalState.name();
  }

  /**
   * Generates a dead server list as a JSON object
   *
   * @return dead server list
   */
  public static DeadServerList getDeadTservers() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return new DeadServerList();
    }

    DeadServerList deadServers = new DeadServerList();
    // Add new dead servers to the list
    for (DeadServer dead : mmi.deadTabletServers) {
      deadServers.addDeadServer(new DeadServerInformation(dead.server, dead.lastStatus, dead.status));
    }
    return deadServers;
  }

  /**
   * Generates a dead logger list as a JSON object
   *
   * @return dead logger list
   */
  public static DeadLoggerList getDeadLoggers() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return new DeadLoggerList();
    }

    DeadLoggerList deadLoggers = new DeadLoggerList();
    // Add new dead loggers to the list
    for (DeadServer dead : mmi.deadTabletServers) {
      deadLoggers.addDeadLogger(new DeadLoggerInformation(dead.server, dead.lastStatus, dead.status));
    }
    return deadLoggers;
  }

  /**
   * Generates bad tserver lists as a JSON object
   *
   * @return bad tserver list
   */
  public static BadTabletServers getNumBadTservers() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return new BadTabletServers();
    }

    Map<String,Byte> badServers = mmi.getBadTServers();

    if (null == badServers || badServers.isEmpty()) {
      return new BadTabletServers();
    }

    BadTabletServers readableBadServers = new BadTabletServers();
    // Add new bad tservers to the list
    for (Entry<String,Byte> badServer : badServers.entrySet()) {
      try {
        TabletServerState state = TabletServerState.getStateById(badServer.getValue());
        readableBadServers.addBadServer(new BadTabletServerInformation(badServer.getKey(), state.name()));
      } catch (IndexOutOfBoundsException e) {
        readableBadServers.addBadServer(new BadTabletServerInformation(badServer.getKey(), "Unknown state"));
      }
    }
    return readableBadServers;
  }

  /**
   * Generates a JSON object of a list of servers shutting down
   *
   * @return servers shutting down list
   */
  public static ServersShuttingDown getServersShuttingDown() {
    ServersShuttingDown servers = new ServersShuttingDown();
    // Add new servers to the list
    for (String server : Monitor.getMmi().serversShuttingDown) {
      servers.addServerShuttingDown(new ServerShuttingDownInformation(server));
    }
    return servers;
  }

}

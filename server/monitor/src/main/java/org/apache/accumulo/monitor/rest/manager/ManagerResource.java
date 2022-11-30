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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.manager.thrift.DeadServer;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
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
import org.apache.accumulo.server.manager.state.TabletServerState;

/**
 * Responsible for generating a new Manager information JSON object
 *
 * @since 2.0.0
 */
@Path("/manager")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class ManagerResource {
  public static final String NO_MANAGERS = "No Managers running";

  @Inject
  private Monitor monitor;

  /**
   * Generates a manager information JSON object
   *
   * @return manager JSON object
   */
  @GET
  public ManagerInformation getTables() {
    return getTables(monitor);
  }

  public static ManagerInformation getTables(Monitor monitor) {
    ManagerInformation managerInformation;
    ManagerMonitorInfo mmi = monitor.getMmi();

    if (mmi != null) {
      GCStatus gcStatusObj = monitor.getGcStatus();
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
      List<String> managers = monitor.getContext().getManagerLocations();

      String manager =
          managers.isEmpty() ? "Down" : AddressUtil.parseAddress(managers.get(0), false).getHost();
      int onlineTabletServers = mmi.tServerInfo.size();
      int totalTabletServers = tservers.size();
      int tablets = monitor.getTotalTabletCount();
      int unassignedTablets = mmi.unassignedTablets;
      long entries = monitor.getTotalEntries();
      double ingest = monitor.getTotalIngestRate();
      double entriesRead = monitor.getTotalScanRate();
      double entriesReturned = monitor.getTotalQueryRate();
      long holdTime = monitor.getTotalHoldTime();
      double osLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();

      int tables = monitor.getTotalTables();
      int deadTabletServers = mmi.deadTabletServers.size();
      long lookups = monitor.getTotalLookups();
      long uptime = System.currentTimeMillis() - monitor.getStartTime();

      managerInformation = new ManagerInformation(manager, onlineTabletServers, totalTabletServers,
          gcStatus, tablets, unassignedTablets, entries, ingest, entriesRead, entriesReturned,
          holdTime, osLoad, tables, deadTabletServers, lookups, uptime, label,
          getGoalState(monitor), getState(monitor), getNumBadTservers(monitor),
          getServersShuttingDown(monitor), getDeadTservers(monitor), getDeadLoggers(monitor));
    } else {
      managerInformation = new ManagerInformation();
    }
    return managerInformation;
  }

  /**
   * Returns the current state of the manager
   *
   * @return manager state
   */
  public static String getState(Monitor monitor) {
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return NO_MANAGERS;
    }
    return mmi.state.toString();
  }

  /**
   * Returns the goal state of the manager
   *
   * @return manager goal state
   */
  public static String getGoalState(Monitor monitor) {
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return NO_MANAGERS;
    }
    return mmi.goalState.name();
  }

  /**
   * Generates a dead server list as a JSON object
   *
   * @return dead server list
   */
  public static DeadServerList getDeadTservers(Monitor monitor) {
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return new DeadServerList();
    }

    DeadServerList deadServers = new DeadServerList();
    // Add new dead servers to the list
    for (DeadServer dead : mmi.deadTabletServers) {
      deadServers
          .addDeadServer(new DeadServerInformation(dead.server, dead.lastStatus, dead.status));
    }
    return deadServers;
  }

  /**
   * Generates a dead logger list as a JSON object
   *
   * @return dead logger list
   */
  public static DeadLoggerList getDeadLoggers(Monitor monitor) {
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return new DeadLoggerList();
    }

    DeadLoggerList deadLoggers = new DeadLoggerList();
    // Add new dead loggers to the list
    for (DeadServer dead : mmi.deadTabletServers) {
      deadLoggers
          .addDeadLogger(new DeadLoggerInformation(dead.server, dead.lastStatus, dead.status));
    }
    return deadLoggers;
  }

  /**
   * Generates bad tserver lists as a JSON object
   *
   * @return bad tserver list
   */
  public static BadTabletServers getNumBadTservers(Monitor monitor) {
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return new BadTabletServers();
    }

    Map<String,Byte> badServers = mmi.getBadTServers();

    if (badServers == null || badServers.isEmpty()) {
      return new BadTabletServers();
    }

    BadTabletServers readableBadServers = new BadTabletServers();
    // Add new bad tservers to the list
    for (Entry<String,Byte> badServer : badServers.entrySet()) {
      try {
        TabletServerState state = TabletServerState.getStateById(badServer.getValue());
        readableBadServers
            .addBadServer(new BadTabletServerInformation(badServer.getKey(), state.name()));
      } catch (IndexOutOfBoundsException e) {
        readableBadServers
            .addBadServer(new BadTabletServerInformation(badServer.getKey(), "Unknown state"));
      }
    }
    return readableBadServers;
  }

  /**
   * Generates a JSON object of a list of servers shutting down
   *
   * @return servers shutting down list
   */
  public static ServersShuttingDown getServersShuttingDown(Monitor monitor) {
    ManagerMonitorInfo mmi = monitor.getMmi();
    ServersShuttingDown servers = new ServersShuttingDown();
    if (mmi == null) {
      return servers;
    }

    // Add new servers to the list
    for (String server : mmi.serversShuttingDown) {
      servers.addServerShuttingDown(new ServerShuttingDownInformation(server));
    }
    return servers;
  }

}

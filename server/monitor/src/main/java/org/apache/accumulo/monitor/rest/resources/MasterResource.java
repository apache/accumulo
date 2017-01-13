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
package org.apache.accumulo.monitor.rest.resources;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.BadTabletServerInformation;
import org.apache.accumulo.monitor.rest.api.BadTabletServers;
import org.apache.accumulo.monitor.rest.api.DeadLoggerInformation;
import org.apache.accumulo.monitor.rest.api.DeadLoggerList;
import org.apache.accumulo.monitor.rest.api.DeadServerInformation;
import org.apache.accumulo.monitor.rest.api.DeadServerList;
import org.apache.accumulo.monitor.rest.api.MasterInformation;
import org.apache.accumulo.monitor.rest.api.ServerShuttingDownInformation;
import org.apache.accumulo.monitor.rest.api.ServersShuttingDown;
import org.apache.accumulo.server.master.state.TabletServerState;

public class MasterResource extends BasicResource {
  public static final String NO_MASTERS = "No Masters running";

  /**
   * Gets the MasterMonitorInfo, allowing for mocking frameworks for testability
   */
  protected MasterMonitorInfo getMmi() {
    return Monitor.getMmi();
  }

  @GET
  public MasterInformation getTables() {

    MasterInformation masterInformation;

    if (Monitor.getMmi() != null) {
      String gcStatus = "Waiting";
      String label = "";
      if (Monitor.getGcStatus() != null) {
        long start = 0;
        if (Monitor.getGcStatus().current.started != 0 || Monitor.getGcStatus().currentLog.started != 0) {
          start = Math.max(Monitor.getGcStatus().current.started, Monitor.getGcStatus().currentLog.started);
          label = "Running";
        } else if (Monitor.getGcStatus().lastLog.finished != 0) {
          start = Monitor.getGcStatus().lastLog.finished;
        }
        if (start != 0) {
          gcStatus = String.valueOf(start);
        }
      } else {
        gcStatus = "Down";
      }

      List<String> tservers = new ArrayList<>();
      for (TabletServerStatus up : Monitor.getMmi().tServerInfo) {
        tservers.add(up.name);
      }
      for (DeadServer down : Monitor.getMmi().deadTabletServers) {
        tservers.add(down.server);
      }
      List<String> masters = Monitor.getContext().getInstance().getMasterLocations();

      String master = masters.size() == 0 ? "Down" : AddressUtil.parseAddress(masters.get(0), false).getHostText();
      Integer onlineTabletServers = Monitor.getMmi().tServerInfo.size();
      Integer totalTabletServers = tservers.size();
      Integer tablets = Monitor.getTotalTabletCount();
      Integer unassignedTablets = Monitor.getMmi().unassignedTablets;
      long entries = Monitor.getTotalEntries();
      double ingest = Monitor.getTotalIngestRate();
      double entriesRead = Monitor.getTotalScanRate();
      double entriesReturned = Monitor.getTotalQueryRate();
      long holdTime = Monitor.getTotalHoldTime();
      double osLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();

      int tables = Monitor.getTotalTables();
      int deadTabletServers = Monitor.getMmi().deadTabletServers.size();
      long lookups = Monitor.getTotalLookups();
      long uptime = System.currentTimeMillis() - Monitor.getStartTime(); // TODO Check that this works when starting accumulo

      System.out.println(Monitor.getStartTime());

      masterInformation = new MasterInformation(master, onlineTabletServers, totalTabletServers, gcStatus, tablets, unassignedTablets, entries, ingest,
          entriesRead, entriesReturned, holdTime, osLoad, tables, deadTabletServers, lookups, uptime, label, getGoalState(), getState(), getNumBadTservers(),
          getServersShuttingDown(), getDeadTservers(), getDeadLoggers());

    } else {
      masterInformation = new MasterInformation();
    }

    return masterInformation;
  }

  public String getState() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return NO_MASTERS;
    }

    return mmi.state.toString();
  }

  public String getGoalState() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return NO_MASTERS;
    }

    return mmi.goalState.name();
  }

  public DeadServerList getDeadTservers() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return new DeadServerList();
    }

    DeadServerList deadServers = new DeadServerList();
    for (DeadServer dead : mmi.deadTabletServers) {
      deadServers.addDeadServer(new DeadServerInformation(dead.server, dead.lastStatus, dead.status));
    }
    return deadServers;
  }

  public DeadLoggerList getDeadLoggers() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return new DeadLoggerList();
    }

    DeadLoggerList deadLoggers = new DeadLoggerList();
    for (DeadServer dead : mmi.deadTabletServers) {
      deadLoggers.addDeadLogger(new DeadLoggerInformation(dead.server, dead.lastStatus, dead.status));
    }
    return deadLoggers;
  }

  public BadTabletServers getNumBadTservers() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return new BadTabletServers();
    }

    Map<String,Byte> badServers = mmi.getBadTServers();

    if (null == badServers || badServers.isEmpty()) {
      return new BadTabletServers();
    }

    BadTabletServers readableBadServers = new BadTabletServers();
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

  public ServersShuttingDown getServersShuttingDown() {
    ServersShuttingDown servers = new ServersShuttingDown();

    for (String server : Monitor.getMmi().serversShuttingDown) {
      servers.addServerShuttingDown(new ServerShuttingDownInformation(server));
    }

    return servers;
  }

  @Path("/tserver_info")
  @GET
  public List<TabletServerStatus> getTabletServerInfo() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return Collections.emptyList();
    }

    return mmi.getTServerInfo();
  }
}

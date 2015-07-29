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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.master.state.TabletServerState;

/**
 * 
 */
@Path("/master")
@Produces(MediaType.APPLICATION_JSON)
public class MasterResource {
  public static final String NO_MASTERS = "No Masters running";

  /**
   * Gets the MasterMonitorInfo, allowing for mocking frameworks for testability
   */
  protected MasterMonitorInfo getMmi() {
    return Monitor.getMmi();
  }

  @Path("/state")
  @GET
  public String getState() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return NO_MASTERS;
    }

    return mmi.state.toString();
  }

  @Path("/goal_state")
  @GET
  public String getGoalState() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return NO_MASTERS;
    }

    return mmi.goalState.name();
  }

  @Path("/dead_tservers")
  @GET
  public List<DeadServer> getDeadTservers() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return Collections.emptyList();
    }

    List<DeadServer> deadServers = mmi.deadTabletServers;
    return null == deadServers ? Collections.<DeadServer> emptyList() : deadServers;
  }

  @Path("/bad_tservers")
  @GET
  public Map<String,String> getNumBadTservers() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return Collections.emptyMap();
    }

    Map<String,Byte> badServers = mmi.getBadTServers();

    if (null == badServers || badServers.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String,String> readableBadServers = new HashMap<String,String>();
    for (Entry<String,Byte> badServer : badServers.entrySet()) {
      try {
        TabletServerState state = TabletServerState.getStateById(badServer.getValue());
        readableBadServers.put(badServer.getKey(), state.name());
      } catch (IndexOutOfBoundsException e) {
        readableBadServers.put(badServer.getKey(), "Unknown state");
      }
    }

    return readableBadServers;
  }

  @Path("/unassigned_tablets")
  @GET
  public int getUnassignedTablets() {
    MasterMonitorInfo mmi = getMmi();
    if (null == mmi) {
      return -1;
    }

    return mmi.getUnassignedTablets();
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

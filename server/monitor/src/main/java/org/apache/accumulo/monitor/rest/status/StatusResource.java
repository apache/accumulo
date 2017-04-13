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
package org.apache.accumulo.monitor.rest.status;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.monitor.DedupedLogEvent;
import org.apache.accumulo.server.monitor.LogService;
import org.apache.log4j.Level;

/**
 *
 * Generates the status for master, gc, and tservers as well as log and problem reports
 *
 * @since 2.0.0
 *
 */
@Path("/status")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class StatusResource {

  public enum Status {
    OK, ERROR, WARN;
  }

  /**
   * Generates the JSON object with the status
   *
   * @return Status report
   */
  @GET
  public StatusInformation getTables() {

    StatusInformation status;
    Status masterStatus;
    Status gcStatus;
    Status tServerStatus = Status.ERROR;

    if (Monitor.getMmi() != null) {
      if (Monitor.getGcStatus() != null) {
        gcStatus = Status.OK;
      } else {
        gcStatus = Status.ERROR;
      }

      List<String> tservers = new ArrayList<>();
      for (TabletServerStatus up : Monitor.getMmi().tServerInfo) {
        tservers.add(up.name);
      }
      for (DeadServer down : Monitor.getMmi().deadTabletServers) {
        tservers.add(down.server);
      }
      List<String> masters = Monitor.getContext().getInstance().getMasterLocations();

      masterStatus = masters.size() == 0 ? Status.ERROR : Status.OK;

      int tServerUp = Monitor.getMmi().getTServerInfoSize();
      int tServerDown = Monitor.getMmi().getDeadTabletServersSize();
      int tServerBad = Monitor.getMmi().getBadTServersSize();

      /*
       * If there are no dead or bad servers and there are tservers up, status is OK, if there are dead or bad servers and there is at least a tserver up,
       * status is WARN, otherwise, the status is an error.
       */
      if ((tServerDown > 0 || tServerBad > 0) && tServerUp > 0) {
        tServerStatus = Status.WARN;
      } else if ((tServerDown == 0 || tServerBad == 0) && tServerUp > 0) {
        tServerStatus = Status.OK;
      } else if (tServerUp == 0) {
        tServerStatus = Status.ERROR;
      }
    } else {
      masterStatus = Status.ERROR;
      if (null == Monitor.getGcStatus()) {
        gcStatus = Status.ERROR;
      } else {
        gcStatus = Status.OK;
      }
      tServerStatus = Status.ERROR;
    }

    List<DedupedLogEvent> logs = LogService.getInstance().getEvents();
    boolean logsHaveError = false;
    for (DedupedLogEvent dedupedLogEvent : logs) {
      if (dedupedLogEvent.getEvent().getLevel().isGreaterOrEqual(Level.ERROR)) {
        logsHaveError = true;
        break;
      }
    }

    int numProblems = Monitor.getProblemSummary().entrySet().size();

    status = new StatusInformation(masterStatus.toString(), gcStatus.toString(), tServerStatus.toString(), logs.size(), logsHaveError, numProblems);

    return status;
  }
}

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
package org.apache.accumulo.monitor.rest.scans;

import java.util.Map;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.Monitor.ScanStats;

/**
 * Generate a new Scan list JSON object
 *
 * @since 2.0.0
 */
@Path("/scans")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class ScansResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates a new JSON object with scan information
   *
   * @return Scan JSON object
   */
  @GET
  public Scans getActiveScans() throws Exception {
    Scans scans = new Scans();
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return scans;
    }

    Map<HostAndPort,ScanStats> entry = monitor.getScans();

    // Adds new scans to the array for tservers known to the Manager
    for (TabletServerStatus tserverInfo : mmi.getTServerInfo()) {
      ScanStats stats = entry.get(HostAndPort.fromString(tserverInfo.name));
      if (stats != null) {
        scans.addScan(new ScanInformation(tserverInfo.name, stats));
      }
    }
    // Add all ScanServer entries, the Manager is not aware of the ScanServers
    monitor.getScanServerScans().forEach((k, v) -> {
      scans.addScan(new ScanInformation(k.toString(), v));
    });
    return scans;
  }
}

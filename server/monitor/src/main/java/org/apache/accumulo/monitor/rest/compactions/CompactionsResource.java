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
package org.apache.accumulo.monitor.rest.compactions;

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

/**
 * Generate a new Compaction list JSON object
 *
 * @since 2.1.0
 */
@Path("/compactions")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class CompactionsResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates a new JSON object with compaction information
   *
   * @return JSON object
   */
  @GET
  public Compactions getActiveCompactions() {
    Compactions compactions = new Compactions();
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return compactions;
    }

    Map<HostAndPort,Monitor.CompactionStats> entry = monitor.getCompactions();

    for (TabletServerStatus tserverInfo : mmi.getTServerInfo()) {
      var stats = entry.get(HostAndPort.fromString(tserverInfo.name));
      if (stats != null) {
        compactions.addCompaction(new CompactionInfo(tserverInfo, stats));
      }
    }
    return compactions;
  }
}

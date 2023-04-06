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
package org.apache.accumulo.monitor.rest.bulkImports;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.BulkImportStatus;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;

/**
 * The BulkImportResource is responsible for obtaining the information of the bulk import, and
 * tablet server bulk import from the Monitor and creating the JSON objects with each
 *
 * @since 2.0.0
 */
@Path("/bulkImports")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class BulkImportResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates bulk import and tserver bulk imports with the information from the Monitor
   *
   * @return JSON object with BulkImport information
   */
  @GET
  public BulkImport getTables() {
    BulkImport bulkImport = new BulkImport();
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return bulkImport;
    }

    // Generating Bulk Import and adding it to the return object
    for (BulkImportStatus bulk : mmi.bulkImports) {
      bulkImport
          .addBulkImport(new BulkImportInformation(bulk.filename, bulk.startTime, bulk.state));
    }

    // Generating TServer Bulk Import and adding it to the return object
    for (TabletServerStatus tserverInfo : mmi.getTServerInfo()) {
      int size = 0;
      long oldest = 0L;

      List<BulkImportStatus> stats = tserverInfo.bulkImports;
      if (stats != null) {
        size = stats.size();
        oldest = Long.MAX_VALUE;
        for (BulkImportStatus bulk : stats) {
          oldest = Math.min(oldest, bulk.startTime);
        }
        if (oldest == Long.MAX_VALUE) {
          oldest = 0L;
        }
      }
      bulkImport.addTabletServerBulkImport(
          new TabletServerBulkImportInformation(tserverInfo, size, oldest));
    }
    return bulkImport;
  }
}

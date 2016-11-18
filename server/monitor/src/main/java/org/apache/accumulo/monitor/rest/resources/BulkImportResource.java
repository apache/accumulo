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

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.master.thrift.BulkImportStatus;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.BulkImport;
import org.apache.accumulo.monitor.rest.api.BulkImportInformation;
import org.apache.accumulo.monitor.rest.api.TabletServerBulkImportInformation;

@Path("/bulkImports")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class BulkImportResource {

  @GET
  public BulkImport getTables() {

    BulkImport bulkImport = new BulkImport();

    for (BulkImportStatus bulk : Monitor.getMmi().bulkImports) {
      bulkImport.addBulkImport(new BulkImportInformation(bulk.filename, bulk.startTime, bulk.state));
    }

    for (TabletServerStatus tserverInfo : Monitor.getMmi().getTServerInfo()) {

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

      bulkImport.addTabletServerBulkImport(new TabletServerBulkImportInformation(tserverInfo, size, oldest));

    }

    return bulkImport;
  }
}

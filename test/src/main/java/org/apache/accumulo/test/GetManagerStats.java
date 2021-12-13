/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.clientImpl.ManagerClient;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.manager.thrift.DeadServer;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.BulkImportStatus;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.TableInfoUtil;

public class GetManagerStats {
  public static void main(String[] args) throws Exception {
    ManagerClientService.Iface client = null;
    ManagerMonitorInfo stats = null;
    var context = new ServerContext(SiteConfiguration.auto());
    while (true) {
      try {
        client = ManagerClient.getConnectionWithRetry(context);
        stats = client.getManagerStats(TraceUtil.traceInfo(), context.rpcCreds());
        break;
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } finally {
        if (client != null) {
          ManagerClient.close(client, context);
        }
      }
    }
    out(0, "State: %s%n", stats.state.name());
    out(0, "Goal State: %s%n", stats.goalState.name());
    if (stats.serversShuttingDown != null && !stats.serversShuttingDown.isEmpty()) {
      out(0, "Servers to shutdown%n");
      for (String server : stats.serversShuttingDown) {
        out(1, "%s%n", server);
      }
    }
    out(0, "Unassigned tablets: %d%n", stats.unassignedTablets);
    if (stats.badTServers != null && !stats.badTServers.isEmpty()) {
      out(0, "Bad servers%n");

      for (Entry<String,Byte> entry : stats.badTServers.entrySet()) {
        out(1, "%s: %d%n", entry.getKey(), (int) entry.getValue());
      }
    }
    out(0, "Dead tablet servers count: %s%n", stats.deadTabletServers.size());
    for (DeadServer dead : stats.deadTabletServers) {
      out(1, "Dead tablet server: %s%n", dead.server);
      out(2, "Last report: %s%n", new SimpleDateFormat().format(new Date(dead.lastStatus)));
      out(2, "Cause: %s%n", dead.status);
    }
    out(0, "Bulk imports: %s%n", stats.bulkImports.size());
    for (BulkImportStatus bulk : stats.bulkImports) {
      out(1, "Import directory: %s%n", bulk.filename);
      out(2, "Bulk state %s%n", bulk.state);
      out(2, "Bulk start %s%n", bulk.startTime);
    }
    if (stats.tableMap != null && !stats.tableMap.isEmpty()) {
      out(0, "Tables%n");
      for (Entry<String,TableInfo> entry : stats.tableMap.entrySet()) {
        TableInfo v = entry.getValue();
        out(1, "%s%n", entry.getKey());
        out(2, "Records: %d%n", v.recs);
        out(2, "Records in Memory: %d%n", v.recsInMemory);
        out(2, "Tablets: %d%n", v.tablets);
        out(2, "Online Tablets: %d%n", v.onlineTablets);
        out(2, "Ingest Rate: %.2f%n", v.ingestRate);
        out(2, "Query Rate: %.2f%n", v.queryRate);
      }
    }
    if (stats.tServerInfo != null && !stats.tServerInfo.isEmpty()) {
      out(0, "Tablet Servers%n");
      long now = System.currentTimeMillis();
      for (TabletServerStatus server : stats.tServerInfo) {
        TableInfo summary = TableInfoUtil.summarizeTableStats(server);
        out(1, "Name: %s%n", server.name);
        out(2, "Ingest: %.2f%n", summary.ingestRate);
        out(2, "Last Contact: %s%n", server.lastContact);
        out(2, "OS Load Average: %.2f%n", server.osLoad);
        out(2, "Queries: %.2f%n", summary.queryRate);
        out(2, "Time Difference: %.1f%n", ((now - server.lastContact) / 1000.));
        out(2, "Total Records: %d%n", summary.recs);
        out(2, "Lookups: %d%n", server.lookups);
        if (server.holdTime > 0) {
          out(2, "Hold Time: %d%n", server.holdTime);
        }
        if (server.tableMap != null && !server.tableMap.isEmpty()) {
          out(2, "Tables%n");
          for (Entry<String,TableInfo> status : server.tableMap.entrySet()) {
            TableInfo info = status.getValue();
            out(3, "Table: %s%n", status.getKey());
            out(4, "Tablets: %d%n", info.onlineTablets);
            out(4, "Records: %d%n", info.recs);
            out(4, "Records in Memory: %d%n", info.recsInMemory);
            out(4, "Ingest: %.2f%n", info.ingestRate);
            out(4, "Queries: %.2f%n", info.queryRate);
            out(4, "Major Compacting: %d%n", info.majors == null ? 0 : info.majors.running);
            out(4, "Queued for Major Compaction: %d%n",
                info.majors == null ? 0 : info.majors.queued);
            out(4, "Minor Compacting: %d%n", info.minors == null ? 0 : info.minors.running);
            out(4, "Queued for Minor Compaction: %d%n",
                info.minors == null ? 0 : info.minors.queued);
          }
        }
        out(2, "Recoveries: %d%n", server.logSorts.size());
        for (RecoveryStatus sort : server.logSorts) {
          out(3, "File: %s%n", sort.name);
          out(3, "Progress: %.2f%%%n", sort.progress * 100);
          out(3, "Time running: %s%n", sort.runtime / 1000.);
        }
        out(3, "Bulk imports: %s%n", stats.bulkImports.size());
        for (BulkImportStatus bulk : stats.bulkImports) {
          out(4, "Import file: %s%n", bulk.filename);
          out(5, "Bulk state %s%n", bulk.state);
          out(5, "Bulk start %s%n", bulk.startTime);
        }

      }
    }
  }

  private static void out(int indent, String fmtString, Object... args) {
    for (int i = 0; i < indent; i++) {
      System.out.print(" ");
    }
    System.out.printf(fmtString, args);
  }

}

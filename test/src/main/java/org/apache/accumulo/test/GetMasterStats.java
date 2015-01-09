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
package org.apache.accumulo.test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.accumulo.trace.instrument.Tracer;

public class GetMasterStats {
  public static void main(String[] args) throws Exception {
    MasterClientService.Iface client = null;
    MasterMonitorInfo stats = null;
    try {
      Instance instance = HdfsZooInstance.getInstance();
      client = MasterClient.getConnectionWithRetry(instance);
      stats = client.getMasterStats(Tracer.traceInfo(), SystemCredentials.get().toThrift(instance));
    } finally {
      if (client != null)
        MasterClient.close(client);
    }
    out(0, "State: " + stats.state.name());
    out(0, "Goal State: " + stats.goalState.name());
    if (stats.serversShuttingDown != null && stats.serversShuttingDown.size() > 0) {
      out(0, "Servers to shutdown");
      for (String server : stats.serversShuttingDown) {
        out(1, "%s", server);
      }
    }
    out(0, "Unassigned tablets: %d", stats.unassignedTablets);
    if (stats.badTServers != null && stats.badTServers.size() > 0) {
      out(0, "Bad servers");

      for (Entry<String,Byte> entry : stats.badTServers.entrySet()) {
        out(1, "%s: %d", entry.getKey(), (int) entry.getValue());
      }
    }
    out(0, "Dead tablet servers count: %s", stats.deadTabletServers.size());
    for (DeadServer dead : stats.deadTabletServers) {
      out(1, "Dead tablet server: %s", dead.server);
      out(2, "Last report: %s", new SimpleDateFormat().format(new Date(dead.lastStatus)));
      out(2, "Cause: %s", dead.status);
    }
    if (stats.tableMap != null && stats.tableMap.size() > 0) {
      out(0, "Tables");
      for (Entry<String,TableInfo> entry : stats.tableMap.entrySet()) {
        TableInfo v = entry.getValue();
        out(1, "%s", entry.getKey());
        out(2, "Records: %d", v.recs);
        out(2, "Records in Memory: %d", v.recsInMemory);
        out(2, "Tablets: %d", v.tablets);
        out(2, "Online Tablets: %d", v.onlineTablets);
        out(2, "Ingest Rate: %.2f", v.ingestRate);
        out(2, "Query Rate: %.2f", v.queryRate);
      }
    }
    if (stats.tServerInfo != null && stats.tServerInfo.size() > 0) {
      out(0, "Tablet Servers");
      long now = System.currentTimeMillis();
      for (TabletServerStatus server : stats.tServerInfo) {
        TableInfo summary = TableInfoUtil.summarizeTableStats(server);
        out(1, "Name: %s", server.name);
        out(2, "Ingest: %.2f", summary.ingestRate);
        out(2, "Last Contact: %s", server.lastContact);
        out(2, "OS Load Average: %.2f", server.osLoad);
        out(2, "Queries: %.2f", summary.queryRate);
        out(2, "Time Difference: %.1f", ((now - server.lastContact) / 1000.));
        out(2, "Total Records: %d", summary.recs);
        out(2, "Lookups: %d", server.lookups);
        if (server.holdTime > 0)
          out(2, "Hold Time: %d", server.holdTime);
        if (server.tableMap != null && server.tableMap.size() > 0) {
          out(2, "Tables");
          for (Entry<String,TableInfo> status : server.tableMap.entrySet()) {
            TableInfo info = status.getValue();
            out(3, "Table: %s", status.getKey());
            out(4, "Tablets: %d", info.onlineTablets);
            out(4, "Records: %d", info.recs);
            out(4, "Records in Memory: %d", info.recsInMemory);
            out(4, "Ingest: %.2f", info.ingestRate);
            out(4, "Queries: %.2f", info.queryRate);
            out(4, "Major Compacting: %d", info.majors == null ? 0 : info.majors.running);
            out(4, "Queued for Major Compaction: %d", info.majors == null ? 0 : info.majors.queued);
            out(4, "Minor Compacting: %d", info.minors == null ? 0 : info.minors.running);
            out(4, "Queued for Minor Compaction: %d", info.minors == null ? 0 : info.minors.queued);
          }
        }
        out(2, "Recoveries: %d", server.logSorts.size());
        for (RecoveryStatus sort : server.logSorts) {
          out(3, "File: %s", sort.name);
          out(3, "Progress: %.2f%%", sort.progress * 100);
          out(3, "Time running: %s", sort.runtime / 1000.);
        }
      }
    }
  }

  private static void out(int indent, String string, Object... args) {
    for (int i = 0; i < indent; i++) {
      System.out.print(" ");
    }
    System.out.println(String.format(string, args));
  }

}

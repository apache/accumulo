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
package org.apache.accumulo.server.test;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.MasterNotRunningException;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.RecoveryStatus;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.monitor.Monitor;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.thrift.transport.TTransportException;

public class GetMasterStats {
  /**
   * @param args
   * @throws MasterNotRunningException
   * @throws IOException
   * @throws TTransportException
   */
  public static void main(String[] args) throws Exception {
    MasterClientService.Iface client = null;
    MasterMonitorInfo stats = null;
    try {
      client = MasterClient.getConnectionWithRetry(HdfsZooInstance.getInstance());
      stats = client.getMasterStats(null, SecurityConstants.getSystemCredentials());
    } finally {
      if (client != null)
        MasterClient.close(client);
    }
    out(0, "State: " + stats.state.name());
    out(0, "Goal State: " + stats.goalState.name());
    if (stats.serversShuttingDown != null && stats.serversShuttingDown.size() > 0) {
      out(0, "Servers to shutdown: ");
      for (String server : stats.serversShuttingDown) {
        out(1, "%s", server);
      }
    }
    out(1, "Unassigned tablets: %d", stats.unassignedTablets);
    if (stats.badTServers != null && stats.badTServers.size() > 0) {
      out(0, "Bad servers");
      
      for (Entry<String,Byte> entry : stats.badTServers.entrySet()) {
        out(1, "%s: %d", entry.getKey(), (int) entry.getValue());
      }
    }
    if (stats.tableMap != null && stats.tableMap.size() > 0) {
      out(0, "Tables");
      for (Entry<String,TableInfo> entry : stats.tableMap.entrySet()) {
        TableInfo v = entry.getValue();
        out(1, "%s", entry.getKey());
        out(2, "Records: %d", v.recs);
        out(2, "Records in Memory: %d", v.recsInMemory);
        out(2, "Tablets %d", v.tablets);
        out(2, "Online Tablets %d", v.onlineTablets);
        out(2, "Ingest Rate %.2f", v.ingestRate);
        out(2, "Query Rate %.2f", v.queryRate);
      }
    }
    if (stats.tServerInfo != null && stats.tServerInfo.size() > 0) {
      out(0, "Tablet Servers");
      long now = System.currentTimeMillis();
      for (TabletServerStatus server : stats.tServerInfo) {
        TableInfo summary = Monitor.summarizeTableStats(server);
        out(1, "Name %s", server.name);
        out(2, "Ingest %.2f", summary.ingestRate);
        out(2, "Last Contact %s", server.lastContact);
        out(2, "OS Load Average %.2f", server.osLoad);
        out(2, "Queries %.2f", summary.queryRate);
        out(2, "Time Difference %.1f", ((now - server.lastContact) / 1000.));
        out(2, "Total Records %d", summary.recs);
        out(2, "Lookups %d", server.lookups);
        out(2, "Loggers %d", server.loggers.size());
        for (String logger : server.loggers)
          out(3, "Logger %s", logger);
        if (server.holdTime > 0)
          out(2, "Hold Time %d", server.holdTime);
        if (server.tableMap != null && server.tableMap.size() > 0) {
          out(2, "Tables");
          for (Entry<String,TableInfo> status : server.tableMap.entrySet()) {
            TableInfo info = status.getValue();
            out(3, "Table %s", status.getKey());
            out(4, "Tablets %d", info.onlineTablets);
            out(4, "Records %d", info.recs);
            out(4, "Records in Memory %d", info.recsInMemory);
            out(4, "Ingest %.2f", info.ingestRate);
            out(4, "Queries %.2f", info.queryRate);
            out(4, "Major Compacting %d", info.major == null ? 0 : info.major.running);
            out(4, "Queued for Major Compaction %d", info.major == null ? 0 : info.major.queued);
            out(4, "Minor Compacting %d", info.minor == null ? 0 : info.minor.running);
            out(4, "Queued for Minor Compaction %d", info.minor == null ? 0 : info.minor.queued);
          }
        }
      }
    }
    if (stats.recovery != null && stats.recovery.size() > 0) {
      out(0, "Recovery");
      for (RecoveryStatus r : stats.recovery) {
        out(1, "Log Server %s", r.host);
        out(1, "Log Name %s", r.name);
        out(1, "Map Progress: %.2f%%", r.mapProgress * 100);
        out(1, "Reduce Progress: %.2f%%", r.reduceProgress * 100);
        out(1, "Time running: %s", r.runtime / 1000.);
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

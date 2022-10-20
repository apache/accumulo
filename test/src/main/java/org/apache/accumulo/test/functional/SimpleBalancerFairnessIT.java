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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class SimpleBalancerFairnessIT extends ConfigurableMacBase {

  private static final int NUM_SPLITS = 50;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAXMEM, "1K");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "50ms");
    cfg.setMemory(ServerType.TABLET_SERVER, cfg.getMemory(ServerType.TABLET_SERVER) * 3,
        MemoryUnit.BYTE);
  }

  @Test
  public void simpleBalancerFairness() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.tableOperations().create("test_ingest");
      c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "1K");
      c.tableOperations().create("unused");
      TreeSet<Text> splits = TestIngest.getSplitPoints(0, 10000000, NUM_SPLITS);
      log.info("Creating {} splits", splits.size());
      c.tableOperations().addSplits("unused", splits);
      List<String> tservers = c.instanceOperations().getTabletServers();
      TestIngest.IngestParams params = new TestIngest.IngestParams(getClientProperties());
      params.rows = 5000;
      TestIngest.ingest(c, params);
      c.tableOperations().flush("test_ingest", null, null, false);
      sleepUninterruptibly(45, TimeUnit.SECONDS);
      Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));

      int unassignedTablets = 1;
      ManagerMonitorInfo stats = null;
      ClientContext context = (ClientContext) c;
      for (int i = 0; unassignedTablets > 0 && i < 20; i++) {
        stats = ThriftClientTypes.MANAGER.execute(context,
            client -> client.getManagerStats(TraceUtil.traceInfo(),
                creds.toThrift(c.instanceOperations().getInstanceId())));
        unassignedTablets = stats.getUnassignedTablets();
        if (unassignedTablets > 0) {
          log.info("Found {} unassigned tablets, sleeping 3 seconds for tablet assignment",
              unassignedTablets);
          Thread.sleep(3000);
        }
      }

      assertEquals(0, unassignedTablets, "Unassigned tablets were not assigned within 60 seconds");

      // Compute online tablets per tserver
      List<Integer> counts = new ArrayList<>();
      for (TabletServerStatus server : stats.tServerInfo) {
        int count = 0;
        for (TableInfo table : server.tableMap.values()) {
          count += table.onlineTablets;
        }
        counts.add(count);
      }
      assertTrue(counts.size() > 1, "Expected to have at least two TabletServers");
      for (int i = 1; i < counts.size(); i++) {
        int diff = Math.abs(counts.get(0) - counts.get(i));
        assertTrue(diff <= tservers.size(),
            "Expected difference in tablets to be less than or equal to " + counts.size()
                + " but was " + diff + ". Counts " + counts);
      }
    }
  }

}

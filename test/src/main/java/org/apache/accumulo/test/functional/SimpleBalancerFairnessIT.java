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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class SimpleBalancerFairnessIT extends ConfigurableMacBase {

  private static final int NUM_SPLITS = 50;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAXMEM, "1K");
    cfg.setMemory(ServerType.TABLET_SERVER, cfg.getMemory(ServerType.TABLET_SERVER) * 3,
        MemoryUnit.BYTE);
  }

  @Test
  public void simpleBalancerFairness() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      final String ingestTable = "test_ingest";
      final String unusedTable = "unused";

      c.tableOperations().create(ingestTable);
      c.tableOperations().setProperty(ingestTable, Property.TABLE_SPLIT_THRESHOLD.getKey(), "1K");
      c.tableOperations().create(unusedTable);
      TreeSet<Text> splits = TestIngest.getSplitPoints(0, 10_000_000, NUM_SPLITS);
      log.info("Creating {} splits", splits.size());
      c.tableOperations().addSplits(unusedTable, splits);
      Set<ServerId> tservers = c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
      TestIngest.IngestParams params = new TestIngest.IngestParams(getClientProperties());
      params.rows = 5000;
      TestIngest.ingest(c, params);
      c.tableOperations().flush(ingestTable, null, null, false);
      Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));

      ClientContext context = (ClientContext) c;

      // wait for tablet assignment
      Wait.waitFor(() -> {
        ManagerMonitorInfo stats = ThriftClientTypes.MANAGER.execute(context, client -> client
            .getManagerStats(creds.toThrift(c.instanceOperations().getInstanceId())));
        int unassignedTablets = stats.getUnassignedTablets();
        if (unassignedTablets > 0) {
          log.info("Found {} unassigned tablets, sleeping 3 seconds for tablet assignment",
              unassignedTablets);
          return false;
        } else {
          return true;
        }
      }, SECONDS.toMillis(45), SECONDS.toMillis(3));

      // wait for tablets to be balanced
      Wait.waitFor(() -> {
        ManagerMonitorInfo stats = ThriftClientTypes.MANAGER.execute(context, client -> client
            .getManagerStats(creds.toThrift(c.instanceOperations().getInstanceId())));

        List<Integer> counts = new ArrayList<>();
        for (TabletServerStatus server : stats.tServerInfo) {
          int count = 0;
          for (TableInfo table : server.tableMap.values()) {
            count += table.onlineTablets;
          }
          counts.add(count);
        }
        assertTrue(counts.size() >= 2,
            "Expected at least 2 tservers to have tablets, but found " + counts);

        for (int i = 1; i < counts.size(); i++) {
          int diff = Math.abs(counts.get(0) - counts.get(i));
          log.info(" Counts: {}", counts);
          if (diff > tservers.size()) {
            log.info("Difference in tablets between tservers is greater than expected. Counts: {}",
                counts);
            return false;
          }
        }

        // if diff is less than the number of tservers, then we are good
        return true;
      }, SECONDS.toMillis(60), SECONDS.toMillis(3));
    }
  }

}

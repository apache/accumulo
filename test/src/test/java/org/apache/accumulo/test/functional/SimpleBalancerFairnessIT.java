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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class SimpleBalancerFairnessIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "0");
    cfg.setSiteConfig(siteConfig);
    cfg.setMemory(ClusterServerType.TABLET_SERVER, cfg.getMemory(ClusterServerType.TABLET_SERVER) * 3, MemoryUnit.BYTE);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  @Test
  public void simpleBalancerFairness() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    c.tableOperations().create("unused");
    TreeSet<Text> splits = TestIngest.getSplitPoints(0, 10000000, 500);
    log.info("Creating " + splits.size() + " splits");
    c.tableOperations().addSplits("unused", splits);
    List<String> tservers = c.instanceOperations().getTabletServers();
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = 50000;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    c.tableOperations().flush("test_ingest", null, null, false);
    UtilWaitThread.sleep(45 * 1000);
    Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));

    MasterMonitorInfo stats = null;
    int unassignedTablets = 1;
    for (int i = 0; unassignedTablets > 0 && i < 10; i++) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(c.getInstance());
        stats = client.getMasterStats(Tracer.traceInfo(), creds.toThrift(c.getInstance()));
      } finally {
        if (client != null)
          MasterClient.close(client);
      }
      unassignedTablets = stats.getUnassignedTablets();
      if (unassignedTablets > 0) {
        log.info("Found " + unassignedTablets + " unassigned tablets, sleeping 3 seconds for tablet assignment");
        Thread.sleep(3000);
      }
    }

    assertEquals("Unassigned tablets were not assigned within 30 seconds", 0, unassignedTablets);

    // Compute online tablets per tserver
    List<Integer> counts = new ArrayList<Integer>();
    for (TabletServerStatus server : stats.tServerInfo) {
      int count = 0;
      for (TableInfo table : server.tableMap.values()) {
        count += table.onlineTablets;
      }
      counts.add(count);
    }
    assertTrue("Expected to have at least two TabletServers", counts.size() > 1);
    for (int i = 1; i < counts.size(); i++) {
      int diff = Math.abs(counts.get(0) - counts.get(i));
      assertTrue("Expected difference in tablets to be less than or equal to " + counts.size() + " but was " + diff + ". Counts " + counts,
          diff <= tservers.size());
    }
  }

}

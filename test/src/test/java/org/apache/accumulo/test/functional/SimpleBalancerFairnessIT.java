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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.trace.instrument.Tracer;
import org.junit.Test;

public class SimpleBalancerFairnessIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String,String> siteConfig = new HashMap<String, String>();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "0");
    cfg.setSiteConfig(siteConfig );
  }
  
  @Test(timeout=120*1000)
  public void simpleBalancerFairness() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    c.tableOperations().create("unused");
    c.tableOperations().addSplits("unused", TestIngest.getSplitPoints(0, 10000000, 2000));
    List<String> tservers = c.instanceOperations().getTabletServers();
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = 200000;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    c.tableOperations().flush("test_ingest", null, null, false);
    UtilWaitThread.sleep(15*1000);
    TCredentials creds = CredentialHelper.create("root", new PasswordToken(MacTest.PASSWORD), c.getInstance().getInstanceName());
    
    MasterClientService.Iface client = null;
    MasterMonitorInfo stats = null;
    try {
      client = MasterClient.getConnectionWithRetry(c.getInstance());
      stats = client.getMasterStats(Tracer.traceInfo(), creds);
    } finally {
      if (client != null)
        MasterClient.close(client);
    }
    List<Integer> counts = new ArrayList<Integer>();
    for (TabletServerStatus server: stats.tServerInfo) {
      int count = 0;
      for (TableInfo table : server.tableMap.values()) {
        count += table.onlineTablets;
      }
      counts.add(count);
    }
    assertTrue(counts.size() > 1);
    for (int i = 1; i < counts.size(); i++)
      assertTrue(Math.abs(counts.get(0) - counts.get(i)) <= tservers.size());
  }
  
}

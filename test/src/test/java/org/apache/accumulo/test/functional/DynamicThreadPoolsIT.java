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

import java.util.Collections;

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

public class DynamicThreadPoolsIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setSiteConfig(Collections.singletonMap(Property.TSERV_MAJC_DELAY.getKey(), "100ms"));
  }
  
  @Test(timeout = 30 * 1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.instanceOperations().setProperty(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), "1");
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = 100*1000;
    opts.createTable = true;
    TestIngest.ingest(c, opts, BWOPTS);
    c.tableOperations().flush("test_ingest", null, null, true);
    c.tableOperations().clone("test_ingest", "test_ingest2", true, null, null);
    c.tableOperations().clone("test_ingest", "test_ingest3", true, null, null);
    c.tableOperations().clone("test_ingest", "test_ingest4", true, null, null);
    c.tableOperations().clone("test_ingest", "test_ingest5", true, null, null);
    c.tableOperations().clone("test_ingest", "test_ingest6", true, null, null);
    
    TCredentials creds = CredentialHelper.create("root", new PasswordToken(MacTest.PASSWORD), c.getInstance().getInstanceName());
    UtilWaitThread.sleep(10);
    for (int i = 2; i < 7; i++)
      c.tableOperations().compact("test_ingest" + i, null, null, true, false);
    int count = 0;
    while (count == 0) {
      MasterClientService.Iface client = null;
      MasterMonitorInfo stats = null;
      try {
        client = MasterClient.getConnectionWithRetry(c.getInstance());
        stats = client.getMasterStats(Tracer.traceInfo(), creds);
      } finally {
        if (client != null)
          MasterClient.close(client);
      }
      for (TabletServerStatus server : stats.tServerInfo) {
        for (TableInfo table : server.tableMap.values()) {
          count += table.majors.running;
        }
      }
      System.out.println("count " + count);
      UtilWaitThread.sleep(1000);
    }
    assertTrue(count == 1 || count == 2); // sometimes we get two threads due to the way the stats are pulled
  }
  
}

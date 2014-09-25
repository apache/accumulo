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

import static org.junit.Assert.fail;

import java.util.Collections;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.ManagedAccumuloIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class DynamicThreadPoolsIT extends ManagedAccumuloIT {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setSiteConfig(Collections.singletonMap(Property.TSERV_MAJC_DELAY.getKey(), "100ms"));
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void test() throws Exception {
    final int TABLES = 15;
    Connector c = getConnector();
    c.instanceOperations().setProperty(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), "5");
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = 500 * 1000;
    opts.createTable = true;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    c.tableOperations().flush("test_ingest", null, null, true);
    for (int i = 1; i < TABLES; i++)
      c.tableOperations().clone("test_ingest", "test_ingest" + i, true, null, null);
    UtilWaitThread.sleep(11 * 1000); // time between checks of the thread pool sizes
    Credentials creds = new Credentials(getPrincipal(), getToken());
    for (int i = 1; i < TABLES; i++)
      c.tableOperations().compact("test_ingest" + i, null, null, true, false);
    for (int i = 0; i < 30; i++) {
      int count = 0;
      MasterClientService.Iface client = null;
      MasterMonitorInfo stats = null;
      try {
        client = MasterClient.getConnectionWithRetry(c.getInstance());
        stats = client.getMasterStats(Tracer.traceInfo(), creds.toThrift(c.getInstance()));
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
      if (count > 3)
        return;
      UtilWaitThread.sleep(500);
    }
    fail("Could not observe higher number of threads after changing the config");
  }
}

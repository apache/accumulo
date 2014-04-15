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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class RestartStressIT extends ConfigurableMacIT {
  
  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String, String> opts = new HashMap<String, String>();
    opts.put(Property.TSERV_MAXMEM.getKey(), "100K");
    opts.put(Property.TSERV_MAJC_DELAY.getKey(), "100ms");
    opts.put(Property.TSERV_WALOG_MAX_SIZE.getKey(), "1M");
    opts.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "5s");
    opts.put(Property.MASTER_RECOVERY_DELAY.getKey(), "1s");
    cfg.setSiteConfig(opts);
    cfg.useMiniDFS(true);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  private static final TestIngest.Opts IOPTS;
  private static final VerifyIngest.Opts VOPTS;
  static {
    IOPTS = new TestIngest.Opts();
    VOPTS = new VerifyIngest.Opts();
    IOPTS.rows = VOPTS.rows = 10*1000;
  }
  private static final ScannerOpts SOPTS = new ScannerOpts();
  
  
  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "500K");
    Process ingest = cluster.exec(TestIngest.class, 
        "-u", "root", "-p", ROOT_PASSWORD, 
        "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(), 
        "--rows", "" + IOPTS.rows);
    for (int i = 0; i < 2; i++) {
      UtilWaitThread.sleep(10*1000);
      cluster.killProcess(ServerType.TABLET_SERVER, cluster.getProcesses().get(ServerType.TABLET_SERVER).iterator().next());
      cluster.start();
    }
    assertEquals(0, ingest.waitFor());
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }
  
}

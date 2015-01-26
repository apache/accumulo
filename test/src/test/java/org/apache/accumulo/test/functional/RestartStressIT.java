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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

public class RestartStressIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(RestartStressIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> opts = new HashMap<String,String>();
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

  private ExecutorService svc;

  @Before
  public void setup() throws Exception {
    svc = Executors.newFixedThreadPool(1);
  }

  @After
  public void teardown() throws Exception {
    if (null == svc) {
      return;
    }

    if (!svc.isShutdown()) {
      svc.shutdown();
    }

    while (!svc.awaitTermination(10, TimeUnit.SECONDS)) {
      log.info("Waiting for threadpool to terminate");
    }
  }

  private static final TestIngest.Opts IOPTS;
  private static final VerifyIngest.Opts VOPTS;
  static {
    IOPTS = new TestIngest.Opts();
    VOPTS = new VerifyIngest.Opts();
    IOPTS.rows = VOPTS.rows = 10 * 1000;
  }
  private static final ScannerOpts SOPTS = new ScannerOpts();

  @Test
  public void test() throws Exception {
    final Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    final PasswordToken token = (PasswordToken) getToken();
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "500K");
    final ClusterControl control = getCluster().getClusterControl();
    Future<Integer> retCode = svc.submit(new Callable<Integer>() {
      @Override
      public Integer call() {
        try {
          return control.exec(TestIngest.class,
              new String[] {"-u", "root", "-p", new String(token.getPassword(), Charsets.UTF_8), "-i", cluster.getInstanceName(), "-z",
                  cluster.getZooKeepers(), "--rows", "" + IOPTS.rows, "--table", tableName});
        } catch (Exception e) {
          log.error("Error running TestIngest", e);
          return -1;
        }
      }
    });

    for (int i = 0; i < 2; i++) {
      UtilWaitThread.sleep(10 * 1000);
      control.stopAllServers(ClusterServerType.TABLET_SERVER);
      control.startAllServers(ClusterServerType.TABLET_SERVER);
    }
    assertEquals(0, retCode.get().intValue());
    VOPTS.tableName = tableName;
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }

}

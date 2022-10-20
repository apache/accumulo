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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestartStressIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(RestartStressIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> opts = cfg.getSiteConfig();
    opts.put(Property.TSERV_MAXMEM.getKey(), "100K");
    opts.put(Property.TSERV_MAJC_DELAY.getKey(), "100ms");
    opts.put(Property.TSERV_WAL_MAX_SIZE.getKey(), "1M");
    opts.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "15s");
    opts.put(Property.MANAGER_RECOVERY_DELAY.getKey(), "1s");
    cfg.setSiteConfig(opts);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private ExecutorService svc;

  @BeforeEach
  public void setup() {
    svc = Executors.newFixedThreadPool(1);
  }

  @AfterEach
  public void teardown() throws Exception {
    if (svc == null) {
      return;
    }

    if (!svc.isShutdown()) {
      svc.shutdown();
    }

    while (!svc.awaitTermination(10, TimeUnit.SECONDS)) {
      log.info("Waiting for threadpool to terminate");
    }
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "500K");
      final ClusterControl control = getCluster().getClusterControl();

      VerifyParams params = new VerifyParams(getClientProps(), tableName, 10_000);

      Future<Integer> retCode = svc.submit(() -> {
        try {
          return control.exec(TestIngest.class, new String[] {"-c", cluster.getClientPropsPath(),
              "--rows", "" + params.rows, "--table", tableName});
        } catch (Exception e) {
          log.error("Error running TestIngest", e);
          return -1;
        }
      });

      for (int i = 0; i < 2; i++) {
        sleepUninterruptibly(10, TimeUnit.SECONDS);
        control.stopAllServers(ServerType.TABLET_SERVER);
        control.startAllServers(ServerType.TABLET_SERVER);
      }
      assertEquals(0, retCode.get().intValue());
      VerifyIngest.verifyIngest(c, params);
    }
  }
}

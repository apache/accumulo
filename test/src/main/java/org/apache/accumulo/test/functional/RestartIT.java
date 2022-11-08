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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestartIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(RestartIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
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
  public void restartManager() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      final ClusterControl control = getCluster().getClusterControl();

      VerifyParams params = new VerifyParams(getClientProps(), tableName, 10_000);

      Future<Integer> ret = svc.submit(() -> {
        try {
          return control.exec(TestIngest.class, new String[] {"-c", cluster.getClientPropsPath(),
              "--rows", "" + params.rows, "--table", tableName});
        } catch (IOException e) {
          log.error("Error running TestIngest", e);
          return -1;
        }
      });

      control.stopAllServers(ServerType.MANAGER);
      control.startAllServers(ServerType.MANAGER);
      assertEquals(0, ret.get().intValue());
      VerifyIngest.verifyIngest(c, params);
    }
  }

  @Test
  public void restartManagerRecovery() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      VerifyParams params = new VerifyParams(getClientProps(), tableName, 10_000);
      TestIngest.ingest(c, params);
      ClusterControl control = getCluster().getClusterControl();

      // TODO implement a kill all too?
      // cluster.stop() would also stop ZooKeeper
      control.stopAllServers(ServerType.MANAGER);
      control.stopAllServers(ServerType.TABLET_SERVER);
      control.stopAllServers(ServerType.GARBAGE_COLLECTOR);
      control.stopAllServers(ServerType.MONITOR);

      ZooCache zcache = cluster.getServerContext().getZooCache();
      var zLockPath = ServiceLock
          .path(ZooUtil.getRoot(c.instanceOperations().getInstanceId()) + Constants.ZMANAGER_LOCK);
      byte[] managerLockData;
      do {
        managerLockData = ServiceLock.getLockData(zcache, zLockPath, null);
        if (managerLockData != null) {
          log.info("Manager lock is still held");
          Thread.sleep(1000);
        }
      } while (managerLockData != null);

      cluster.start();
      sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
      control.stopAllServers(ServerType.MANAGER);

      managerLockData = new byte[0];
      do {
        managerLockData = ServiceLock.getLockData(zcache, zLockPath, null);
        if (managerLockData != null) {
          log.info("Manager lock is still held");
          Thread.sleep(1000);
        }
      } while (managerLockData != null);
      cluster.start();
      VerifyIngest.verifyIngest(c, params);
    }
  }

  @Test
  public void restartManagerSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      final ClusterControl control = getCluster().getClusterControl();
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "5K");

      VerifyParams params = new VerifyParams(getClientProps(), tableName, 10_000);

      Future<Integer> ret = svc.submit(() -> {
        try {
          return control.exec(TestIngest.class, new String[] {"-c", cluster.getClientPropsPath(),
              "--rows", "" + params.rows, "--table", tableName});
        } catch (Exception e) {
          log.error("Error running TestIngest", e);
          return -1;
        }
      });

      control.stopAllServers(ServerType.MANAGER);

      ZooCache zcache = cluster.getServerContext().getZooCache();
      var zLockPath = ServiceLock
          .path(ZooUtil.getRoot(c.instanceOperations().getInstanceId()) + Constants.ZMANAGER_LOCK);
      byte[] managerLockData;
      do {
        managerLockData = ServiceLock.getLockData(zcache, zLockPath, null);
        if (managerLockData != null) {
          log.info("Manager lock is still held");
          Thread.sleep(1000);
        }
      } while (managerLockData != null);

      cluster.start();
      assertEquals(0, ret.get().intValue());
      VerifyIngest.verifyIngest(c, params);
    }
  }

  @Test
  public void killedTabletServer() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      VerifyParams params = new VerifyParams(getClientProps(), tableName, 10_000);
      TestIngest.ingest(c, params);
      VerifyIngest.verifyIngest(c, params);
      cluster.getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      cluster.start();
      VerifyIngest.verifyIngest(c, params);
    }
  }

  @Test
  public void killedTabletServer2() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String[] names = getUniqueNames(2);
      final String tableName = names[0];
      final ClusterControl control = getCluster().getClusterControl();
      c.tableOperations().create(tableName);
      // Original test started and then stopped a GC. Not sure why it did this. The GC was
      // already running by default, and it would have nothing to do after only creating a table
      control.stopAllServers(ServerType.TABLET_SERVER);

      cluster.start();
      c.tableOperations().create(names[1]);
    }
  }

  @Test
  public void killedTabletServerDuringShutdown() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      IngestParams params = new IngestParams(getClientProps(), tableName, 10_000);
      TestIngest.ingest(c, params);
      try {
        getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getCluster().getClusterControl().adminStopAll();
      } finally {
        // make sure processes are cleaned up before we restart clean for other tests
        getCluster().stop();
        getCluster().start();
      }
    }
  }

  @Test
  public void shutdownDuringCompactingSplitting() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      VerifyParams params = new VerifyParams(getClientProps(), tableName, 10_000);
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
      String splitThreshold = null;
      for (Entry<String,String> entry : c.tableOperations().getProperties(tableName)) {
        if (entry.getKey().equals(Property.TABLE_SPLIT_THRESHOLD.getKey())) {
          splitThreshold = entry.getValue();
          break;
        }
      }
      assertNotNull(splitThreshold);
      try {
        c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(),
            "20K");
        TestIngest.ingest(c, params);
        c.tableOperations().flush(tableName, null, null, false);
        VerifyIngest.verifyIngest(c, params);
        getCluster().stop();
      } finally {
        if (getClusterType() == ClusterType.STANDALONE) {
          getCluster().start();
          c.tableOperations().setProperty(MetadataTable.NAME,
              Property.TABLE_SPLIT_THRESHOLD.getKey(), splitThreshold);
        }
      }
    }
  }
}

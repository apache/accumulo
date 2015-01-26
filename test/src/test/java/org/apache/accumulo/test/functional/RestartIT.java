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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

public class RestartIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(RestartIT.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> props = new HashMap<String,String>();
    props.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "5s");
    props.put(Property.GC_CYCLE_DELAY.getKey(), "1s");
    props.put(Property.GC_CYCLE_START.getKey(), "1s");
    cfg.setSiteConfig(props);
    cfg.useMiniDFS(true);
  }

  private static final ScannerOpts SOPTS = new ScannerOpts();
  private static final VerifyIngest.Opts VOPTS = new VerifyIngest.Opts();
  private static final TestIngest.Opts OPTS = new TestIngest.Opts();
  private static final BatchWriterOpts BWOPTS = new BatchWriterOpts();
  static {
    OPTS.rows = VOPTS.rows = 10 * 1000;
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

  @Test
  public void restartMaster() throws Exception {
    Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    OPTS.tableName = tableName;
    VOPTS.tableName = tableName;
    c.tableOperations().create(tableName);
    final PasswordToken token = (PasswordToken) getToken();
    final ClusterControl control = getCluster().getClusterControl();

    Future<Integer> ret = svc.submit(new Callable<Integer>() {
      @Override
      public Integer call() {
        try {
          return control.exec(TestIngest.class,
              new String[] {"-u", "root", "-p", new String(token.getPassword(), Charsets.UTF_8), "-i", cluster.getInstanceName(), "-z",
                  cluster.getZooKeepers(), "--rows", "" + OPTS.rows, "--table", tableName});
        } catch (IOException e) {
          log.error("Error running TestIngest", e);
          return -1;
        }
      }
    });

    control.stopAllServers(ClusterServerType.MASTER);
    control.startAllServers(ClusterServerType.MASTER);
    assertEquals(0, ret.get().intValue());
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }

  @Test
  public void restartMasterRecovery() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    OPTS.tableName = tableName;
    VOPTS.tableName = tableName;
    TestIngest.ingest(c, OPTS, BWOPTS);
    ClusterControl control = getCluster().getClusterControl();

    // TODO implement a kill all too?
    // cluster.stop() would also stop ZooKeeper
    control.stopAllServers(ClusterServerType.MASTER);
    control.stopAllServers(ClusterServerType.TRACER);
    control.stopAllServers(ClusterServerType.TABLET_SERVER);
    control.stopAllServers(ClusterServerType.GARBAGE_COLLECTOR);
    control.stopAllServers(ClusterServerType.MONITOR);

    ZooReader zreader = new ZooReader(c.getInstance().getZooKeepers(), c.getInstance().getZooKeepersSessionTimeOut());
    ZooCache zcache = new ZooCache(zreader, null);
    byte[] masterLockData;
    do {
      masterLockData = ZooLock.getLockData(zcache, ZooUtil.getRoot(c.getInstance()) + Constants.ZMASTER_LOCK, null);
      if (null != masterLockData) {
        log.info("Master lock is still held");
        Thread.sleep(1000);
      }
    } while (null != masterLockData);

    cluster.start();
    UtilWaitThread.sleep(5);
    control.stopAllServers(ClusterServerType.MASTER);

    masterLockData = new byte[0];
    do {
      masterLockData = ZooLock.getLockData(zcache, ZooUtil.getRoot(c.getInstance()) + Constants.ZMASTER_LOCK, null);
      if (null != masterLockData) {
        log.info("Master lock is still held");
        Thread.sleep(1000);
      }
    } while (null != masterLockData);
    cluster.start();
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }

  @Test
  public void restartMasterSplit() throws Exception {
    Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    final PasswordToken token = (PasswordToken) getToken();
    final ClusterControl control = getCluster().getClusterControl();
    VOPTS.tableName = tableName;
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "5K");
    Future<Integer> ret = svc.submit(new Callable<Integer>() {
      @Override
      public Integer call() {
        try {
          return control.exec(TestIngest.class,
              new String[] {"-u", "root", "-p", new String(token.getPassword(), Charsets.UTF_8), "-i", cluster.getInstanceName(), "-z",
                  cluster.getZooKeepers(), "--rows", Integer.toString(VOPTS.rows), "--table", tableName});
        } catch (Exception e) {
          log.error("Error running TestIngest", e);
          return -1;
        }
      }
    });

    control.stopAllServers(ClusterServerType.MASTER);

    ZooReader zreader = new ZooReader(c.getInstance().getZooKeepers(), c.getInstance().getZooKeepersSessionTimeOut());
    ZooCache zcache = new ZooCache(zreader, null);
    byte[] masterLockData;
    do {
      masterLockData = ZooLock.getLockData(zcache, ZooUtil.getRoot(c.getInstance()) + Constants.ZMASTER_LOCK, null);
      if (null != masterLockData) {
        log.info("Master lock is still held");
        Thread.sleep(1000);
      }
    } while (null != masterLockData);

    cluster.start();
    assertEquals(0, ret.get().intValue());
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }

  @Test
  public void killedTabletServer() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    OPTS.tableName = tableName;
    VOPTS.tableName = tableName;
    TestIngest.ingest(c, OPTS, BWOPTS);
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
    cluster.getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
    cluster.start();
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }

  @Test
  public void killedTabletServer2() throws Exception {
    final Connector c = getConnector();
    final String[] names = getUniqueNames(2);
    final String tableName = names[0];
    final ClusterControl control = getCluster().getClusterControl();
    c.tableOperations().create(tableName);
    // Original test started and then stopped a GC. Not sure why it did this. The GC was
    // already running by default, and it would have nothing to do after only creating a table
    control.stopAllServers(ClusterServerType.TABLET_SERVER);

    cluster.start();
    c.tableOperations().create(names[1]);
  }

  @Test
  public void killedTabletServerDuringShutdown() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    OPTS.tableName = tableName;
    TestIngest.ingest(c, OPTS, BWOPTS);
    try {
      getCluster().getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
      getCluster().getClusterControl().adminStopAll();
    } finally {
      getCluster().start();
    }
  }

  @Test
  public void shutdownDuringCompactingSplitting() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    VOPTS.tableName = tableName;
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    String splitThreshold = null;
    for (Entry<String,String> entry : c.tableOperations().getProperties(tableName)) {
      if (entry.getKey().equals(Property.TABLE_SPLIT_THRESHOLD.getKey())) {
        splitThreshold = entry.getValue();
        break;
      }
    }
    Assert.assertNotNull(splitThreshold);
    try {
      c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), "20K");
      TestIngest.Opts opts = new TestIngest.Opts();
      opts.tableName = tableName;
      TestIngest.ingest(c, opts, BWOPTS);
      c.tableOperations().flush(tableName, null, null, false);
      VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
      getCluster().stop();
    } finally {
      if (getClusterType() == ClusterType.STANDALONE) {
        getCluster().start();
        c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), splitThreshold);
      }
    }
  }
}

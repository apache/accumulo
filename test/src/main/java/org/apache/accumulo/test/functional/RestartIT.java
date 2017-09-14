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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestartIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(RestartIT.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
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
    OPTS.setTableName(tableName);
    VOPTS.setTableName(tableName);
    c.tableOperations().create(tableName);
    final AuthenticationToken token = getAdminToken();
    final ClusterControl control = getCluster().getClusterControl();

    final String[] args;
    if (token instanceof PasswordToken) {
      byte[] password = ((PasswordToken) token).getPassword();
      args = new String[] {"-u", getAdminPrincipal(), "-p", new String(password, UTF_8), "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(),
          "--rows", "" + OPTS.rows, "--table", tableName};
      OPTS.setPrincipal(getAdminPrincipal());
      VOPTS.setPrincipal(getAdminPrincipal());
    } else if (token instanceof KerberosToken) {
      ClusterUser rootUser = getAdminUser();
      args = new String[] {"-u", getAdminPrincipal(), "--keytab", rootUser.getKeytab().getAbsolutePath(), "-i", cluster.getInstanceName(), "-z",
          cluster.getZooKeepers(), "--rows", "" + OPTS.rows, "--table", tableName};
      ClientConfiguration clientConfig = cluster.getClientConfig();
      OPTS.updateKerberosCredentials(clientConfig);
      VOPTS.updateKerberosCredentials(clientConfig);
    } else {
      throw new RuntimeException("Unknown token");
    }

    Future<Integer> ret = svc.submit(new Callable<Integer>() {
      @Override
      public Integer call() {
        try {
          return control.exec(TestIngest.class, args);
        } catch (IOException e) {
          log.error("Error running TestIngest", e);
          return -1;
        }
      }
    });

    control.stopAllServers(ServerType.MASTER);
    control.startAllServers(ServerType.MASTER);
    assertEquals(0, ret.get().intValue());
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
  }

  @Test
  public void restartMasterRecovery() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    OPTS.setTableName(tableName);
    VOPTS.setTableName(tableName);
    ClientConfiguration clientConfig = cluster.getClientConfig();
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      OPTS.updateKerberosCredentials(clientConfig);
      VOPTS.updateKerberosCredentials(clientConfig);
    } else {
      OPTS.setPrincipal(getAdminPrincipal());
      VOPTS.setPrincipal(getAdminPrincipal());
    }
    TestIngest.ingest(c, OPTS, BWOPTS);
    ClusterControl control = getCluster().getClusterControl();

    // TODO implement a kill all too?
    // cluster.stop() would also stop ZooKeeper
    control.stopAllServers(ServerType.MASTER);
    control.stopAllServers(ServerType.TRACER);
    control.stopAllServers(ServerType.TABLET_SERVER);
    control.stopAllServers(ServerType.GARBAGE_COLLECTOR);
    control.stopAllServers(ServerType.MONITOR);

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
    sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
    control.stopAllServers(ServerType.MASTER);

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
    final AuthenticationToken token = getAdminToken();
    final ClusterControl control = getCluster().getClusterControl();
    VOPTS.setTableName(tableName);
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "5K");

    final String[] args;
    if (token instanceof PasswordToken) {
      byte[] password = ((PasswordToken) token).getPassword();
      args = new String[] {"-u", getAdminPrincipal(), "-p", new String(password, UTF_8), "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers(),
          "--rows", Integer.toString(VOPTS.rows), "--table", tableName};
      OPTS.setPrincipal(getAdminPrincipal());
      VOPTS.setPrincipal(getAdminPrincipal());
    } else if (token instanceof KerberosToken) {
      ClusterUser rootUser = getAdminUser();
      args = new String[] {"-u", getAdminPrincipal(), "--keytab", rootUser.getKeytab().getAbsolutePath(), "-i", cluster.getInstanceName(), "-z",
          cluster.getZooKeepers(), "--rows", Integer.toString(VOPTS.rows), "--table", tableName};
      ClientConfiguration clientConfig = cluster.getClientConfig();
      OPTS.updateKerberosCredentials(clientConfig);
      VOPTS.updateKerberosCredentials(clientConfig);
    } else {
      throw new RuntimeException("Unknown token");
    }

    Future<Integer> ret = svc.submit(new Callable<Integer>() {
      @Override
      public Integer call() {
        try {
          return control.exec(TestIngest.class, args);
        } catch (Exception e) {
          log.error("Error running TestIngest", e);
          return -1;
        }
      }
    });

    control.stopAllServers(ServerType.MASTER);

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
    OPTS.setTableName(tableName);
    VOPTS.setTableName(tableName);
    ClientConfiguration clientConfig = cluster.getClientConfig();
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      OPTS.updateKerberosCredentials(clientConfig);
      VOPTS.updateKerberosCredentials(clientConfig);
    } else {
      OPTS.setPrincipal(getAdminPrincipal());
      VOPTS.setPrincipal(getAdminPrincipal());
    }
    TestIngest.ingest(c, OPTS, BWOPTS);
    VerifyIngest.verifyIngest(c, VOPTS, SOPTS);
    cluster.getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
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
    control.stopAllServers(ServerType.TABLET_SERVER);

    cluster.start();
    c.tableOperations().create(names[1]);
  }

  @Test
  public void killedTabletServerDuringShutdown() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    OPTS.setTableName(tableName);
    ClientConfiguration clientConfig = cluster.getClientConfig();
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      OPTS.updateKerberosCredentials(clientConfig);
    } else {
      OPTS.setPrincipal(getAdminPrincipal());
    }
    TestIngest.ingest(c, OPTS, BWOPTS);
    try {
      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().adminStopAll();
    } finally {
      getCluster().start();
    }
  }

  @Test
  public void shutdownDuringCompactingSplitting() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    VOPTS.setTableName(tableName);
    ClientConfiguration clientConfig = cluster.getClientConfig();
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      OPTS.updateKerberosCredentials(clientConfig);
      VOPTS.updateKerberosCredentials(clientConfig);
    } else {
      OPTS.setPrincipal(getAdminPrincipal());
      VOPTS.setPrincipal(getAdminPrincipal());
    }
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
      opts.setTableName(tableName);
      if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
        opts.updateKerberosCredentials(clientConfig);
      } else {
        opts.setPrincipal(getAdminPrincipal());
      }
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

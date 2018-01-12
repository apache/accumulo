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
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.CheckForMetadataProblems;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(SplitIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAXMEM, "5K");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "100ms");
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private String tservMaxMem, tservMajcDelay;

  @Before
  public void alterConfig() throws Exception {
    Assume.assumeTrue(ClusterType.MINI == getClusterType());

    InstanceOperations iops = getConnector().instanceOperations();
    Map<String,String> config = iops.getSystemConfiguration();
    tservMaxMem = config.get(Property.TSERV_MAXMEM.getKey());
    tservMajcDelay = config.get(Property.TSERV_MAJC_DELAY.getKey());

    if (!tservMajcDelay.equals("100ms")) {
      iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), "100ms");
    }

    // Property.TSERV_MAXMEM can't be altered on a running server
    boolean restarted = false;
    if (!tservMaxMem.equals("5K")) {
      iops.setProperty(Property.TSERV_MAXMEM.getKey(), "5K");
      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      restarted = true;
    }

    // If we restarted the tservers, we don't need to re-wait for the majc delay
    if (!restarted) {
      long millis = ConfigurationTypeHelper.getTimeInMillis(tservMajcDelay);
      log.info("Waiting for majc delay period: {}ms", millis);
      Thread.sleep(millis);
      log.info("Finished waiting for majc delay period");
    }
  }

  @After
  public void resetConfig() throws Exception {
    if (null != tservMaxMem) {
      log.info("Resetting {}={}", Property.TSERV_MAXMEM.getKey(), tservMaxMem);
      getConnector().instanceOperations().setProperty(Property.TSERV_MAXMEM.getKey(), tservMaxMem);
      tservMaxMem = null;
      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
    }
    if (null != tservMajcDelay) {
      log.info("Resetting {}={}", Property.TSERV_MAJC_DELAY.getKey(), tservMajcDelay);
      getConnector().instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), tservMajcDelay);
      tservMajcDelay = null;
    }
  }

  @Test
  public void tabletShouldSplit() throws Exception {
    Connector c = getConnector();
    String table = getUniqueNames(1)[0];
    c.tableOperations().create(table);
    c.tableOperations().setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey(), "256K");
    c.tableOperations().setProperty(table, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K");
    TestIngest.Opts opts = new TestIngest.Opts();
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    opts.rows = 100000;
    opts.setTableName(table);

    ClientConfiguration clientConfig = cluster.getClientConfig();
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      opts.updateKerberosCredentials(clientConfig);
      vopts.updateKerberosCredentials(clientConfig);
    } else {
      opts.setPrincipal(getAdminPrincipal());
      vopts.setPrincipal(getAdminPrincipal());
    }

    TestIngest.ingest(c, opts, new BatchWriterOpts());
    vopts.rows = opts.rows;
    vopts.setTableName(table);
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    while (c.tableOperations().listSplits(table).size() < 10) {
      sleepUninterruptibly(15, TimeUnit.SECONDS);
    }
    Table.ID id = Table.ID.of(c.tableOperations().tableIdMap().get(table));
    try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      KeyExtent extent = new KeyExtent(id, null, null);
      s.setRange(extent.toMetadataRange());
      MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
      int count = 0;
      int shortened = 0;
      for (Entry<Key,Value> entry : s) {
        extent = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        if (extent.getEndRow() != null && extent.getEndRow().toString().length() < 14)
          shortened++;
        count++;
      }

      assertTrue("Shortened should be greater than zero: " + shortened, shortened > 0);
      assertTrue("Count should be cgreater than 10: " + count, count > 10);
    }

    String[] args;
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      ClusterUser rootUser = getAdminUser();
      args = new String[] {"-i", cluster.getInstanceName(), "-u", rootUser.getPrincipal(), "--keytab", rootUser.getKeytab().getAbsolutePath(), "-z",
          cluster.getZooKeepers()};
    } else {
      PasswordToken token = (PasswordToken) getAdminToken();
      args = new String[] {"-i", cluster.getInstanceName(), "-u", "root", "-p", new String(token.getPassword(), UTF_8), "-z", cluster.getZooKeepers()};
    }

    assertEquals(0, getCluster().getClusterControl().exec(CheckForMetadataProblems.class, args));
  }

  @Test
  public void interleaveSplit() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    c.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
    sleepUninterruptibly(5, TimeUnit.SECONDS);
    ReadWriteIT.interleaveTest(c, tableName);
    sleepUninterruptibly(5, TimeUnit.SECONDS);
    int numSplits = c.tableOperations().listSplits(tableName).size();
    while (numSplits <= 20) {
      log.info("Waiting for splits to happen");
      Thread.sleep(2000);
      numSplits = c.tableOperations().listSplits(tableName).size();
    }
    assertTrue("Expected at least 20 splits, saw " + numSplits, numSplits > 20);
  }

  @Test
  public void deleteSplit() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    ClientConfiguration clientConfig = getCluster().getClientConfig();
    String password = null, keytab = null;
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      keytab = getAdminUser().getKeytab().getAbsolutePath();
    } else {
      password = new String(((PasswordToken) getAdminToken()).getPassword(), UTF_8);
    }
    DeleteIT.deleteTest(c, getCluster(), getAdminPrincipal(), password, tableName, keytab);
    c.tableOperations().flush(tableName, null, null, true);
    for (int i = 0; i < 5; i++) {
      sleepUninterruptibly(10, TimeUnit.SECONDS);
      if (c.tableOperations().listSplits(tableName).size() > 20)
        break;
    }
    assertTrue(c.tableOperations().listSplits(tableName).size() > 20);
  }

}

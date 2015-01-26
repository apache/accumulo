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
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.CheckForMetadataProblems;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

public class SplitIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(SplitIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "5K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "100ms");
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private String tservMaxMem, tservMajcDelay;

  @Before
  public void alterConfig() throws Exception {
    if (ClusterType.MINI == getClusterType()) {
      return;
    }

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
      getCluster().getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
      getCluster().getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);
      restarted = true;
    }

    // If we restarted the tservers, we don't need to re-wait for the majc delay
    if (!restarted) {
      long millis = AccumuloConfiguration.getTimeInMillis(tservMajcDelay);
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
      getCluster().getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
      getCluster().getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);
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
    opts.rows = 100000;
    opts.tableName = table;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.rows = opts.rows;
    vopts.tableName = table;
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    while (c.tableOperations().listSplits(table).size() < 10) {
      UtilWaitThread.sleep(15 * 1000);
    }
    String id = c.tableOperations().tableIdMap().get(table);
    Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    KeyExtent extent = new KeyExtent(new Text(id), null, null);
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
    PasswordToken token = (PasswordToken) getToken();
    assertEquals(
        0,
        getCluster().getClusterControl().exec(CheckForMetadataProblems.class,
            new String[] {"-i", cluster.getInstanceName(), "-u", "root", "-p", new String(token.getPassword(), Charsets.UTF_8), "-z", cluster.getZooKeepers()}));
  }

  @Test
  public void interleaveSplit() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    c.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
    UtilWaitThread.sleep(5 * 1000);
    ReadWriteIT.interleaveTest(c, tableName);
    UtilWaitThread.sleep(5 * 1000);
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
    PasswordToken token = (PasswordToken) getToken();
    c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    DeleteIT.deleteTest(c, getCluster(), new String(token.getPassword(), Charsets.UTF_8), tableName);
    c.tableOperations().flush(tableName, null, null, true);
    for (int i = 0; i < 5; i++) {
      UtilWaitThread.sleep(10 * 1000);
      if (c.tableOperations().listSplits(tableName).size() > 20)
        break;
    }
    assertTrue(c.tableOperations().listSplits(tableName).size() > 20);
  }

}

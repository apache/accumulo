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
package org.apache.accumulo.test;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanWithoutInitialWaitIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
  }

  private static final Logger log = LoggerFactory.getLogger(ScanWithoutInitialWaitIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 120;
  }

  @Test
  public void test() throws Exception {
    log.info("Creating table");
    String tableName = getUniqueNames(1)[0];
    Connector waitConn = getConnector();

    ClientConfiguration conf = clusterConf.getClientConf();
    conf.setProperty(ClientProperty.TSERV_SCAN_INITIAL_WAIT_ENABLED, "false");
    Credentials credentials = new Credentials(getAdminPrincipal(), getAdminToken());
    ClientContext clientContext = new ClientContext(waitConn.getInstance(), credentials, conf);
    Connector noWaitConn = new ConnectorImpl(clientContext);

    waitConn.tableOperations().create(tableName);
    log.info("Adding slow iterator");
    IteratorSetting setting = new IteratorSetting(50, SlowIterator.class);
    SlowIterator.setSleepTime(setting, 1000);
    waitConn.tableOperations().attachIterator(tableName, setting);
    log.info("Splitting the table");
    SortedSet<Text> partitionKeys = new TreeSet<>();
    partitionKeys.add(new Text("5"));
    waitConn.tableOperations().addSplits(tableName, partitionKeys);
    log.info("waiting for zookeeper propagation");
    UtilWaitThread.sleep(5 * 1000);
    log.info("Adding a few entries");
    BatchWriter bw = waitConn.createBatchWriter(tableName, null);
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation("" + i);
      m.put("", "", "");
      bw.addMutation(m);
    }
    bw.close();
    log.info("Fetching some entries: should timeout and return something");

    log.info("Testing Scanner Times");
    Scanner noWaitConnScanner = noWaitConn.createScanner(tableName, Authorizations.EMPTY);
    noWaitConnScanner.setBatchTimeout(500, TimeUnit.MILLISECONDS);
    long noWaitScanTime = testScanner(noWaitConnScanner, 1200, "No Wait Scanner");

    Scanner waitConnScanner = waitConn.createScanner(tableName, Authorizations.EMPTY);
    waitConnScanner.setBatchTimeout(500, TimeUnit.MILLISECONDS);
    long waitScanTime = testScanner(waitConnScanner, 1200, "Wait Scanner");

    log.info("Testing Batch Scanner Times");
    BatchScanner noWaitBatchScanner =
        noWaitConn.createBatchScanner(tableName, Authorizations.EMPTY, 5);
    noWaitBatchScanner.setBatchTimeout(500, TimeUnit.MILLISECONDS);
    noWaitBatchScanner.setRanges(Collections.singletonList(new Range()));
    long noWaitBatchScanTime = testScanner(noWaitBatchScanner, 1200, "No Wait Batch Scanner");

    BatchScanner waitBatchScanner = waitConn.createBatchScanner(tableName, Authorizations.EMPTY, 5);
    waitBatchScanner.setBatchTimeout(500, TimeUnit.MILLISECONDS);
    waitBatchScanner.setRanges(Collections.singletonList(new Range()));
    long waitBatchScanTime = testScanner(waitBatchScanner, 1200, "Wait Batch Scammer");

    log.info("Testing non-User scan times");
    // Set a tolerance of 5 milliseconds to evaluate scan differences.
    long scanTolerance = 5;
    Scanner waitMetaScanner = waitConn.createScanner("accumulo.metadata", Authorizations.EMPTY);
    waitMetaScanner.setBatchTimeout(500, TimeUnit.MILLISECONDS);
    long waitMetaScanTime = testScanner(waitMetaScanner, 1200, "Wait Meta Scanner");

    Scanner metaScanner = noWaitConn.createScanner("accumulo.metadata", Authorizations.EMPTY);
    metaScanner.setBatchTimeout(500, TimeUnit.MILLISECONDS);
    long metaScanTime = testScanner(metaScanner, 1200, "No Wait Meta Scanner");

    assertTrue((waitMetaScanTime - metaScanTime) >= 0);
    assertTrue((waitMetaScanTime - metaScanTime) < scanTolerance);

    assertTrue((waitScanTime - noWaitScanTime) >= 0);
    assertTrue((waitScanTime - noWaitScanTime) < scanTolerance);

    assertTrue((waitBatchScanTime - noWaitBatchScanTime) >= 0);
    assertTrue((waitBatchScanTime - noWaitBatchScanTime) < scanTolerance);
  }

  private long testScanner(ScannerBase s, long expected, String scannerName) {
    long now = System.currentTimeMillis();
    try {
      s.iterator().next();
    } finally {
      s.close();
    }
    long scanDuration = System.currentTimeMillis() - now;
    log.info("{} Scan Duration = {}", scannerName, scanDuration);
    assertTrue("Scanner taking too long to return intermediate results: " + scanDuration,
        scanDuration < expected);
    return scanDuration;
  }
}

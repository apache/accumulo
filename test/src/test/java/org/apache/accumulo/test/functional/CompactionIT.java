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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(CompactionIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> map = new HashMap<String,String>();
    map.put(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "4");
    map.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
    map.put(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), "1");
    cfg.setSiteConfig(map);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private String majcThreadMaxOpen, majcDelay, majcMaxConcurrent;

  @Before
  public void alterConfig() throws Exception {
    if (ClusterType.STANDALONE == getClusterType()) {
      InstanceOperations iops = getConnector().instanceOperations();
      Map<String,String> config = iops.getSystemConfiguration();
      majcThreadMaxOpen = config.get(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey());
      majcDelay = config.get(Property.TSERV_MAJC_DELAY.getKey());
      majcMaxConcurrent = config.get(Property.TSERV_MAJC_MAXCONCURRENT.getKey());

      iops.setProperty(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "4");
      iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), "1");
      iops.setProperty(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), "1");

      getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);
    }
  }

  @After
  public void resetConfig() throws Exception {
    // We set the values..
    if (null != majcThreadMaxOpen) {
      InstanceOperations iops = getConnector().instanceOperations();

      iops.setProperty(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), majcThreadMaxOpen);
      iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
      iops.setProperty(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), majcMaxConcurrent);

      getClusterControl().stopAllServers(ClusterServerType.TABLET_SERVER);
      getClusterControl().startAllServers(ClusterServerType.TABLET_SERVER);
    }
  }

  @Test
  public void test() throws Exception {
    final Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1.0");
    FileSystem fs = getFileSystem();
    String root = getUsableDir();
    Path testrf = new Path(root, "testrf");
    FunctionalTestUtils.createRFiles(c, fs, testrf.toString(), 500000, 59, 4);
    FunctionalTestUtils.bulkImport(c, fs, tableName, testrf.toString());
    int beforeCount = countFiles(c);

    final AtomicBoolean fail = new AtomicBoolean(false);
    for (int count = 0; count < 5; count++) {
      List<Thread> threads = new ArrayList<Thread>();
      final int span = 500000 / 59;
      for (int i = 0; i < 500000; i += 500000 / 59) {
        final int finalI = i;
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              VerifyIngest.Opts opts = new VerifyIngest.Opts();
              opts.startRow = finalI;
              opts.rows = span;
              opts.random = 56;
              opts.dataSize = 50;
              opts.cols = 1;
              opts.tableName = tableName;
              VerifyIngest.verifyIngest(c, opts, new ScannerOpts());
            } catch (Exception ex) {
              log.warn("Got exception verifying data", ex);
              fail.set(true);
            }
          }
        };
        t.start();
        threads.add(t);
      }
      for (Thread t : threads)
        t.join();
      assertFalse("Failed to successfully run all threads, Check the test output for error", fail.get());
    }

    int finalCount = countFiles(c);
    assertTrue(finalCount < beforeCount);
    try {
      getClusterControl().adminStopAll();
    } finally {
      // Make sure the internal state in the cluster is reset (e.g. processes in MAC)
      getCluster().stop();
      if (ClusterType.STANDALONE == getClusterType()) {
        // Then restart things for the next test if it's a standalone
        getCluster().start();
      }
    }
  }

  private int countFiles(Connector c) throws Exception {
    Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.fetchColumnFamily(MetadataSchema.TabletsSection.TabletColumnFamily.NAME);
    s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    return FunctionalTestUtils.count(s);
  }

}

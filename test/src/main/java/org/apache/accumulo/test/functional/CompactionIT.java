/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class CompactionIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(CompactionIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_MAJC_THREAD_MAXOPEN, "4");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1");
    cfg.setProperty(Property.TSERV_MAJC_MAXCONCURRENT, "1");
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private String majcThreadMaxOpen, majcDelay, majcMaxConcurrent;

  @Before
  public void alterConfig() throws Exception {
    if (getClusterType() == ClusterType.STANDALONE) {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        InstanceOperations iops = client.instanceOperations();
        Map<String,String> config = iops.getSystemConfiguration();
        majcThreadMaxOpen = config.get(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey());
        majcDelay = config.get(Property.TSERV_MAJC_DELAY.getKey());
        majcMaxConcurrent = config.get(Property.TSERV_MAJC_MAXCONCURRENT.getKey());

        iops.setProperty(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "4");
        iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), "1");
        iops.setProperty(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), "1");

        getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      }
    }
  }

  @After
  public void resetConfig() throws Exception {
    // We set the values..
    if (majcThreadMaxOpen != null) {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        InstanceOperations iops = client.instanceOperations();

        iops.setProperty(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), majcThreadMaxOpen);
        iops.setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
        iops.setProperty(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), majcMaxConcurrent);

        getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      }
    }
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1.0");
      FileSystem fs = getFileSystem();
      Path root =
          new Path(fs.getUri().toString() + cluster.getTemporaryPath(), getClass().getName());
      fs.deleteOnExit(root);
      Path testrf = new Path(root, "testrf");
      fs.deleteOnExit(testrf);
      FunctionalTestUtils.createRFiles(c, fs, testrf.toString(), 500000, 59, 4);

      c.tableOperations().importDirectory(testrf.toString()).to(tableName).load();
      int beforeCount = countFiles(c);

      final AtomicBoolean fail = new AtomicBoolean(false);
      final int THREADS = 5;
      for (int count = 0; count < THREADS; count++) {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        final int span = 500000 / 59;
        for (int i = 0; i < 500000; i += 500000 / 59) {
          final int finalI = i;
          Runnable r = () -> {
            try {
              VerifyParams params = new VerifyParams(getClientProps(), tableName, span);
              params.startRow = finalI;
              params.random = 56;
              params.dataSize = 50;
              params.cols = 1;
              VerifyIngest.verifyIngest(c, params);
            } catch (Exception ex) {
              log.warn("Got exception verifying data", ex);
              fail.set(true);
            }
          };
          executor.execute(r);
        }
        executor.shutdown();
        executor.awaitTermination(defaultTimeoutSeconds(), TimeUnit.SECONDS);
        assertFalse("Failed to successfully run all threads, Check the test output for error",
            fail.get());
      }

      int finalCount = countFiles(c);
      assertTrue(finalCount < beforeCount);
      try {
        getClusterControl().adminStopAll();
      } finally {
        // Make sure the internal state in the cluster is reset (e.g. processes in MAC)
        getCluster().stop();
        if (getClusterType() == ClusterType.STANDALONE) {
          // Then restart things for the next test if it's a standalone
          getCluster().start();
        }
      }
    }
  }

  private int countFiles(AccumuloClient c) throws Exception {
    try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(new Text(MetadataSchema.TabletsSection.TabletColumnFamily.NAME));
      s.fetchColumnFamily(new Text(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME));
      return Iterators.size(s.iterator());
    }
  }

}

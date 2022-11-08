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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This test verifies that when a lot of files are bulk imported into a table with one tablet and
 * then splits that not all map files go to the children tablets.
 */
public class BulkSplitOptimizationIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1s");
  }

  private String majcDelay;

  @BeforeEach
  public void alterConfig() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      majcDelay = client.instanceOperations().getSystemConfiguration()
          .get(Property.TSERV_MAJC_DELAY.getKey());
      if (!"1s".equals(majcDelay)) {
        client.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), "1s");
        getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      }
    }
  }

  @AfterEach
  public void resetConfig() throws Exception {
    if (majcDelay != null) {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        client.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), majcDelay);
        getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      }
    }
  }

  private static final int ROWS = 100000;
  private static final int SPLITS = 99;

  @Test
  public void testBulkSplitOptimization() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1000");
      c.tableOperations().setProperty(tableName, Property.TABLE_FILE_MAX.getKey(), "1000");
      c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "1G");
      FileSystem fs = cluster.getFileSystem();
      Path testDir = new Path(cluster.getTemporaryPath(), "testmf");
      fs.deleteOnExit(testDir);
      FunctionalTestUtils.createRFiles(c, fs, testDir.toString(), ROWS, SPLITS, 8);
      FileStatus[] stats = fs.listStatus(testDir);

      System.out.println("Number of generated files: " + stats.length);
      c.tableOperations().importDirectory(testDir.toString()).to(tableName).load();
      FunctionalTestUtils.checkSplits(c, tableName, 0, 0);
      FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 100, 100);

      // initiate splits
      c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "100K");

      sleepUninterruptibly(2, TimeUnit.SECONDS);

      // wait until over split threshold -- should be 78 splits
      while (c.tableOperations().listSplits(tableName).size() < 75) {
        sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      }

      FunctionalTestUtils.checkSplits(c, tableName, 50, 100);
      VerifyParams params = new VerifyParams(getClientProps(), tableName, ROWS);
      params.timestamp = 1;
      params.dataSize = 50;
      params.random = 56;
      params.startRow = 0;
      params.cols = 1;
      VerifyIngest.verifyIngest(c, params);

      // ensure each tablet does not have all map files, should be ~2.5 files per tablet
      FunctionalTestUtils.checkRFiles(c, tableName, 50, 100, 1, 4);
    }
  }
}

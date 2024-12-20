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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies that when a lot of files are bulk imported into a table with one tablet and
 * then splits that not all data files go to the children tablets.
 */
public class BulkSplitOptimizationIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(BulkSplitOptimizationIT.class);

  Path testDir;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @BeforeEach
  public void alterConfig() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final int initialTserverCount =
          client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size();
      log.info("Tserver count: {}", initialTserverCount);
      Timer timer = Timer.startNew();
      getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      Wait.waitFor(
          () -> client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).isEmpty(),
          120_000);
      log.info("Took {} ms to stop all tservers", timer.elapsed(MILLISECONDS));
      timer.restart();
      getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      Wait.waitFor(() -> client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size()
          < initialTserverCount, 120_000);
      log.info("Took {} ms to start all tservers", timer.elapsed(MILLISECONDS));

      FileSystem fs = cluster.getFileSystem();
      testDir = new Path(cluster.getTemporaryPath(), "testmf");
      fs.deleteOnExit(testDir);

      timer.restart();
      FunctionalTestUtils.createRFiles(client, fs, testDir.toString(), ROWS, SPLITS, 8);
      long elapsed = timer.elapsed(MILLISECONDS);
      FileStatus[] stats = fs.listStatus(testDir);
      log.info("Generated {} files in {} ms", stats.length, elapsed);
    }
  }

  @AfterEach
  public void resetConfig() throws Exception {
    getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
    getClusterControl().startAllServers(ServerType.TABLET_SERVER);
  }

  private static final int ROWS = 100000;
  private static final int SPLITS = 99;

  @Test
  public void testBulkSplitOptimization() throws Exception {
    log.info("Starting BulkSplitOptimizationIT test");
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      final String tableName = getUniqueNames(1)[0];
      Map<String,String> tableProps = new HashMap<>();
      tableProps.put(Property.TABLE_MAJC_RATIO.getKey(), "1000");
      tableProps.put(Property.TABLE_FILE_MAX.getKey(), "1000");
      tableProps.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "1G");

      log.info("Creating table {}", tableName);
      Timer timer = Timer.startNew();
      c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(tableProps)
          .withInitialTabletAvailability(TabletAvailability.HOSTED));
      log.info("Created table in {} ms. Starting bulk import", timer.elapsed(MILLISECONDS));

      timer.restart();
      c.tableOperations().importDirectory(testDir.toString()).to(tableName).load();
      log.info("Imported into table {} in {} ms", tableName, timer.elapsed(MILLISECONDS));

      timer.restart();
      FunctionalTestUtils.checkSplits(c, tableName, 0, 0);
      FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 100, 100);
      log.info("Checked splits and rfiles in {} ms", timer.elapsed(MILLISECONDS));

      log.info("Lowering split threshold to 100K to initiate splits");
      c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "100K");

      timer.restart();

      // wait until over split threshold -- should be 78 splits
      Wait.waitFor(() -> {
        try {
          FunctionalTestUtils.checkSplits(c, tableName, 50, 100);
        } catch (Exception e) {
          if (e.getMessage().contains("splits points out of range")) {
            return false;
          } else {
            throw e;
          }
        }
        return true;
      });

      log.info("Took {} ms for split count to reach expected range", timer.elapsed(MILLISECONDS));

      VerifyParams params = new VerifyParams(getClientProps(), tableName, ROWS);
      params.timestamp = 1;
      params.dataSize = 50;
      params.random = 56;
      params.startRow = 0;
      params.cols = 1;
      VerifyIngest.verifyIngest(c, params);

      // ensure each tablet does not have all data files, should be ~2.5 files per tablet
      FunctionalTestUtils.checkRFiles(c, tableName, 50, 100, 1, 4);
    }
  }
}

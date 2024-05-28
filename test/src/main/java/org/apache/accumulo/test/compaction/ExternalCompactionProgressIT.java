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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getRunningCompactions;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that external compactions report progress from start to finish. To prevent flaky test
 * failures, we only measure progress in quarter segments: STARTED, QUARTER, HALF, THREE_QUARTERS.
 * We can detect if the compaction finished without errors but the coordinator will never report
 * 100% progress since it will remove the ECID upon completion. The {@link SlowIterator} is used to
 * control the length of time it takes to complete the compaction.
 */
public class ExternalCompactionProgressIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(ExternalCompactionProgressIT.class);
  private static final int ROWS = 10_000;

  enum EC_PROGRESS {
    STARTED, QUARTER, HALF, THREE_QUARTERS, INVALID
  }

  Map<String,RunningCompactionInfo> runningMap = new HashMap<>();
  List<EC_PROGRESS> progressList = new ArrayList<>();

  private final AtomicBoolean compactionFinished = new AtomicBoolean(false);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
  }

  @Test
  public void testProgress() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      createTable(client, table1, "cs1");
      writeData(client, table1, ROWS);

      cluster.getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);
      cluster.getClusterControl().startCoordinator(CompactionCoordinator.class);

      Thread checkerThread = startChecker();
      checkerThread.start();

      IteratorSetting setting = new IteratorSetting(50, "Slow", SlowIterator.class);
      SlowIterator.setSleepTime(setting, 1);
      client.tableOperations().attachIterator(table1, setting,
          EnumSet.of(IteratorUtil.IteratorScope.majc));
      log.info("Compacting table");
      compact(client, table1, 2, QUEUE1, true);
      verify(client, table1, 2, ROWS);

      log.info("Done Compacting table");
      compactionFinished.set(true);
      checkerThread.join();

      verifyProgress();
    } finally {
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTION_COORDINATOR);
    }
  }

  @Test
  public void testProgressWithBulkImport() throws Exception {
    /*
     * Tests the progress of an external compaction done on a table with bulk imported files.
     * Progress should stay 0-100. There was previously a bug with the Compactor showing a >100%
     * progress for compactions with bulk import files.
     */
    String[] tableNames = getUniqueNames(2);
    String tableName1 = tableNames[0];
    String tableName2 = tableNames[1];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      log.info("Creating table " + tableName1);
      createTable(client, tableName1, "cs1");
      log.info("Creating table " + tableName2);
      createTable(client, tableName2, "cs1");
      log.info("Writing " + ROWS + " rows to table " + tableName1);
      writeData(client, tableName1, ROWS);
      log.info("Writing " + ROWS + " rows to table " + tableName2);
      writeData(client, tableName2, ROWS);
      // This is done to avoid system compactions
      client.tableOperations().setProperty(tableName1, Property.TABLE_MAJC_RATIO.getKey(), "1000");
      client.tableOperations().setProperty(tableName2, Property.TABLE_MAJC_RATIO.getKey(), "1000");

      getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
      getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);

      String dir = getDir(client, tableName1);

      log.info("Bulk importing files in dir " + dir + " to table " + tableName2);
      client.tableOperations().importDirectory(dir).to(tableName2).load();
      log.info("Finished bulk import");

      log.info("Starting a compaction progress checker thread");
      Thread checkerThread = startChecker();
      checkerThread.start();

      log.info("Attaching a slow iterator to table " + tableName2);
      IteratorSetting setting = new IteratorSetting(50, "Slow", SlowIterator.class);
      SlowIterator.setSleepTime(setting, 1);

      log.info("Compacting table " + tableName2);
      client.tableOperations().compact(tableName2,
          new CompactionConfig().setWait(true).setIterators(List.of(setting)));
      log.info("Finished compacting table " + tableName2);
      compactionFinished.set(true);

      log.info("Waiting on progress checker thread");
      checkerThread.join();

      verifyProgress();
    } finally {
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTION_COORDINATOR);
    }
  }

  /**
   * @param client an AccumuloClient
   * @param tableName the table name
   * @return the directory of the files for the table
   */
  private String getDir(AccumuloClient client, String tableName) {
    var tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

    try (var tabletsMeta = TabletsMetadata.builder(client).forTable(tableId)
        .fetch(TabletMetadata.ColumnType.FILES).build()) {
      return tabletsMeta.iterator().next().getFiles().iterator().next().getPath().getParent()
          .toUri().getRawPath();
    }
  }

  public Thread startChecker() {
    return Threads.createThread("RC checker", () -> {
      try {
        while (!compactionFinished.get()) {
          checkRunning();
          sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        }
      } catch (TException e) {
        log.warn("{}", e.getMessage(), e);
      }
    });
  }

  /**
   * Check running compaction progress.
   */
  private void checkRunning() throws TException {
    var ecList = getRunningCompactions(getCluster().getServerContext());
    var ecMap = ecList.getCompactions();
    if (ecMap != null) {
      ecMap.forEach((ecid, ec) -> {
        // returns null if it's a new mapping
        RunningCompactionInfo rci = new RunningCompactionInfo(ec);
        RunningCompactionInfo previousRci = runningMap.put(ecid, rci);
        if (previousRci == null) {
          log.debug("New ECID {} with inputFiles: {}", ecid, rci.numFiles);
        } else {
          if (rci.progress <= previousRci.progress) {
            log.warn("{} did not progress. It went from {} to {}", ecid, previousRci.progress,
                rci.progress);
          } else {
            log.debug("{} progressed from {} to {}", ecid, previousRci.progress, rci.progress);
            if (rci.progress > 0 && rci.progress <= 25) {
              progressList.add(EC_PROGRESS.STARTED);
            } else if (rci.progress > 25 && rci.progress <= 50) {
              progressList.add(EC_PROGRESS.QUARTER);
            } else if (rci.progress > 50 && rci.progress <= 75) {
              progressList.add(EC_PROGRESS.HALF);
            } else if (rci.progress > 75 && rci.progress <= 100) {
              progressList.add(EC_PROGRESS.THREE_QUARTERS);
            } else {
              progressList.add(EC_PROGRESS.INVALID);
              log.warn("An invalid progress {} has been seen. This should never occur.",
                  rci.progress);
            }
          }
          if (!rci.status.equals(TCompactionState.IN_PROGRESS.name())) {
            log.debug("Saw status other than IN_PROGRESS: {}", rci.status);
          }
        }
      });
    }
  }

  private void verifyProgress() {
    log.info("Verify Progress.");
    assertAll(
        () -> assertTrue(progressList.contains(EC_PROGRESS.STARTED), "Missing start of progress"),
        () -> assertTrue(progressList.contains(EC_PROGRESS.QUARTER), "Missing quarter progress"),
        () -> assertTrue(progressList.contains(EC_PROGRESS.HALF), "Missing half progress"),
        () -> assertTrue(progressList.contains(EC_PROGRESS.THREE_QUARTERS),
            "Missing three quarters progress"),
        () -> assertFalse(progressList.contains(EC_PROGRESS.INVALID), "Invalid progress seen"));
  }
}

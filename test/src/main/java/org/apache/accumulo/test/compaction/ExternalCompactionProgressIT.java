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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
  public static final int CHECKER_THREAD_SLEEP_MS = 1_000;

  enum EC_PROGRESS {
    STARTED, QUARTER, HALF, THREE_QUARTERS, INVALID
  }

  Map<String,RunningCompactionInfo> runningMap = new HashMap<>();
  List<EC_PROGRESS> progressList = new ArrayList<>();

  private static final AtomicBoolean stopCheckerThread = new AtomicBoolean(false);
  private static TestStatsDSink sink;

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    if (sink != null) {
      sink.close();
    }
  }

  @BeforeEach
  public void setup() {
    stopCheckerThread.set(false);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, TestStatsDRegistryFactory.class.getName());
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

  @Test
  public void testCompactionDurationContinuesAfterCoordinatorStop() throws Exception {
    String table = this.getUniqueNames(1)[0];

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      createTable(client, table, "cs1");
      writeData(client, table, ROWS);

      cluster.getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);
      cluster.getClusterControl().startCoordinator(CompactionCoordinator.class);

      IteratorSetting setting = new IteratorSetting(50, "Slow", SlowIterator.class);
      SlowIterator.setSleepTime(setting, 5);
      client.tableOperations().attachIterator(table, setting,
          EnumSet.of(IteratorUtil.IteratorScope.majc));

      log.info("Compacting table");
      compact(client, table, 2, QUEUE1, false);

      // Wait until the compaction starts
      Wait.waitFor(() -> {
        Map<String,TExternalCompaction> compactions =
            getRunningCompactions(getCluster().getServerContext()).getCompactions();
        return compactions == null || compactions.isEmpty();
      }, 30_000, 100, "Compaction did not start within the expected time");

      // start a timer after the compaction starts
      long compactionStartTime = System.nanoTime();

      // let the compaction advance a bit
      sleepUninterruptibly(6, TimeUnit.SECONDS);

      // Stop the coordinator
      log.info("Stopping the coordinator");
      cluster.getClusterControl().stopAllServers(ServerType.COMPACTION_COORDINATOR);

      sleepUninterruptibly(5, TimeUnit.SECONDS);

      log.info("Restarting the coordinator");
      cluster.getClusterControl().startCoordinator(CompactionCoordinator.class);
      long coordinatorRestartTime = System.nanoTime();

      // Wait for compactions to be present
      Map<String,TExternalCompaction> metrics = null;
      while (metrics == null) {
        try {
          metrics = getRunningCompactions(getCluster().getServerContext()).getCompactions();
        } catch (TException e) {
          UtilWaitThread.sleep(250);
        }
      }

      // let the compaction advance a bit
      sleepUninterruptibly(6, TimeUnit.SECONDS);

      TExternalCompaction updatedCompaction = getRunningCompactions(getCluster().getServerContext())
          .getCompactions().values().iterator().next();
      RunningCompactionInfo updatedCompactionInfo = new RunningCompactionInfo(updatedCompaction);

      final Duration reportedCompactionDuration = Duration.ofNanos(updatedCompactionInfo.duration);
      final Duration measuredCompactionDuration =
          Duration.ofNanos(System.nanoTime() - compactionStartTime);
      final Duration coordinatorAge = Duration.ofNanos(System.nanoTime() - coordinatorRestartTime);
      log.info(
          "Coordinator age: {}s. Measured compaction duration: {}s. Reported compaction duration: {}s",
          coordinatorAge.toSeconds(), measuredCompactionDuration.toSeconds(),
          reportedCompactionDuration.toSeconds());

      assertTrue(coordinatorAge.compareTo(reportedCompactionDuration) < 0,
          "Reported compaction age should be greater than the coordinator age");

      // Verify that the reported duration is approximately equal to the elapsed time
      Duration tolerance = Duration.ofSeconds(7);
      long reportedVsMeasuredDiff =
          Math.abs(reportedCompactionDuration.minus(measuredCompactionDuration).toNanos());
      assertTrue(reportedVsMeasuredDiff <= tolerance.toNanos(),
          String.format(
              "Reported duration (%s) and elapsed time (%s) differ by more than the tolerance (%s)",
              reportedCompactionDuration.toSeconds(), measuredCompactionDuration.toSeconds(),
              tolerance.toSeconds()));
    } finally {
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTION_COORDINATOR);
    }
  }

  @Test
  public void testProgressViaMetrics() throws Exception {
    String table = this.getUniqueNames(1)[0];

    final AtomicLong totalEntriesRead = new AtomicLong(0);
    final AtomicLong totalEntriesWritten = new AtomicLong(0);
    final long expectedEntriesRead = 9216;
    final long expectedEntriesWritten = 4096;

    Thread checkerThread = getMetricsCheckerThread(totalEntriesRead, totalEntriesWritten);

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      createTable(client, table, "cs1");
      writeData(client, table, ROWS);

      cluster.getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);
      cluster.getClusterControl().startCoordinator(CompactionCoordinator.class);

      checkerThread.start();

      IteratorSetting setting = new IteratorSetting(50, "Slow", SlowIterator.class);
      SlowIterator.setSleepTime(setting, 1);
      client.tableOperations().attachIterator(table, setting,
          EnumSet.of(IteratorUtil.IteratorScope.majc));
      log.info("Compacting table");

      compact(client, table, 2, QUEUE1, true);

      Wait.waitFor(() -> {
        if (totalEntriesRead.get() == expectedEntriesRead
            && totalEntriesWritten.get() == expectedEntriesWritten) {
          return true;
        }
        log.info(
            "Waiting for entries read to be {} (currently {}) and entries written to be {} (currently {})",
            expectedEntriesRead, totalEntriesRead.get(), expectedEntriesWritten,
            totalEntriesWritten.get());
        return false;
      }, 30_000, CHECKER_THREAD_SLEEP_MS,
          "Entries read and written metrics values did not match expected values");

      log.info("Done Compacting table");
      verify(client, table, 2, ROWS);
    } finally {
      stopCheckerThread.set(true);
      checkerThread.join();
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTION_COORDINATOR);
    }
  }

  /**
   * Pulls metrics from the configured sink and updates the provided variables.
   *
   * @param totalEntriesRead this is set to the value of the entries read metric
   * @param totalEntriesWritten this is set to the value of the entries written metric
   */
  private static Thread getMetricsCheckerThread(AtomicLong totalEntriesRead,
      AtomicLong totalEntriesWritten) {
    return Threads.createThread("metric-tailer", () -> {
      log.info("Starting metric tailer");

      sink.getLines().clear();

      out: while (!stopCheckerThread.get()) {
        List<String> statsDMetrics = sink.getLines();
        for (String s : statsDMetrics) {
          if (stopCheckerThread.get()) {
            break out;
          }
          TestStatsDSink.Metric metric = TestStatsDSink.parseStatsDMetric(s);
          if (!metric.getName().startsWith(MetricsProducer.METRICS_COMPACTOR_PREFIX)) {
            continue;
          }
          int value = Integer.parseInt(metric.getValue());
          log.debug("Found metric: {} with value: {}", metric.getName(), value);
          switch (metric.getName()) {
            case MetricsProducer.METRICS_COMPACTOR_ENTRIES_READ:
              totalEntriesRead.addAndGet(value);
              break;
            case MetricsProducer.METRICS_COMPACTOR_ENTRIES_WRITTEN:
              totalEntriesWritten.addAndGet(value);
              break;
          }
        }
        sleepUninterruptibly(CHECKER_THREAD_SLEEP_MS, TimeUnit.MILLISECONDS);
      }
      log.info("Metric tailer thread finished");
    });
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
      stopCheckerThread.set(true);
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
      stopCheckerThread.set(true);

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
        while (!stopCheckerThread.get()) {
          checkRunning();
          sleepUninterruptibly(CHECKER_THREAD_SLEEP_MS, TimeUnit.MILLISECONDS);
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
    TExternalCompactionList ecList = getRunningCompactions(getCluster().getServerContext());
    Map<String,TExternalCompaction> ecMap = ecList.getCompactions();
    if (ecMap != null) {
      ecMap.forEach((ecid, ec) -> {
        // returns null if it's a new mapping
        RunningCompactionInfo rci = new RunningCompactionInfo(ec);
        RunningCompactionInfo previousRci = runningMap.put(ecid, rci);
        log.debug("ECID {} has been running for {} seconds", ecid,
            NANOSECONDS.toSeconds(rci.duration));
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

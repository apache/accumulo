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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.harness.AccumuloClusterHarness;
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

  enum EC_PROGRESS {
    STARTED, QUARTER, HALF, THREE_QUARTERS
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
  public void testProgressViaMetrics() throws Exception {
    String table = this.getUniqueNames(1)[0];

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      createTable(client, table, "cs1");
      writeData(client, table, ROWS);

      cluster.getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);
      cluster.getClusterControl().startCoordinator(CompactionCoordinator.class);

      final AtomicLong expectedEntriesRead = new AtomicLong(9216);
      final AtomicLong expectedEntriesWritten = new AtomicLong(4096);
      final AtomicLong totalEntriesRead = new AtomicLong(0);
      final AtomicLong totalEntriesWritten = new AtomicLong(0);

      Thread checkerThread = getMetricsCheckerThread(totalEntriesRead, totalEntriesWritten);
      checkerThread.start();

      IteratorSetting setting = new IteratorSetting(50, "Slow", SlowIterator.class);
      SlowIterator.setSleepTime(setting, 1);
      client.tableOperations().attachIterator(table, setting,
          EnumSet.of(IteratorUtil.IteratorScope.majc));
      log.info("Compacting table");
      compact(client, table, 2, QUEUE1, true);
      log.info("Done Compacting table");
      verify(client, table, 2, ROWS);

      Wait.waitFor(() -> {
        if (totalEntriesRead.get() == expectedEntriesRead.get()
            && totalEntriesWritten.get() == expectedEntriesWritten.get()) {
          return true;
        }
        log.info(
            "Waiting for entries read to be {} (currently {}) and entries written to be {} (currently {})",
            expectedEntriesRead.get(), totalEntriesRead.get(), expectedEntriesWritten.get(),
            totalEntriesWritten.get());
        return false;
      }, 30000, 3000, "Entries read and written metrics values did not match expected values");

      stopCheckerThread.set(true);
      checkerThread.join();
    }
  }

  /**
   * Get a thread that checks the metrics for entries read and written.
   *
   * @param totalEntriesRead this is set to the value of the entries read metric
   * @param totalEntriesWritten this is set to the value of the entries written metric
   */
  private static Thread getMetricsCheckerThread(AtomicLong totalEntriesRead,
      AtomicLong totalEntriesWritten) {
    return Threads.createThread("metric-tailer", () -> {
      log.info("Starting metric tailer");

      while (!stopCheckerThread.get()) {
        List<String> statsDMetrics = sink.getLines();
        for (String s : statsDMetrics) {
          if (stopCheckerThread.get()) {
            break;
          }
          if (s.startsWith(MetricsProducer.METRICS_COMPACTOR_PREFIX + "entries.read")) {
            TestStatsDSink.Metric e = TestStatsDSink.parseStatsDMetric(s);
            int value = Integer.parseInt(e.getValue());
            totalEntriesRead.set(value);
            log.info("Found entries.read metric: {} with value: {}", e.getName(), value);
          } else if (s.startsWith(MetricsProducer.METRICS_COMPACTOR_PREFIX + "entries.written")) {
            TestStatsDSink.Metric e = TestStatsDSink.parseStatsDMetric(s);
            int value = Integer.parseInt(e.getValue());
            totalEntriesWritten.set(value);
            log.info("Found entries.written metric: {} with value: {}", e.getName(), value);
          }
        }
        sleepUninterruptibly(3000, TimeUnit.MILLISECONDS);
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
      compact(client, table1, 2, "DCQ1", true);
      verify(client, table1, 2, ROWS);

      log.info("Done Compacting table");
      stopCheckerThread.set(true);
      checkerThread.join();

      verifyProgress();
    }
  }

  public Thread startChecker() {
    return Threads.createThread("RC checker", () -> {
      try {
        while (!stopCheckerThread.get()) {
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
    assertTrue(progressList.contains(EC_PROGRESS.STARTED), "Missing start of progress");
    assertTrue(progressList.contains(EC_PROGRESS.QUARTER), "Missing quarter progress");
    assertTrue(progressList.contains(EC_PROGRESS.HALF), "Missing half progress");
    assertTrue(progressList.contains(EC_PROGRESS.THREE_QUARTERS),
        "Missing three quarters progress");
  }
}

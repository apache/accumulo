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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that external compactions wait when there is no ompaction work and can be woken by the
 * coordinator when there is work.
 */
public class ExternalCompactionWaitIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(ExternalCompactionWaitIT.class);
  private static final int ROWS = 10_000;
  public static final int CHECKER_THREAD_SLEEP_MS = 1_000;

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
    cfg.setProperty(Property.COMPACTOR_MIN_JOB_WAIT_TIME, "3m");
    cfg.setProperty(Property.COMPACTION_COORDINATOR_COMPACTOR_WAKEUP_THREADS, "1");
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, TestStatsDRegistryFactory.class.getName());
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

  @Test
  public void testWaitWakeViaMetrics() throws Exception {
    String table = this.getUniqueNames(1)[0];

    final AtomicBoolean compactorIdle = new AtomicBoolean(false);

    Thread checkerThread = getMetricsCheckerThread(compactorIdle);

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      createTable(client, table, "cs1");
      writeData(client, table, ROWS);

      cluster.getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);
      cluster.getClusterControl().startCoordinator(CompactionCoordinator.class);

      checkerThread.start();

      Wait.waitFor(() -> compactorIdle.get());

      IteratorSetting setting = new IteratorSetting(50, "Slow", SlowIterator.class);
      SlowIterator.setSleepTime(setting, 1);
      client.tableOperations().attachIterator(table, setting,
          EnumSet.of(IteratorUtil.IteratorScope.majc));
      log.info("Compacting table");
      compact(client, table, 2, QUEUE1, true);

      Wait.waitFor(() -> !compactorIdle.get(), SECONDS.toMillis(60));

      log.info("Done Compacting table");
      verify(client, table, 2, ROWS);
    } finally {
      stopCheckerThread.set(true);
      checkerThread.join();
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTION_COORDINATOR);
    }
  }

  /*
   * Pulls metrics from the configured sink and updates the provided variables.
   */
  private static Thread getMetricsCheckerThread(AtomicBoolean compactorIdle) {
    return Threads.createNonCriticalThread("metric-tailer", () -> {
      log.info("Starting metric tailer");

      sink.getLines().clear();

      out: while (!stopCheckerThread.get()) {
        List<String> statsDMetrics = sink.getLines();
        for (String s : statsDMetrics) {
          if (stopCheckerThread.get()) {
            break out;
          }
          TestStatsDSink.Metric metric = TestStatsDSink.parseStatsDMetric(s);
          // When the tablet server flushes memory to disk that can cause metrics that may throw the
          // test off, so only look for metrics from the compactor.
          String process = metric.getTags().getOrDefault("process.name", "none");
          if (process.equals("compactor")
              && metric.getName().equals(MetricsProducer.METRICS_SERVER_IDLE)) {
            int value = Integer.parseInt(metric.getValue());
            log.debug("Found metric: {} {} with value: {}", metric.getName(), metric.getTags(),
                value);
            switch (metric.getName()) {
              case MetricsProducer.METRICS_SERVER_IDLE:
                compactorIdle.set(value == 0 ? false : true);
                break;
            }
          }
        }
        sleepUninterruptibly(CHECKER_THREAD_SLEEP_MS, TimeUnit.MILLISECONDS);
      }
      log.info("Metric tailer thread finished");
    });
  }

}

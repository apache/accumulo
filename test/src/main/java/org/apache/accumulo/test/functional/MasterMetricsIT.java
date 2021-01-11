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
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.metrics.MetricsFileTailer;
import org.apache.accumulo.test.util.SlowOps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test that uses a hadoop metrics 2 file sink to read published metrics for
 * verification.
 */
public class MasterMetricsIT extends AccumuloClusterHarness {

  @Override
  public boolean canRunTest(ClusterType type) {
    return type == ClusterType.MINI;
  }

  private static final Logger log = LoggerFactory.getLogger(MasterMetricsIT.class);

  private AccumuloClient accumuloClient;

  private static final int NUM_TAIL_ATTEMPTS = 20;
  private static final long TAIL_DELAY = 5_000;

  // number of tables / concurrent compactions used during testing.
  private final int tableCount = 1;

  private long maxWait;

  private static final Set<String> REQUIRED_METRIC_KEYS =
      new HashSet<>(Arrays.asList("currentFateOps", "totalFateOps", "totalZkConnErrors",
          "FateTxState_NEW", "FateTxState_IN_PROGRESS", "FateTxState_FAILED_IN_PROGRESS",
          "FateTxState_FAILED", "FateTxState_SUCCESSFUL", "FateTxState_UNKNOWN"));

  private static final Set<String> OPTIONAL_METRIC_KEYS =
      new HashSet<>(Collections.singletonList("FateTxOpType_CompactRange"));

  private final MetricsFileTailer metricsTail = new MetricsFileTailer("accumulo.sink.file-master");

  @Before
  public void setup() {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
    maxWait = defaultTimeoutSeconds() <= 0 ? 60_000 : ((defaultTimeoutSeconds() * 1000) / 2);
    metricsTail.startDaemonThread();
  }

  @After
  public void cleanup() {
    metricsTail.close();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  /**
   * Validates that the expected metrics are published - this excludes the dynamic metrics derived
   * from operation types.
   */
  @Test
  public void metricsPublished() throws AccumuloException, AccumuloSecurityException {

    assumeTrue(accumuloClient.instanceOperations().getSystemConfiguration()
        .get(Property.MASTER_FATE_METRICS_ENABLED.getKey()).compareTo("true") == 0);

    log.trace("Client started, properties:{}", accumuloClient.properties());

    MetricsFileTailer.LineUpdate firstUpdate =
        metricsTail.waitForUpdate(-1, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    Map<String,Long> firstSeenMap = parseLine(firstUpdate.getLine());

    log.info("L:{}", firstUpdate.getLine());
    log.info("Expected: ({})", REQUIRED_METRIC_KEYS.size());
    log.info("M({}):{}", firstSeenMap.size(), firstSeenMap);

    assertTrue(lookForExpectedKeys(firstSeenMap));
    sanity(firstSeenMap);

    MetricsFileTailer.LineUpdate nextUpdate =
        metricsTail.waitForUpdate(firstUpdate.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    Map<String,Long> updateSeenMap = parseLine(nextUpdate.getLine());

    log.debug("Line received:{}", nextUpdate.getLine());
    log.trace("Mapped values:{}", updateSeenMap);

    assertTrue(lookForExpectedKeys(updateSeenMap));
    sanity(updateSeenMap);

    validate(firstSeenMap, updateSeenMap);
  }

  /**
   * Run a few compactions - this should trigger the a dynamic op type to be included in the
   * metrics.
   */
  @Test
  public void compactionMetrics() throws AccumuloSecurityException, AccumuloException {

    assumeTrue(accumuloClient.instanceOperations().getSystemConfiguration()
        .get(Property.MASTER_FATE_METRICS_ENABLED.getKey()).compareTo("true") == 0);

    MetricsFileTailer.LineUpdate firstUpdate =
        metricsTail.waitForUpdate(-1, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    List<SlowOps> tables = new ArrayList<>();

    for (int i = 0; i < tableCount; i++) {
      String uniqueName = getUniqueNames(1)[0] + "_" + i;
      SlowOps.setExpectedCompactions(accumuloClient, tableCount);
      SlowOps gen = new SlowOps(accumuloClient, uniqueName, maxWait);
      tables.add(gen);
      gen.startCompactTask();
    }

    // check file tailer here....
    MetricsFileTailer.LineUpdate nextUpdate =
        metricsTail.waitForUpdate(firstUpdate.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    log.info("Received metrics {}", nextUpdate);

    Map<String,String> results = blockForRequiredTables();

    assertFalse(results.isEmpty());
    log.error("IN_PROGRESS: {}", results.get("FateTxState_IN_PROGRESS"));

    assertTrue(Long.parseLong(results.get("FateTxState_IN_PROGRESS")) >= tableCount);
    assertTrue(Long.parseLong(results.get("FateTxOpType_CompactRange")) >= tableCount);

    for (String k : OPTIONAL_METRIC_KEYS) {
      assertTrue(results.containsKey(k));
      assertTrue(Long.parseLong(results.get(k)) >= tableCount);
    }

    for (SlowOps t : tables) {
      try {
        accumuloClient.tableOperations().cancelCompaction(t.getTableName());
        // block if compaction still running
        boolean cancelled = t.blockWhileCompactionRunning();
        if (!cancelled) {
          log.info("Failed to cancel compaction during multiple compaction test clean-up for {}",
              t.getTableName());
        }
      } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException ex) {
        log.debug("Exception thrown during multiple table test clean-up", ex);
      }
    }

    // wait for one more metrics update after compactions cancelled.
    MetricsFileTailer.LineUpdate update =
        metricsTail.waitForUpdate(0L, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    metricsTail.waitForUpdate(update.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    results = metricsTail.parseLine("");

    log.info("Received metrics {}", results);

  }

  private Map<String,String> blockForRequiredTables() {

    MetricsFileTailer.LineUpdate update =
        metricsTail.waitForUpdate(0L, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    for (int i = 0; i < 20; i++) {

      update = metricsTail.waitForUpdate(update.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

      log.info("Received metrics update {}", update);

      Map<String,String> results = metricsTail.parseLine("");

      if (results != null && !results.isEmpty()
          && Long.parseLong(results.get("currentFateOps")) >= tableCount) {
        log.info("Found required number of fate operations");
        return results;
      }
      try {
        Thread.sleep(10_000);
      } catch (InterruptedException iex) {
        Thread.currentThread().interrupt();
        return Collections.emptyMap();
      }

    }
    return Collections.emptyMap();
  }

  /**
   * Validate metrics for consistency withing a run cycle.
   *
   * @param values
   *          map of values from one run cycle.
   */
  private void sanity(final Map<String,Long> values) {

    assertTrue(values.get("currentFateOps") <= values.get("totalFateOps"));

    long total = values.entrySet().stream().filter(x -> x.getKey().startsWith("FateTxState_"))
        .mapToLong(Map.Entry::getValue).sum();

    assertTrue(total >= values.get("currentFateOps"));
  }

  /**
   * A series of sanity checks for the metrics between different update cycles, some values should
   * be at least different, and some of the checks can include ordering.
   *
   * @param firstSeen
   *          map of first metric update
   * @param nextSeen
   *          map of a later metric update.
   */
  private void validate(Map<String,Long> firstSeen, Map<String,Long> nextSeen) {
    // total fate ops should not decrease.
    assertTrue(firstSeen.get("totalFateOps") <= nextSeen.get("totalFateOps"));
  }

  /**
   * The hadoop metrics file sink published records as a line with comma separated key=value pairs.
   * This method parses the line and extracts the key, value pair from metrics that start with AccGc
   * and returns them in a sort map.
   *
   * @param line
   *          a line from the metrics system file sink.
   * @return a map of the metrics that start with AccGc
   */
  private Map<String,Long> parseLine(final String line) {

    if (line == null) {
      return Collections.emptyMap();
    }

    Map<String,Long> m = new TreeMap<>();

    String[] csvTokens = line.split(",");

    for (String token : csvTokens) {
      token = token.trim();
      String[] parts = token.split("=");
      if (REQUIRED_METRIC_KEYS.contains(parts[0])) {
        m.put(parts[0], Long.parseLong(parts[1]));
      }
    }
    return m;
  }

  private boolean lookForExpectedKeys(final Map<String,Long> received) {

    for (String e : REQUIRED_METRIC_KEYS) {
      if (!received.containsKey(e)) {
        log.info("Couldn't find {}", e);
        return false;
      }
    }

    return true;
  }
}

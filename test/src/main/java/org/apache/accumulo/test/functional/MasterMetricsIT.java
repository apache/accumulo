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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.MetricsFileTailer;
import org.apache.accumulo.test.util.SlowOps;
import org.apache.hadoop.conf.Configuration;
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

  private static final Logger log = LoggerFactory.getLogger(MasterMetricsIT.class);

  private static final int NUM_TAIL_ATTEMPTS = 20;
  private static final long TAIL_DELAY = 5_000;

  // number of tables / concurrent compactions used during testing.
  private final int tableCount = 4;

  private long maxWait;

  private static final Set<String> REQUIRED_METRIC_KEYS =
      new HashSet<>(Arrays.asList("currentFateOps", "totalFateOps", "totalZkConnErrors",
          "FateTxState_NEW", "FateTxState_IN_PROGRESS", "FateTxState_FAILED_IN_PROGRESS",
          "FateTxState_FAILED", "FateTxState_SUCCESSFUL", "FateTxState_UNKNOWN"));

  private static final Set<String> OPTIONAL_METRIC_KEYS =
      new HashSet<>(Collections.singletonList("FateTxOpType_CompactRange"));

  private MetricsFileTailer metricsTail = null;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GENERAL_LEGACY_METRICS, "false");
    cfg.setProperty(Property.MASTER_FATE_METRICS_ENABLED, "true");
    cfg.setProperty(Property.MASTER_FATE_METRICS_MIN_UPDATE_INTERVAL, "5s");
  }

  @Before
  public void setup() {

    if (testDisabled()) {
      return;
    }

    maxWait = defaultTimeoutSeconds() <= 0 ? 60_000 : ((defaultTimeoutSeconds() * 1000) / 2);

    metricsTail = new MetricsFileTailer("accumulo.sink.file-master");
    Thread t1 = new Thread(metricsTail);
    t1.start();

  }

  @After
  public void cleanup() {
    if (metricsTail != null) {
      metricsTail.close();
    }
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
  public void metricsPublished() {

    if (testDisabled()) {
      log.info("Skipping test - master metrics not enabled.");
      return;
    }

    // throw away first update - could be from previous test (possible with cluster
    // restarts in each test)
    MetricsFileTailer.LineUpdate firstUpdate =
        metricsTail.waitForUpdate(-1, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    firstUpdate =
        metricsTail.waitForUpdate(firstUpdate.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    Map<String,Long> firstSeenMap = parseLine(firstUpdate.getLine());

    log.debug("Line received: {}", firstUpdate.getLine());
    log.info("Expected metrics count: {}", REQUIRED_METRIC_KEYS.size());
    log.info("Received metrics count: {},  values:{}", firstSeenMap.size(), firstSeenMap);

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
  public void compactionMetrics() {

    if (testDisabled()) {
      log.info("Skipping test - MASTER_FATE_METRICS_ENABLED is not enabled");
      return;
    }

    MetricsFileTailer.LineUpdate firstUpdate =
        metricsTail.waitForUpdate(-1, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    List<SlowOps> tables = new ArrayList<>();

    for (int i = 0; i < tableCount; i++) {
      String uniqueName = getUniqueNames(1)[0] + "_" + i;
      SlowOps gen = new SlowOps(getConnector(), uniqueName, maxWait, tableCount);
      tables.add(gen);
      gen.startCompactTask();
    }

    // check file tailer here....
    MetricsFileTailer.LineUpdate nextUpdate =
        metricsTail.waitForUpdate(firstUpdate.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    log.info("Received metrics {}", nextUpdate);

    Map<String,String> results = blockForRequiredTables();

    assertFalse(results.isEmpty());
    log.info("IN_PROGRESS: {}", results.get("FateTxState_IN_PROGRESS"));

    assertTrue(Long.parseLong(results.get("FateTxState_IN_PROGRESS")) >= tableCount);
    assertTrue(Long.parseLong(results.get("FateTxOpType_CompactRange")) >= tableCount);

    for (String k : OPTIONAL_METRIC_KEYS) {
      assertTrue(results.containsKey(k));
      assertTrue(Long.parseLong(results.get(k)) >= tableCount);
    }

    // clean-up cancel running compactions
    for (SlowOps t : tables) {
      try {
        getConnector().tableOperations().cancelCompaction(t.getTableName());
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

    for (SlowOps t : tables) {
      try {
        log.debug("delete table {}", t.getTableName());
        getConnector().tableOperations().delete(t.getTableName());
      } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
        // empty
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

      if (results != null && results.size() > 0
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
   * Validate metrics for consistency with in a run cycle.
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
    log.debug("Total fate ops.  Before:{}, Update:{}", firstSeen.get("totalFateOps"),
        nextSeen.get("totalFateOps"));
    assertTrue(firstSeen.get("totalFateOps") <= nextSeen.get("totalFateOps"));
  }

  /**
   * The hadoop metrics file sink published records as a line with comma separated key=value pairs.
   * This method parses the line and extracts the key, value pair from metrics that start with AccGc
   * and returns them in a sort map.
   *
   * @param line
   *          a line from the metrics system file sink.
   * @return a map of the metrics that match REQUIRED_METRICS_KEYS
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

  /**
   * FATE metrics only valid when MASTER_FATE_METRICS_ENABLED=true and GENERAL_LEGACY_METRICS=flase
   *
   * @return true if test should run
   */
  private boolean testDisabled() {

    boolean fateMetricsEnabled =
        cluster.getSiteConfiguration().getBoolean(Property.MASTER_FATE_METRICS_ENABLED);

    boolean useLegacyMetrics =
        cluster.getSiteConfiguration().getBoolean(Property.GENERAL_LEGACY_METRICS);

    if (!fateMetricsEnabled || useLegacyMetrics) {

      log.info("master fate metrics are disabled - MASTER_FATE_METRICS_ENABLED={}, "
          + "GENERAL_LEGACY_METRICS={}", fateMetricsEnabled, useLegacyMetrics);

      return true;
    }

    return false;
  }

}

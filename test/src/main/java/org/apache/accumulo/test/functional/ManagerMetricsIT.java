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

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

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

import com.google.common.collect.Sets;

/**
 * Functional test that uses a hadoop metrics 2 file sink to read published metrics for
 * verification.
 */
public class ManagerMetricsIT extends AccumuloClusterHarness {

  @Override
  public boolean canRunTest(ClusterType type) {
    return type == ClusterType.MINI;
  }

  private static final Logger log = LoggerFactory.getLogger(ManagerMetricsIT.class);

  private AccumuloClient accumuloClient;

  private static final int NUM_TAIL_ATTEMPTS = 20;
  private static final long TAIL_DELAY = 5_000L;

  // number of tables / concurrent compactions used during testing.
  private final int tableCount = 1;

  private long maxWait;

  private static final Set<String> EXPECTED_METRIC_KEYS_HADOOP =
      Set.of("currentFateOps", "totalFateOps", "totalZkConnErrors", "FateTxState_NEW",
          "FateTxState_IN_PROGRESS", "FateTxState_FAILED_IN_PROGRESS", "FateTxState_FAILED",
          "FateTxState_SUCCESSFUL", "FateTxState_UNKNOWN");

  private static final Set<String> EXPECTED_METRIC_KEYS_MICROMETER =
      Set.of("currentFateOps", "zkChildFateOpsTotal", "zkConnectionErrorsTotal", "FateTxState_NEW",
          "FateTxState_IN_PROGRESS", "FateTxState_FAILED_IN_PROGRESS", "FateTxState_FAILED",
          "FateTxState_SUCCESSFUL", "FateTxState_UNKNOWN");

  private static final Set<String> OPTIONAL_METRIC_KEYS = Set.of("FateTxOpType_CompactRange");

  private final MetricsFileTailer metricsTail = new MetricsFileTailer("accumulo.sink.file-manager");

  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final HttpRequest REQUEST =
      HttpRequest.newBuilder().uri(java.net.URI.create("http://localhost:9234/metrics")).build();

  @Before
  public void setup() {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
    maxWait = defaultTimeoutSeconds() <= 0 ? 60_000 : ((defaultTimeoutSeconds() * 1000L) / 2);
    metricsTail.startDaemonThread();
  }

  @After
  public void cleanup() {
    metricsTail.close();
    accumuloClient.close();
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
  public void managerMetricsPublishedHadoop() throws AccumuloException, AccumuloSecurityException {
    // assume metrics are enabled via properties
    @SuppressWarnings("deprecation")
    String p = Property.MANAGER_FATE_METRICS_ENABLED.getKey();
    assumeTrue(
        accumuloClient.instanceOperations().getSystemConfiguration().get(p).compareTo("true") == 0);

    log.trace("Client started, properties:{}", accumuloClient.properties());

    MetricsFileTailer.LineUpdate firstUpdate =
        metricsTail.waitForUpdate(-1, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    Map<String,Long> firstSeenMap = parseLine(firstUpdate.getLine());

    log.info("L:{}", firstUpdate.getLine());
    log.info("Expected: ({})", EXPECTED_METRIC_KEYS_HADOOP.size());
    log.info("M({}):{}", firstSeenMap.size(), firstSeenMap);

    // ensure all metrics arrived as expected
    assertTrue(lookForExpectedKeys(firstSeenMap, EXPECTED_METRIC_KEYS_HADOOP));
    sanityCheckHadoop(firstSeenMap);

    MetricsFileTailer.LineUpdate nextUpdate =
        metricsTail.waitForUpdate(firstUpdate.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    Map<String,Long> updateSeenMap = parseLine(nextUpdate.getLine());

    log.debug("Line received:{}", nextUpdate.getLine());
    log.trace("Mapped values:{}", updateSeenMap);

    assertTrue(lookForExpectedKeys(updateSeenMap, EXPECTED_METRIC_KEYS_HADOOP));
    sanityCheckHadoop(updateSeenMap);

    validateHadoop(firstSeenMap, updateSeenMap);
  }

  /**
   * Validates that the expected metrics are published - this excludes the dynamic metrics derived
   * from operation types.
   */
  @Test
  public void managerMetricsPublishedMicrometer()
      throws AccumuloException, AccumuloSecurityException {
    @SuppressWarnings("deprecation")
    String p = Property.MANAGER_FATE_METRICS_ENABLED.getKey();
    // assume metrics are enabled via properties
    assumeTrue(
        accumuloClient.instanceOperations().getSystemConfiguration().get(p).compareTo("true") == 0);

    log.trace("Client started, properties:{}", accumuloClient.properties());

    ScrapeUpdate firstScrape = captureMetricsAfterTimestampPrometheus(-1L);

    log.info("S:{}", firstScrape);
    log.info("Expected: ({})", EXPECTED_METRIC_KEYS_MICROMETER.size());
    log.info("M({})", firstScrape.values.size());

    // ensure all metrics arrived as expected
    assertTrue(lookForExpectedKeys(firstScrape.values, EXPECTED_METRIC_KEYS_MICROMETER));
    sanityCheckMicrometer(firstScrape.values);

    ScrapeUpdate nextScrape = captureMetricsAfterTimestampPrometheus(firstScrape.timestamp);

    log.debug("Scrape received:{}", nextScrape);

    assertTrue(lookForExpectedKeys(nextScrape.values, EXPECTED_METRIC_KEYS_MICROMETER));
    sanityCheckMicrometer(nextScrape.values);

    validateMicrometer(firstScrape.values, nextScrape.values);
  }

  /**
   * Run a few compactions - this should trigger a dynamic op type to be included in the metrics.
   */
  @Test
  public void compactionMetricsHadoop() throws AccumuloSecurityException, AccumuloException {
    @SuppressWarnings("deprecation")
    String p = Property.MANAGER_FATE_METRICS_ENABLED.getKey();
    // assume metrics are enabled via properties
    assumeTrue(
        accumuloClient.instanceOperations().getSystemConfiguration().get(p).compareTo("true") == 0);

    MetricsFileTailer.LineUpdate firstUpdate =
        metricsTail.waitForUpdate(-1, NUM_TAIL_ATTEMPTS, TAIL_DELAY);
    log.info("Received first metrics update {}", firstUpdate);

    // make sure all expected metrics are present
    assertTrue(lookForExpectedKeys(parseLine(firstUpdate.getLine()), EXPECTED_METRIC_KEYS_HADOOP));

    // start compactions
    List<SlowOps> tables = new ArrayList<>();
    String[] uniqueNames = getUniqueNames(tableCount);
    for (int i = 0; i < tableCount; i++) {
      String uniqueName = uniqueNames[i] + "_" + i;
      SlowOps.setExpectedCompactions(accumuloClient, tableCount);
      SlowOps gen = new SlowOps(accumuloClient, uniqueName, maxWait);
      tables.add(gen);
      gen.startCompactTask();
    }

    // get next batch of metrics
    MetricsFileTailer.LineUpdate nextUpdate =
        metricsTail.waitForUpdate(firstUpdate.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    log.info("Received next metrics update {}", nextUpdate);

    // wait until there are in-progress operations present in the metrics
    Map<String,String> results = blockForRequiredTablesHadoop();

    assertFalse(results.isEmpty());
    log.info("IN_PROGRESS: {}", results.get("FateTxState_IN_PROGRESS"));

    // assert compactions are evident in metrics
    assertTrue(Long.parseLong(results.get("FateTxState_IN_PROGRESS")) >= tableCount);
    assertTrue(Long.parseLong(results.get("FateTxOpType_CompactRange")) >= tableCount);

    for (String k : OPTIONAL_METRIC_KEYS) {
      assertTrue(results.containsKey(k));
      assertTrue(Long.parseLong(results.get(k)) >= tableCount);
    }

    // stop compactions
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
        metricsTail.waitForUpdate(-1L, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    metricsTail.waitForUpdate(update.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

    results = metricsTail.parseLine("");

    log.info("Received metrics update {}", results);

  }

  /**
   * Run a few compactions - this should trigger a dynamic op type to be included in the metrics.
   */
  @Test
  public void compactionMetricsMicrometer() throws AccumuloException, AccumuloSecurityException {
    @SuppressWarnings("deprecation")
    String p = Property.MANAGER_FATE_METRICS_ENABLED.getKey();
    // assume metrics are enabled via properties
    assumeTrue(
        accumuloClient.instanceOperations().getSystemConfiguration().get(p).compareTo("true") == 0);

    ScrapeUpdate firstScrape = captureMetricsAfterTimestampPrometheus(-1L);
    log.info("Received first metrics update {}", firstScrape);

    // start compactions
    List<SlowOps> tables = new ArrayList<>();
    String[] uniqueNames = getUniqueNames(tableCount);
    for (int i = 0; i < tableCount; i++) {
      String uniqueName = uniqueNames[i] + "_" + i;
      SlowOps.setExpectedCompactions(accumuloClient, tableCount);
      SlowOps gen = new SlowOps(accumuloClient, uniqueName, maxWait);
      tables.add(gen);
      gen.startCompactTask();
    }

    // wait until there are in-progress operations present in the metrics
    ScrapeUpdate nextScrape = blockForRequiredTablesMicrometer();
    assertNotNull(nextScrape);
    log.info("Received next metrics update {}", nextScrape);

    log.info("IN_PROGRESS: {}", nextScrape.get("FateTxState_IN_PROGRESS"));

    assertTrue(nextScrape.get("FateTxState_IN_PROGRESS") >= tableCount);

    // assert compactions are evident in metrics
    assertTrue(nextScrape.get("FateTxOpType_CompactRange") >= tableCount);
    for (String k : OPTIONAL_METRIC_KEYS) {
      assertTrue(nextScrape.values.containsKey(k));
      assertTrue(nextScrape.get(k) >= tableCount);
    }

    log.debug("Cancelling compactions");
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
    ScrapeUpdate lastUpdate = captureMetricsAfterTimestampPrometheus(nextScrape.timestamp);

    captureMetricsAfterTimestampPrometheus(lastUpdate.timestamp);

    log.info("Received metrics update {}", lastUpdate);

  }

  private Map<String,String> blockForRequiredTablesHadoop() {

    MetricsFileTailer.LineUpdate update =
        metricsTail.waitForUpdate(-1L, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

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

  private ScrapeUpdate blockForRequiredTablesMicrometer() {

    ScrapeUpdate update = captureMetricsAfterTimestampPrometheus(-1L);

    for (int i = 0; i < 20; i++) {

      update = captureMetricsAfterTimestampPrometheus(update.timestamp);

      log.info("Received metrics update {}", update);

      if (update.get("currentFateOps") >= tableCount) {
        log.info("Found required number of fate operations");
        return update;
      }
      try {
        Thread.sleep(10_000);
      } catch (InterruptedException iex) {
        Thread.currentThread().interrupt();
        return null;
      }

    }
    return null;
  }

  /**
   * Validate metrics for consistency within a run cycle.
   *
   * @param values
   *          map of values from one run cycle.
   */
  private void sanityCheckHadoop(final Map<String,Long> values) {

    assertTrue(values.get("currentFateOps") <= values.get("totalFateOps"));

    long total = values.entrySet().stream().filter(x -> x.getKey().startsWith("FateTxState_"))
        .mapToLong(Map.Entry::getValue).sum();

    assertTrue(total >= values.get("currentFateOps"));
  }

  /**
   * Validate metrics for consistency within a run cycle.
   *
   * @param values
   *          map of values from one run cycle.
   */
  private void sanityCheckMicrometer(final Map<String,Long> values) {

    assertTrue(values.get("currentFateOps") <= values.get("zkChildFateOpsTotal"));

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
  private void validateHadoop(Map<String,Long> firstSeen, Map<String,Long> nextSeen) {
    // total fate ops should not decrease.
    assertTrue(firstSeen.get("totalFateOps") <= nextSeen.get("totalFateOps"));
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
  private void validateMicrometer(Map<String,Long> firstSeen, Map<String,Long> nextSeen) {
    // total fate ops should not decrease.
    assertTrue(firstSeen.get("zkChildFateOpsTotal") <= nextSeen.get("zkChildFateOpsTotal"));
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
    log.debug("Line received:{}", line);
    Map<String,
        Long> values = Stream.of(line.split(",")).map(s -> s.trim().split("="))
            .filter(
                s -> Sets.union(EXPECTED_METRIC_KEYS_HADOOP, OPTIONAL_METRIC_KEYS).contains(s[0]))
            .collect(toMap(a -> a[0], a -> Long.parseLong(a[1]), (a, b) -> {
              throw new IllegalArgumentException("Metric field found with two values '" + a
                  + "' and '" + b + "' in GC Metrics line: " + line);
            }, TreeMap::new));
    log.debug("Mapped values:{}", values);
    return values;
  }

  private static class ScrapeUpdate {
    private final Long timestamp = System.currentTimeMillis();
    public final Map<String,Long> values;

    ScrapeUpdate(String scrape) {
      values = parseScrape(scrape);
    }

    public Long get(String key) {
      return values.get(key);
    }

    /**
     * Prometheus displays metrics as a space separated key value pair. This method parses the
     * Prometheus output into a map. Ex. line from metrics scrape:
     * fate_status_Counts{status="FateTxState_UNKNOWN",} 0.0
     *
     * @param scrape
     *          a scrape of the published metrics from micrometer
     * @return a map of the metrics that start with AccGc
     */
    private Map<String,Long> parseScrape(String scrape) {
      Map<String,Long> values = Stream.of(scrape.split("\\r?\\n")).map(s -> s.split("\""))
          .filter(s -> (s.length == 3
              && Sets.union(EXPECTED_METRIC_KEYS_MICROMETER, OPTIONAL_METRIC_KEYS).contains(s[1])))
          .collect(toMap(a -> a[1],
              a -> (new BigDecimal(a[2].replace(",} ", "")).toBigInteger()).longValue(),
              (a, b) -> a, TreeMap::new));
      log.debug("Mapped values:{}", values);
      return values;
    }

    @Override
    public String toString() {
      return values.toString();
    }
  }

  private ScrapeUpdate captureMetricsAfterTimestampPrometheus(final long timestamp) {
    for (int count = 0; count < NUM_TAIL_ATTEMPTS; count++) {
      String scrape = getPrometheusScrape();
      // check for valid metrics scrape update since the previous update
      ScrapeUpdate scrapeUpdate = new ScrapeUpdate(scrape);
      if (scrapeUpdate.timestamp > timestamp
          && lookForExpectedKeys(scrapeUpdate.values, EXPECTED_METRIC_KEYS_MICROMETER)) {
        return scrapeUpdate;
      }
      try {
        Thread.sleep(TAIL_DELAY);
      } catch (InterruptedException e) {
        log.error("Encountered InterruptedException while attempting to get metrics update.", e);
        throw new RuntimeException(e);
      }
    }
    throw new IllegalStateException(
        String.format("Metrics update not received after %d tries in %d sec", NUM_TAIL_ATTEMPTS,
            TimeUnit.MILLISECONDS.toSeconds(TAIL_DELAY * NUM_TAIL_ATTEMPTS)));
  }

  private String getPrometheusScrape() {
    AtomicReference<String> scrape = new AtomicReference<>();
    int count = 0;
    while (true) {
      try {
        HTTP_CLIENT.sendAsync(REQUEST, HttpResponse.BodyHandlers.ofString())
            .thenApply(HttpResponse::body).thenAccept(scrape::set).join();
        break;
      } catch (Exception e) {
        if (++count == NUM_TAIL_ATTEMPTS)
          throw e;
      }
      try {
        log.debug("Couldn't get micrometer metrics. Retrying");
        Thread.sleep(TAIL_DELAY);
      } catch (InterruptedException e) {
        log.error("Encountered InterruptedException while attempting to get metrics update.", e);
        throw new RuntimeException(e);
      }
    }
    return scrape.get();
  }

  private boolean lookForExpectedKeys(final Map<String,Long> received, Set<String> keys) {
    Set<String> diffSet = Sets.difference(keys, received.keySet());
    if (!diffSet.isEmpty()) {
      log.debug("There were expected keys that were not found: {}", diffSet);
      return false;
    }
    return true;
  }
}

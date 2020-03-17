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

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.gc.metrics.GcMetrics;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.MetricsFileTailer;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test that uses a hadoop metrics 2 file sink to read published metrics for
 * verification.
 */
public class GcMetricsIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(GcMetricsIT.class);

  private AccumuloClient accumuloClient;

  private static final int NUM_TAIL_ATTEMPTS = 20;
  private static final long TAIL_DELAY = 5_000;

  private static final String[] EXPECTED_METRIC_KEYS = new String[] {"AccGcCandidates",
      "AccGcDeleted", "AccGcErrors", "AccGcFinished", "AccGcInUse", "AccGcPostOpDuration",
      "AccGcRunCycleCount", "AccGcStarted", "AccGcWalCandidates", "AccGcWalDeleted",
      "AccGcWalErrors", "AccGcWalFinished", "AccGcWalInUse", "AccGcWalStarted"};

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GC_METRICS_ENABLED, "true");
  }

  @Before
  public void init() {
    accumuloClient = Accumulo.newClient().from(getClientProperties()).build();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void gcMetricsPublished() {

    boolean gcMetricsEnabled =
        cluster.getSiteConfiguration().getBoolean(Property.GC_METRICS_ENABLED);

    if (!gcMetricsEnabled) {
      log.info("gc metrics are disabled with GC_METRICS_ENABLED={}", gcMetricsEnabled);
      return;
    }

    // log.trace("Client started, properties:{}", accumuloClient.properties());
    log.trace("Client started, properties:{}", accumuloClient.properties());

    MetricsFileTailer gcTail = new MetricsFileTailer("accumulo.sink.file-gc");
    Thread t1 = new Thread(gcTail);
    t1.start();

    // uncomment for manual jmx / jconsole validation - not for automated testing
    // manualValidationPause();

    try {

      long testStart = System.currentTimeMillis();

      // Read two updates, throw away the first snapshot - it could have been from a previous run
      // or another test (the file appends.)
      LineUpdate firstUpdate = waitForUpdate(-1, gcTail);
      firstUpdate = waitForUpdate(firstUpdate.getLastUpdate(), gcTail);

      Map<String,Long> firstSeenMap = parseLine(firstUpdate.getLine());

      log.trace("L:{}", firstUpdate.getLine());
      log.trace("M:{}", firstSeenMap);

      assertTrue(lookForExpectedKeys(firstSeenMap));
      sanity(testStart, firstSeenMap);

      LineUpdate nextUpdate = waitForUpdate(firstUpdate.getLastUpdate(), gcTail);

      Map<String,Long> updateSeenMap = parseLine(nextUpdate.getLine());

      log.debug("Line received:{}", nextUpdate.getLine());
      log.trace("Mapped values:{}", updateSeenMap);

      assertTrue(lookForExpectedKeys(updateSeenMap));
      sanity(testStart, updateSeenMap);

      validate(firstSeenMap, updateSeenMap);

    } catch (Exception ex) {
      log.debug("reads", ex);
    }
  }

  /**
   * This method just sleeps for a while (test will likely time out) The pause is to allow manual
   * validation of metrics by connecting to the running gc process with jconsole (or other jmx
   * utility). It should not be used for automatic testing.
   */
  @SuppressWarnings("unused")
  private void manualValidationPause() {
    try {
      Thread.sleep(320_000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Validate metrics for consistency withing a run cycle.
   *
   * @param values
   *          map of values from one run cycle.
   */
  private void sanity(final long testStart, final Map<String,Long> values) {

    long start = values.get("AccGcStarted");
    long finished = values.get("AccGcFinished");

    log.debug("test start: {}, gc start: {}, gc finished: {}", testStart, start, finished);

    assertTrue(start >= testStart);
    assertTrue(finished >= start);

    start = values.get("AccGcWalStarted");
    finished = values.get("AccGcWalFinished");

    log.debug("test start: {}, gc start: {}, gc finished: {}", testStart, start, finished);

    assertTrue(start >= testStart);
    assertTrue(finished >= start);

  }

  /**
   * A series of sanity checks for the metrics between different update cycles, some values should
   * be at least different, and some of the checks can include ordering.
   *
   * @param firstSeen
   *          map of first metric update
   * @param update
   *          map of a later metric update.
   */
  private void validate(Map<String,Long> firstSeen, Map<String,Long> update) {

    log.debug("First: {}, Update: {}", firstSeen, update);

    assertTrue("update should start after first",
        update.get("AccGcStarted") > firstSeen.get("AccGcStarted"));
    assertTrue("update should finish after first ",
        update.get("AccGcFinished") > firstSeen.get("AccGcFinished"));
    assertTrue("wal collect should start after gc cycle",
        firstSeen.get("AccGcWalStarted") >= firstSeen.get("AccGcFinished"));
    assertTrue("wal collect should start after gc cycle",
        update.get("AccGcWalStarted") >= update.get("AccGcFinished"));
    assertTrue("cycle count should increment",
        update.get("AccGcRunCycleCount") > firstSeen.get("AccGcRunCycleCount"));
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
      if (token.startsWith(GcMetrics.GC_METRIC_PREFIX)) {
        String[] parts = token.split("=");
        m.put(parts[0], Long.parseLong(parts[1]));
      }
    }
    return m;
  }

  private static class LineUpdate {
    private final long lastUpdate;
    private final String line;

    LineUpdate(long lastUpdate, String line) {
      this.lastUpdate = lastUpdate;
      this.line = line;
    }

    long getLastUpdate() {
      return lastUpdate;
    }

    String getLine() {
      return line;
    }
  }

  private LineUpdate waitForUpdate(final long prevUpdate, final MetricsFileTailer tail) {

    for (int count = 0; count < NUM_TAIL_ATTEMPTS; count++) {

      String line = tail.getLast();
      long currUpdate = tail.getLastUpdate();

      if (line != null && (currUpdate != prevUpdate)) {
        return new LineUpdate(tail.getLastUpdate(), line);
      }

      try {
        Thread.sleep(TAIL_DELAY);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(ex);
      }
    }
    // not found - throw exception.
    throw new IllegalStateException(
        String.format("File source update not received after %d tries in %d sec", NUM_TAIL_ATTEMPTS,
            TimeUnit.MILLISECONDS.toSeconds(TAIL_DELAY * NUM_TAIL_ATTEMPTS)));
  }

  private boolean lookForExpectedKeys(final Map<String,Long> received) {

    for (String e : EXPECTED_METRIC_KEYS) {
      if (!received.containsKey(e)) {
        return false;
      }
    }

    return true;
  }
}

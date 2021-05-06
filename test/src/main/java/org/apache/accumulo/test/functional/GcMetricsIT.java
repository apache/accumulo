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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.gc.metrics.GcMetrics;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.MetricsFileTailer;
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
public class GcMetricsIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(GcMetricsIT.class);
  private static final int NUM_TAIL_ATTEMPTS = 20;
  private static final long TAIL_DELAY = 5_000;
  private static final Pattern metricLinePattern = Pattern.compile("^(?<timestamp>\\d+).*");
  private static final Pattern metricScrapePattern =
      Pattern.compile("^(?<timestamp>\\d+).*", Pattern.DOTALL);
  private static final String[] EXPECTED_METRIC_KEYS = new String[] {"AccGcCandidates",
      "AccGcDeleted", "AccGcErrors", "AccGcFinished", "AccGcInUse", "AccGcPostOpDuration",
      "AccGcRunCycleCount", "AccGcStarted", "AccGcWalCandidates", "AccGcWalDeleted",
      "AccGcWalErrors", "AccGcWalFinished", "AccGcWalInUse", "AccGcWalStarted"};
  private static final String[] EXPECTED_PROMETHEUS_METRIC_KEYS = new String[] {"AccGc_candidates",
      "AccGc_deleted", "AccGc_errors", "AccGc_finished", "AccGc_in_use", "AccGc_post_op_duration",
      "AccGc_run_cycle_count", "AccGc_started", "AccGc_wal_candidates", "AccGc_wal_deleted",
      "AccGc_wal_errors", "AccGc_wal_finished", "AccGc_wal_in_use", "AccGc_wal_started"};
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final String GC_METRICS_URL = "http://localhost:9235/metrics";
  private static final HttpRequest REQUEST =
      HttpRequest.newBuilder().uri(java.net.URI.create(GC_METRICS_URL)).build();

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GC_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GC_CYCLE_START, "5s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "15s");
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private final MetricsFileTailer gcTail = new MetricsFileTailer("accumulo.sink.file-gc");

  @Before
  public void startTailer() {
    gcTail.startDaemonThread();
  }

  @After
  public void stopTailer() {
    gcTail.close();
  }

  @Test
  public void gcMetricsPublishedHadoop() throws Exception {
    assumeTrue("gc metrics are disabled with GC_METRICS_ENABLED=false",
        cluster.getSiteConfiguration().getBoolean(Property.GC_METRICS_ENABLED));

    // uncomment for manual jmx / jconsole validation - not for automated testing
    // Thread.sleep(320_000);

    long testStart = System.currentTimeMillis();
    log.debug("Test started: {}", testStart);

    LineUpdate firstGc = captureMetricsAfterTimestamp(testStart);
    log.debug("First captured metrics finished at {}", firstGc.gcWalFinished);

    LineUpdate secondGc = captureMetricsAfterTimestamp(firstGc.gcWalFinished);
    log.debug("Second captured metrics finished at {}", secondGc.gcWalFinished);

    firstGc.compareWithSubsequentRun(secondGc);
  }

  @Test
  public void gcMetricsPublishedMicrometer() throws Exception {
    assumeTrue("gc metrics are disabled with GC_METRICS_ENABLED=false",
        cluster.getSiteConfiguration().getBoolean(Property.GC_METRICS_ENABLED));

    // uncomment for manual jmx / jconsole validation - not for automated testing
    // Thread.sleep(320_000);

    long testStart = System.currentTimeMillis();
    log.debug("Test started: {}", testStart);

    ScrapeUpdate firstGc = captureMetricsAfterTimestampPrometheus(testStart);
    log.debug("First captured metrics finished at {}", firstGc.gcWalFinished);

    ScrapeUpdate secondGc = captureMetricsAfterTimestampPrometheus(firstGc.gcWalFinished);
    log.debug("Second captured metrics finished at {}", secondGc.gcWalFinished);

    firstGc.compareWithSubsequentRunPrometheus(secondGc);
  }

  private static class LineUpdate {
    private final long gcStarted;
    private final long gcFinished;
    private final long gcWalStarted;
    private final long gcWalFinished;
    private final long gcRunCycleCount;
    private final Map<String,Long> values;

    LineUpdate(String line) {
      values = parseLine(line);
      gcStarted = values.get("AccGcStarted");
      gcFinished = values.get("AccGcFinished");
      gcWalStarted = values.get("AccGcWalStarted");
      gcWalFinished = values.get("AccGcWalFinished");
      gcRunCycleCount = values.get("AccGcRunCycleCount");

      // ensure internal consistency
      assertTrue(Stream.of(EXPECTED_METRIC_KEYS).allMatch(values::containsKey));
      assertTrue(gcStarted <= gcFinished);
      assertTrue(gcWalStarted <= gcWalFinished);
    }

    void compareWithSubsequentRun(LineUpdate update) {
      log.debug("First run: {}", values);
      log.debug("Second run: {}", update.values);
      assertTrue("first gc should finish before second starts", gcFinished < update.gcStarted);
      assertTrue("first gcWal should finish before second starts",
          gcWalFinished < update.gcWalStarted);
      assertTrue("cycle count should increment", gcRunCycleCount < update.gcRunCycleCount);
    }

    /**
     * The hadoop metrics file sink published records as a line with comma separated key=value
     * pairs. This method parses the line and extracts the key, value pair from metrics that start
     * with AccGc and returns them in a sorted map.
     *
     * @return a map of the metrics that start with AccGc
     */
    private static Map<String,Long> parseLine(String line) {
      log.debug("Line received:{}", line);
      Map<String,Long> values = Collections.emptyMap();
      values = Stream.of(line.split(",")).map(String::trim)
          .filter(s -> s.startsWith(GcMetrics.GC_METRIC_PREFIX)).map(s -> s.split("="))
          .collect(toMap(a -> a[0], a -> Long.parseLong(a[1]), (a, b) -> {
            throw new IllegalArgumentException("Metric field found with two values '" + a
                + "' and '" + b + "' in GC Metrics line: " + line);
          }, TreeMap::new));

      log.debug("Mapped values:{}", values);
      return values;
    }
  }

  private LineUpdate captureMetricsAfterTimestamp(final long timestamp) throws Exception {
    for (int count = 0; count < NUM_TAIL_ATTEMPTS; count++) {
      String line = gcTail.getLast();
      // check for valid metrics line update since the previous update
      if (isValidRecentMetricsLine(line, timestamp)) {
        LineUpdate lineUpdate = new LineUpdate(line);
        // capture metrics for a regular gc event and a wal gc event that both occurred
        // after the specified timestamp, and not just the same entry in the metrics file
        // from a subsequent poll that represents the previous gc events;
        // note that regular gc events can be updated in the metrics before the subsequent
        // wal gc event that follows it, so the wal gc timestamps may be earlier than the
        // regular gc timestamps; that's okay, as long as they both occur after the provided
        // timestamp, and are internally consistent (start times < finish times)
        if (timestamp < lineUpdate.gcStarted && timestamp < lineUpdate.gcWalStarted) {
          return lineUpdate;
        }
      }
      Thread.sleep(TAIL_DELAY);
    }
    throw new IllegalStateException(
        String.format("File source update not received after %d tries in %d sec", NUM_TAIL_ATTEMPTS,
            TimeUnit.MILLISECONDS.toSeconds(TAIL_DELAY * NUM_TAIL_ATTEMPTS)));
  }

  private boolean isValidRecentMetricsLine(final String line, final long prevTimestamp) {
    if (Objects.isNull(line)) {
      return false;
    }
    Matcher m = metricLinePattern.matcher(line);
    if (!m.matches()) {
      return false;
    }
    try {
      long timestamp = Long.parseLong(m.group("timestamp"));
      return timestamp > prevTimestamp;
    } catch (NumberFormatException ex) {
      log.debug("Could not parse timestamp from line '{}", line);
      return false;
    }
  }

  private static class ScrapeUpdate {
    private final long gcStarted;
    private final long gcFinished;
    private final long gcWalStarted;
    private final long gcWalFinished;
    private final long gcRunCycleCount;
    private final Map<String,Long> values;

    ScrapeUpdate(String scrape) {
      values = parseScrape(scrape);
      gcStarted = values.get("AccGc_started");
      gcFinished = values.get("AccGc_finished");
      gcWalStarted = values.get("AccGc_wal_started");
      gcWalFinished = values.get("AccGc_wal_finished");
      gcRunCycleCount = values.get("AccGc_run_cycle_count");

      // ensure internal consistency
      assertTrue(Stream.of(EXPECTED_PROMETHEUS_METRIC_KEYS).allMatch(values::containsKey));
      assertTrue(gcStarted <= gcFinished);
      assertTrue(gcWalStarted <= gcWalFinished);
    }

    void compareWithSubsequentRunPrometheus(ScrapeUpdate update) {
      log.debug("First run: {}", values);
      log.debug("Second run: {}", update.values);
      assertTrue("first gc should finish before second starts", gcFinished < update.gcStarted);
      assertTrue("first gcWal should finish before second starts",
          gcWalFinished < update.gcWalStarted);
      assertTrue("cycle count should increment", gcRunCycleCount < update.gcRunCycleCount);
    }

    /**
     * Prometheus displays metrics as a space separated key value pair. This method parses the
     * Prometheus output into a map.
     *
     * @return a map of the metrics that start with AccGc
     */
    private static Map<String,Long> parseScrape(String scrape) {
      Map<String,Long> values = new TreeMap<>();
      Scanner scanner = new Scanner(scrape);
      assertTrue("Scrape is empty", scanner.hasNextLine());
      scanner.nextLine(); // bypass the timestamp
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        if (line.startsWith("#") || line.isBlank() || line.isEmpty()) {
          continue; // do nothing/skip this line
        } else {
          String[] temp = line.split(" "); // split meter name from value
          assertEquals("Error while splitting line", 2, temp.length);
          String key = temp[0]; // meter name
          BigInteger value = new BigDecimal(temp[1]).toBigInteger(); // meter value
          values.put(key, value.longValue());
        }
      }
      log.debug("Mapped values:{}", values);
      return values;
    }
  }

  private ScrapeUpdate captureMetricsAfterTimestampPrometheus(final long timestamp)
      throws Exception {
    for (int count = 0; count < NUM_TAIL_ATTEMPTS; count++) {
      String scrape = getPrometheusScrape();
      // check for valid metrics scrape update since the previous update
      if (isValidRecentMetricsScrape(scrape, timestamp)) {
        ScrapeUpdate scrapeUpdate = new ScrapeUpdate(scrape);
        // capture metrics for a regular gc event and a wal gc event that both occurred
        // after the specified timestamp, and not just the same entry in the metrics file
        // from a subsequent poll that represents the previous gc events;
        // note that regular gc events can be updated in the metrics before the subsequent
        // wal gc event that follows it, so the wal gc timestamps may be earlier than the
        // regular gc timestamps; that's okay, as long as they both occur after the provided
        // timestamp, and are internally consistent (start times < finish times)
        if (timestamp < scrapeUpdate.gcStarted && timestamp < scrapeUpdate.gcWalStarted) {
          return scrapeUpdate;
        }
      }
      Thread.sleep(TAIL_DELAY);
    }
    throw new IllegalStateException(
        String.format("File source update not received after %d tries in %d sec", NUM_TAIL_ATTEMPTS,
            TimeUnit.MILLISECONDS.toSeconds(TAIL_DELAY * NUM_TAIL_ATTEMPTS)));
  }

  private boolean isValidRecentMetricsScrape(final String scrape, final long prevTimestamp) {
    if (Objects.isNull(scrape)) {
      return false;
    }
    Matcher m = metricScrapePattern.matcher(scrape);
    if (!m.matches()) {
      return false;
    }
    try {
      long timestamp = Long.parseLong(m.group("timestamp"));
      return timestamp > prevTimestamp;
    } catch (NumberFormatException ex) {
      log.debug("Could not parse timestamp from scrape '{}", scrape);
      return false;
    }
  }

  private String getPrometheusScrape() throws InterruptedException {
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
      Thread.sleep(TAIL_DELAY);
    }
    return scrape.get();
  }
}

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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
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
  private static final String[] EXPECTED_METRIC_KEYS = new String[] {"AccGcCandidates",
      "AccGcDeleted", "AccGcErrors", "AccGcFinished", "AccGcInUse", "AccGcPostOpDuration",
      "AccGcRunCycleCount", "AccGcStarted", "AccGcWalCandidates", "AccGcWalDeleted",
      "AccGcWalErrors", "AccGcWalFinished", "AccGcWalInUse", "AccGcWalStarted"};

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
  public void gcMetricsPublished() throws Exception {
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
      log.debug("First run: {}, Second run: {}", values, update.values);
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
      if (line != null) {
        values = Stream.of(line.split(",")).map(String::trim)
            .filter(s -> s.startsWith(GcMetrics.GC_METRIC_PREFIX)).map(s -> s.split("="))
            .collect(toMap(a -> a[0], a -> Long.parseLong(a[1]), (a, b) -> {
              throw new IllegalArgumentException("Metric field found with two values '" + a
                  + "' and '" + b + "' in GC Metrics line: " + line);
            }, TreeMap::new));
      }
      log.debug("Mapped values:{}", values);
      return values;
    }
  }

  private LineUpdate captureMetricsAfterTimestamp(final long timestamp) throws Exception {
    for (int count = 0; count < NUM_TAIL_ATTEMPTS; count++) {
      String line = gcTail.getLast();
      // check for valid metrics line update since the previous update
      if (line != null && isValidRecentMetricsLine(line, timestamp)) {
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
    if (m.matches()) {
      try {
        long timestamp = Long.parseLong(m.group("timestamp"));
        return timestamp > prevTimestamp;
      } catch (NumberFormatException ex) {
        log.debug("Could not parse timestamp from line '{}", line);
        return false;
      }
    }
    return false;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.gc.metrics.GcMetrics;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.metrics.MetricsFileTailer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test that uses a hadoop metrics 2 file sink to read published metrics for
 * verification.
 */
public class GcMetricsIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(GcMetricsIT.class);

  private AccumuloClient accumuloClient;

  private static final int NUM_TAIL_ATTEMPTS = 20;
  private static final long TAIL_DELAY = 5_000;
  private static final Set<String> EXPECTED_METRIC_KEYS =
      new HashSet<>(Arrays.asList("AccGcCandidates", "AccGcDeleted", "AccGcErrors", "AccGcFinished",
          "AccGcInUse", "AccGcPostOpDuration", "AccGcRunCycleCount", "AccGcStarted",
          "AccGcWalCandidates", "AccGcWalDeleted", "AccGcWalErrors", "AccGcWalFinished",
          "AccGcWalInUse", "AccGcWalStarted"));

  @Before
  public void setup() {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void gcMetricsPublished() {

    log.trace("Client started, properties:{}", accumuloClient.properties());

    MetricsFileTailer gcTail = new MetricsFileTailer("accumulo.sink.file-gc");
    Thread t1 = new Thread(gcTail);
    t1.start();

    try {

      long testStart = System.currentTimeMillis();

      MetricsFileTailer.LineUpdate firstUpdate =
          gcTail.waitForUpdate(-1, NUM_TAIL_ATTEMPTS, TAIL_DELAY);

      Map<String,String> firstSeenMap = gcTail.parseLine(GcMetrics.GC_METRIC_PREFIX);

      log.trace("L:{}", firstUpdate.getLine());
      log.trace("M:{}", firstSeenMap);

      assertTrue(lookForExpectedKeys(firstSeenMap));
      sanity(testStart, firstSeenMap);

      MetricsFileTailer.LineUpdate nextUpdate =
          gcTail.waitForUpdate(firstUpdate.getLastUpdate(), NUM_TAIL_ATTEMPTS, TAIL_DELAY);

      Map<String,String> updateSeenMap = gcTail.parseLine(GcMetrics.GC_METRIC_PREFIX);

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
   * Validate metrics for consistency withing a run cycle.
   *
   * @param values
   *          map of values from one run cycle.
   */
  private void sanity(final long testStart, final Map<String,String> values) {

    long start = Long.parseLong(values.get("AccGcStarted"));
    long finished = Long.parseLong(values.get("AccGcFinished"));
    assertTrue(start >= testStart);
    assertTrue(finished >= start);

    start = Long.parseLong(values.get("AccGcWalStarted"));
    finished = Long.parseLong(values.get("AccGcWalFinished"));
    assertTrue(start >= testStart);
    assertTrue(finished >= start);

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
  private void validate(Map<String,String> firstSeen, Map<String,String> nextSeen) {

    log.info("First: {}, Next: {}", firstSeen, nextSeen);

    assertTrue(Long.parseLong(nextSeen.get("AccGcStarted"))
        >= Long.parseLong(firstSeen.get("AccGcStarted")));

    String s1 = firstSeen.get("AccGcWalStarted");
    String s2 = nextSeen.get("AccGcWalStarted");

    if (s1 != null && s2 != null) {
      Long first = Long.parseLong(s1);
      Long next = Long.parseLong(s2);
      if (first > 0 && next > 0) {
        assertTrue(first <= next);
      }
    }

    s1 = firstSeen.get("AccGcWalFinished");
    s2 = nextSeen.get("AccGcWalFinished");

    if (s1 != null && s2 != null) {
      Long first = Long.parseLong(s1);
      Long next = Long.parseLong(s2);
      if (first > 0 && next > 0) {
        assertTrue(first <= next);
      }
    }

    assertTrue(Long.parseLong(nextSeen.get("AccGcRunCycleCount"))
        >= Long.parseLong(firstSeen.get("AccGcRunCycleCount")));
  }

  private boolean lookForExpectedKeys(final Map<String,String> received) {

    for (String e : EXPECTED_METRIC_KEYS) {
      if (!received.containsKey(e)) {
        return false;
      }
    }

    return true;
  }
}

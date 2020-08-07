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
package org.apache.accumulo.master.metrics.fate;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.ReadOnlyTStore;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.master.metrics.MasterMetrics;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateMetrics extends MasterMetrics {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(10);
  private long minimumRefreshDelay;

  private static final String FATE_TX_STATE_METRIC_PREFIX = "FateTxState_";
  private static final String FATE_OP_TYPE_METRIC_PREFIX = "FateTxOpType_";

  private final MutableGaugeLong currentFateOps;
  private final MutableGaugeLong zkChildFateOpsTotal;
  private final MutableGaugeLong zkConnectionErrorsTotal;

  private final Map<String,MutableGaugeLong> fateTypeCounts = new TreeMap<>();
  private final Map<String,MutableGaugeLong> fateOpCounts = new TreeMap<>();

  /*
   * lock should be used to guard read and write access to metricValues and the lastUpdate
   * timestamp.
   */
  private final Lock metricsValuesLock = new ReentrantLock();
  private FateMetricValues metricValues;
  private volatile long lastUpdate = 0;

  private final ServerContext context;
  private final ReadOnlyTStore<FateMetrics> zooStore;
  private final String fateRootPath;

  public FateMetrics(final ServerContext context, final long minimumRefreshDelay) {
    super("Fate", "Fate Metrics", "fate");

    this.context = context;
    fateRootPath = context.getZooKeeperRoot() + Constants.ZFATE;

    try {

      zooStore = new ZooStore<>(fateRootPath, context.getZooReaderWriter());

    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "FATE Metrics - Failed to create zoo store - metrics unavailable", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "FATE Metrics - Interrupt received while initializing zoo store");
    }

    this.minimumRefreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);

    // lock not necessary in constructor.
    metricValues = FateMetricValues.getFromZooKeeper(context, fateRootPath, zooStore);

    MetricsRegistry registry = super.getRegistry();

    currentFateOps = registry.newGauge("currentFateOps", "Current number of FATE Ops", 0L);
    zkChildFateOpsTotal = registry.newGauge("totalFateOps", "Total FATE Ops", 0L);
    zkConnectionErrorsTotal =
        registry.newGauge("totalZkConnErrors", "Total ZK Connection Errors", 0L);

    for (ReadOnlyTStore.TStatus t : ReadOnlyTStore.TStatus.values()) {
      MutableGaugeLong g = registry.newGauge(FATE_TX_STATE_METRIC_PREFIX + t.name().toUpperCase(),
          "Transaction count for " + t.name() + " transactions", 0L);
      fateTypeCounts.put(t.name(), g);
    }
  }

  /**
   * For testing only: allow refresh delay to be set to any value, over riding the enforced minimum.
   *
   * @param minimumRefreshDelay
   *          set new min refresh value, in seconds.
   */
  void overrideRefresh(final long minimumRefreshDelay) {
    long delay = Math.max(0, minimumRefreshDelay);
    this.minimumRefreshDelay = TimeUnit.SECONDS.toMillis(delay);
  }

  @Override
  protected void prepareMetrics() {

    metricsValuesLock.lock();
    try {

      long now = System.currentTimeMillis();

      if ((lastUpdate + minimumRefreshDelay) < now) {
        metricValues = FateMetricValues.getFromZooKeeper(context, fateRootPath, zooStore);
        lastUpdate = now;

        recordValues();
      }
    } finally {
      metricsValuesLock.unlock();
    }
  }

  /**
   * Update the metrics gauges from the measured values - this method assumes that concurrent access
   * is controlled externally to this method with the metricsValueLock, and that the lock has been
   * acquired before calling this method, and then released by the caller.
   */
  private void recordValues() {

    // update individual gauges that are reported.
    currentFateOps.set(metricValues.getCurrentFateOps());
    zkChildFateOpsTotal.set(metricValues.getZkFateChildOpsTotal());
    zkConnectionErrorsTotal.set(metricValues.getZkConnectionErrors());

    // the number FATE Tx states (NEW< IN_PROGRESS...) are fixed - the underlying
    // getTxStateCounters call will return a current valid count for each possible state.
    Map<String,Long> states = metricValues.getTxStateCounters();

    states.forEach((key,
        value) -> fateTypeCounts.computeIfAbsent(key,
            v -> super.getRegistry().newGauge(metricNameHelper(FATE_TX_STATE_METRIC_PREFIX, key),
                "By transaction state count for " + key, value))
            .set(value));

    // the op types are dynamic and the metric gauges generated when first seen. After
    // that the values need to be cleared and set to any new values present. This is so
    // that the metrics system will report "known" values once seen. In operation, the
    // number of types will be a fairly small set and should populate a consistent set
    // with normal operations.

    // clear current values.
    fateOpCounts.forEach((key, value) -> value.set(0));

    // update new counts, create new gauge if first time seen.
    Map<String,Long> opTypes = metricValues.getOpTypeCounters();

    log.trace("OP Counts Before: prev {}, updates {}", fateOpCounts, opTypes);

    opTypes.forEach((key,
        value) -> fateOpCounts.computeIfAbsent(key,
            gauge -> super.getRegistry().newGauge(metricNameHelper(FATE_OP_TYPE_METRIC_PREFIX, key),
                "By transaction op type count for " + key, value))
            .set(value));

    log.trace("OP Counts After: prev {}, updates {}", fateOpCounts, opTypes);

  }

  private String metricNameHelper(final String prefix, final String name) {
    return prefix + name;
  }

}

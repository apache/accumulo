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
package org.apache.accumulo.manager.metrics.fate;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.ReadOnlyTStore;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.manager.metrics.ManagerMetrics;
import org.apache.accumulo.server.ServerContext;
// import org.apache.hadoop.metrics2.lib.MetricsRegistry;
// import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class FateMetrics extends ManagerMetrics {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(10);
  private long minimumRefreshDelay;

  // private static final String FATE_TX_STATE_METRIC_PREFIX = "FateTxState_";
  // private static final String FATE_OP_TYPE_METRIC_PREFIX = "FateTxOpType_";

  private static final String FATE_TX_STATE_METRIC_PREFIX1 = "fate.tx.state.";
  private static final String FATE_OP_TYPE_METRIC_PREFIX1 = "fate.tx.op.type.";
  /*
   * private final MutableGaugeLong currentFateOps; private final MutableGaugeLong
   * zkChildFateOpsTotal; private final MutableGaugeLong zkConnectionErrorsTotal;
   */
  private final Gauge currentFateOps1;
  private final Gauge zkChildFateOpsTotal1;
  private final Gauge zkConnectionErrorsTotal1;

  // private final Map<String,MutableGaugeLong> fateTypeCounts = new TreeMap<>();
  // private final Map<String,MutableGaugeLong> fateOpCounts = new TreeMap<>();

  private final Map<String,Gauge> fateTypeCounts1 = new TreeMap<>();
  private final Map<String,AtomicLong> fateOpCounts1 = new TreeMap<>();

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

  private SimpleMeterRegistry registry1;

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

    // MetricsRegistry registry = super.getRegistry();
    // MeterRegistry registry1 = super.getRegistry1();
    registry1 = new SimpleMeterRegistry();
    // may not be the best option, may want to register to the Composite in Metrics.java instead.
    Metrics.globalRegistry.add(registry1);

    /*
     * currentFateOps = registry.newGauge("currentFateOps", "Current number of FATE Ops", 0L);
     * zkChildFateOpsTotal = registry.newGauge("totalFateOps", "Total FATE Ops", 0L);
     * zkConnectionErrorsTotal = registry.newGauge("totalZkConnErrors",
     * "Total ZK Connection Errors", 0L);
     */
    // replacing hadoop gauge with micrometer gauge
    // micrometer gauge does not need to be set, rather initialized with a function to find the
    // value
    currentFateOps1 =
        Gauge.builder("current.fate.ops", metricValues, FateMetricValues::getCurrentFateOps)
            .description("Current number of FATE Ops").register(registry1);
    zkChildFateOpsTotal1 =
        Gauge.builder("total.fate.ops", metricValues, FateMetricValues::getZkFateChildOpsTotal)
            .description("Total FATE Ops").register(registry1);
    zkConnectionErrorsTotal1 =
        Gauge.builder("total.zk.conn.errors", metricValues, FateMetricValues::getZkConnectionErrors)
            .description("Total ZK Connection Errors").register(registry1);

    // creating the gauges for transaction state counters here since we dont need to set them i.e.
    // they auto-update based on the defined value function, we dont need to manually set them
    /*
     * for (ReadOnlyTStore.TStatus t : ReadOnlyTStore.TStatus.values()) { log.debug("Fate Type: {}",
     * t.name().toUpperCase()); AtomicLong l = registry1.gauge(FATE_TX_STATE_METRIC_PREFIX1 +
     * t.name().toUpperCase(), new AtomicLong(0)); fateTypeCounts1.put(t.name(), l); }
     * log.debug("HERE: {}",fateTypeCounts1);
     */
    for (String key : metricValues.getTxStateCounters().keySet()) {
      log.debug("TX Key: {}", key);
      Gauge g = Gauge
          .builder(FATE_TX_STATE_METRIC_PREFIX1 + key.toUpperCase(), metricValues,
              v -> FateMetricValues.getFromZooKeeper(context, fateRootPath, zooStore)
                  .getTxStateCounters().get(key))
          .description("Transaction count for " + key).register(registry1);
      // adding to the map like before
      // may be able to replace this with composite registry
      fateTypeCounts1.put(key, g);
    }
    /*
     * log.debug("HERE: {}",metricValues.getOpTypeCounters().keySet()); for (String key :
     * metricValues.getOpTypeCounters().keySet()) { log.debug("OP Type COunter: {}", key);
     * AtomicLong l = registry1.gauge(FATE_OP_TYPE_METRIC_PREFIX1 + key.toUpperCase(), new
     * AtomicLong(0)); fateOpCounts1.put(key,l); }
     */
    /*
     * for (String key : metricValues.getOpTypeCounters().keySet()) { Gauge g = Gauge
     * .builder(FATE_OP_TYPE_METRIC_PREFIX1 + key.toUpperCase(), metricValues, v ->
     * FateMetricValues.getFromZooKeeper(context, fateRootPath, zooStore)
     * .getOpTypeCounters().get(key)) .description("By transaction op type count for " +
     * key).register(registry1); // adding to the map like before // may be able to replace this
     * with composite registry fateOpCounts1.put(key, g); }
     */

  }

  /**
   * For testing only: allow refresh delay to be set to any value, over riding the enforced minimum.
   *
   * @param minimumRefreshDelay
   *          set new min refresh value, in seconds.
   */
  public void overrideRefresh(final long minimumRefreshDelay) {
    long delay = Math.max(0, minimumRefreshDelay);
    this.minimumRefreshDelay = TimeUnit.SECONDS.toMillis(delay);
  }

  @Override
  public void prepareMetrics() {
    long now = System.currentTimeMillis();

    if ((lastUpdate + minimumRefreshDelay) < now) {
      metricsValuesLock.lock();
      try {

        metricValues = FateMetricValues.getFromZooKeeper(context, fateRootPath, zooStore);
        lastUpdate = now;

        recordValues();

      } finally {
        metricsValuesLock.unlock();
      }
    }
  }

  /**
   * Update the metrics gauges from the measured values - this method assumes that concurrent access
   * is controlled externally to this method with the metricsValueLock, and that the lock has been
   * acquired before calling this method, and then released by the caller.
   */
  private void recordValues() {

    // update individual gauges that are reported.
    // currentFateOps.set(metricValues.getCurrentFateOps());
    // zkChildFateOpsTotal.set(metricValues.getZkFateChildOpsTotal());
    // zkConnectionErrorsTotal.set(metricValues.getZkConnectionErrors());

    // the number FATE Tx states (NEW< IN_PROGRESS...) are fixed - the underlying
    // getTxStateCounters call will return a current valid count for each possible state.
    // Map<String,Long> states = metricValues.getTxStateCounters();

    /*
     * REPLACED WITH MICROMETER GAUGE IN CONSTRUCTOR states.forEach((key, value) ->
     * fateTypeCounts.computeIfAbsent(key, v ->
     * super.getRegistry().newGauge(metricNameHelper(FATE_TX_STATE_METRIC_PREFIX, key),
     * "By transaction state count for " + key, value)) .set(value));
     */
    // the op types are dynamic and the metric gauges generated when first seen. After
    // that the values need to be cleared and set to any new values present. This is so
    // that the metrics system will report "known" values once seen. In operation, the
    // number of types will be a fairly small set and should populate a consistent set
    // with normal operations.

    // DONT NEED THIS SINCE MICROMETER GAUGE DOESNT NEED TO BE SET
    // clear current values.
    fateOpCounts1.forEach((key, value) -> value.set(0));

    // update new counts, create new gauge if first time seen.
    Map<String,Long> opTypes = metricValues.getOpTypeCounters();

    log.debug("OP Counts Before: prev {}, updates {}", fateOpCounts1, opTypes);

    opTypes
        .forEach((key, value) -> fateOpCounts1
            .computeIfAbsent(key,
                v -> registry1.gauge(FATE_OP_TYPE_METRIC_PREFIX1 + key, new AtomicLong(0)))
            .set(value));

    log.debug("OP Counts After: prev {}, updates {}", fateOpCounts1, opTypes);

  }

  public MeterRegistry getRegistry1() {
    return this.registry1;
  }

  /*
   * not needed? just use + or similar built-in sting concat operation private String
   * metricNameHelper(final String prefix, final String name) { return prefix + name; }
   */

}

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
package org.apache.accumulo.master.metrics.fate;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateHadoop2Metrics implements Metrics, MetricsSource {

  private static final Logger log = LoggerFactory.getLogger(FateHadoop2Metrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(10);

  private volatile long minimumRefreshDelay;

  // metrics tag / labels
  public static final String NAME = MASTER_NAME + ",sub=Fate";
  public static final String DESCRIPTION = "Fate Metrics";
  public static final String CONTEXT = "master";
  public static final String RECORD = "fate";

  // metric value names
  public static final String CUR_FATE_OPS = "currentFateOps";
  public static final String TOTAL_FATE_OPS = "totalFateOps";
  public static final String TOTAL_ZK_CONN_ERRORS = "totalZkConnErrors";
  private static final String FATE_TX_STATE_METRIC_PREFIX = "FateTxState_";
  private static final String FATE_OP_TYPE_METRIC_PREFIX = "FateTxOpType_";

  // metric values
  private final MutableGaugeLong currentFateOps;
  private final MutableGaugeLong zkChildFateOpsTotal;
  private final MutableGaugeLong zkConnectionErrorsTotal;
  private final Map<String,MutableGaugeLong> fateTypeCounts = new TreeMap<>();
  private final Map<String,MutableGaugeLong> fateOpCounts = new TreeMap<>();

  private FateMetricSnapshot metricSnapshot;

  private final String instanceId;
  private final MetricsSystem metricsSystem;
  private final MetricsRegistry registry;

  private final Lock metricsValuesLock = new ReentrantLock();
  private volatile long lastUpdate = 0;

  private final ZooReaderWriter zoo;

  public FateHadoop2Metrics(final String instanceId, final MetricsSystem metricsSystem,
      final long minimumRefreshDelay) {

    this.instanceId = instanceId;

    zoo = ZooReaderWriter.getInstance();

    this.minimumRefreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);

    this.metricsSystem = metricsSystem;
    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    this.registry.tag(MsInfo.ProcessName, MetricsSystemHelper.getProcessName());

    currentFateOps = registry.newGauge(CUR_FATE_OPS, "Current number of FATE Ops", 0L);
    zkChildFateOpsTotal = registry.newGauge(TOTAL_FATE_OPS, "Total FATE Ops", 0L);
    zkConnectionErrorsTotal =
        registry.newGauge(TOTAL_ZK_CONN_ERRORS, "Total ZK Connection Errors", 0L);

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
  public void register() {
    metricsSystem.register(NAME, DESCRIPTION, this);
  }

  @Override
  public void add(String name, long time) {
    throw new UnsupportedOperationException("add() is not implemented");
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  protected void prepareMetrics() {

    metricsValuesLock.lock();
    try {

      long now = System.currentTimeMillis();

      if ((lastUpdate + minimumRefreshDelay) < now) {

        log.trace("Update fate metrics, lastUpdate: {}, now {}", lastUpdate, now);

        metricSnapshot = FateMetricSnapshot.getFromZooKeeper(instanceId, zoo);
        lastUpdate = now;

        recordValues();
      }
    } finally {
      metricsValuesLock.unlock();
    }
  }

  /**
   * Update the metrics gauges from the measured values. This method assumes that concurrent access
   * is controlled externally to this method using the metricsValueLock, and that the lock has been
   * acquired before calling this method.
   */
  private void recordValues() {

    // update individual gauges that are reported.
    currentFateOps.set(metricSnapshot.getCurrentFateOps());
    zkChildFateOpsTotal.set(metricSnapshot.getZkFateChildOpsTotal());
    zkConnectionErrorsTotal.set(metricSnapshot.getZkConnectionErrors());

    // the number FATE Tx states (NEW< IN_PROGRESS...) are fixed - the underlying
    // getTxStateCounters call will return a current valid count for each possible state.
    Map<String,Long> states = metricSnapshot.getTxStateCounters();

    states.forEach((key, value) -> {
      fateTypeCounts.computeIfAbsent(key,
          v -> registry.newGauge(metricNameHelper(FATE_TX_STATE_METRIC_PREFIX, key),
              "By transaction state count for " + key, value))
          .set(value);
    });

    // the op types are dynamic and the metric gauges generated when first seen. After
    // that the values need to be cleared and set any new values present. This is so
    // that the metrics system will report "known" values once seen. In operation, the
    // number of types will be a fairly small set and should populate with normal operations.

    // clear current values.
    fateOpCounts.forEach((key, value) -> value.set(0));

    // update new counts, create new gauge if first time seen.
    Map<String,Long> opTypes = metricSnapshot.getOpTypeCounters();

    log.trace("OP Counts Before: prev {}, updates {}", fateOpCounts, opTypes);

    opTypes.forEach((key, value) -> {
      fateOpCounts.computeIfAbsent(key,
          guage -> registry.newGauge(metricNameHelper(FATE_OP_TYPE_METRIC_PREFIX, key),
              "By transaction op type count for " + key, value))
          .set(value);
    });

    log.trace("OP Counts After: prev {}, updates {}", fateOpCounts, opTypes);

  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {

    prepareMetrics();

    recordValues();

    // create the metrics record and publish to the registry.
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);
    registry.snapshot(builder, all);

  }

  private String metricNameHelper(final String prefix, final String name) {
    return prefix + name;
  }

}

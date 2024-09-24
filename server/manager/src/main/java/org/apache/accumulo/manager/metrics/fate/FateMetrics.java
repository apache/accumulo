/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.metrics.fate;

import static org.apache.accumulo.core.metrics.Metric.FATE_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.FATE_OPS;
import static org.apache.accumulo.core.metrics.Metric.FATE_OPS_ACTIVITY;
import static org.apache.accumulo.core.metrics.Metric.FATE_TX;
import static org.apache.accumulo.core.metrics.Metric.FATE_TYPE_IN_PROGRESS;

import java.util.EnumMap;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.ReadOnlyTStore;
import org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

public class FateMetrics implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(5);

  private static final String OP_TYPE_TAG = "op.type";

  private final ServerContext context;
  private final ReadOnlyTStore<FateMetrics> zooStore;
  private final String fateRootPath;
  private final long refreshDelay;

  private final AtomicLong totalCurrentOpsCount = new AtomicLong(0);
  private final AtomicLong totalOpsCount = new AtomicLong(0);
  private final AtomicLong fateErrorsCount = new AtomicLong(0);
  private final EnumMap<TStatus,AtomicLong> txStatusCounters = new EnumMap<>(TStatus.class);

  public FateMetrics(final ServerContext context, final long minimumRefreshDelay) {

    this.context = context;
    this.fateRootPath = context.getZooKeeperRoot() + Constants.ZFATE;
    this.refreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);

    try {
      this.zooStore = new ZooStore<>(fateRootPath, context.getZooReaderWriter());
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "FATE Metrics - Failed to create zoo store - metrics unavailable", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "FATE Metrics - Interrupt received while initializing zoo store");
    }

    for (TStatus status : TStatus.values()) {
      txStatusCounters.put(status, new AtomicLong(0));
    }

  }

  private void update() {

    FateMetricValues metricValues =
        FateMetricValues.getFromZooKeeper(context, fateRootPath, zooStore);

    totalCurrentOpsCount.set(metricValues.getCurrentFateOps());
    totalOpsCount.set(metricValues.getZkFateChildOpsTotal());
    fateErrorsCount.set(metricValues.getZkConnectionErrors());

    for (Entry<TStatus,Long> entry : metricValues.getTxStateCounters().entrySet()) {
      try {
        AtomicLong counter = txStatusCounters.get(entry.getKey());
        if (counter != null) {
          counter.set(entry.getValue());
        } else {
          log.warn("Unhandled TStatus: {}", entry.getKey());
        }
      } catch (IllegalArgumentException e) {
        log.warn("Unknown TStatus value: {}", entry.getKey(), e);
      }
    }

    metricValues.getOpTypeCounters().forEach((name, count) -> Metrics
        .gauge(FATE_TYPE_IN_PROGRESS.getName(), Tags.of(OP_TYPE_TAG, name), count));
  }

  @Override
  public void registerMetrics(final MeterRegistry registry) {
    Gauge.builder(FATE_OPS.getName(), totalCurrentOpsCount, AtomicLong::get)
        .description(FATE_OPS.getDescription()).register(registry);
    Gauge.builder(FATE_OPS_ACTIVITY.getName(), totalOpsCount, AtomicLong::get)
        .description(FATE_OPS_ACTIVITY.getDescription()).register(registry);
    Gauge.builder(FATE_ERRORS.getName(), fateErrorsCount, AtomicLong::get)
        .description(FATE_ERRORS.getDescription()).tags("type", "zk.connection").register(registry);

    txStatusCounters.forEach((status, counter) -> Gauge
        .builder(FATE_TX.getName(), counter, AtomicLong::get).description(FATE_TX.getDescription())
        .tags("state", status.name().toLowerCase()).register(registry));

    update();

    // get fate status is read only operation - no reason to be nice on shutdown.
    ScheduledExecutorService scheduler =
        ThreadPools.getServerThreadPools().createScheduledExecutorService(1, "fateMetricsPoller");
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));

    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
      try {
        update();
      } catch (Exception ex) {
        log.info("Failed to update fate metrics due to exception", ex);
      }
    }, refreshDelay, refreshDelay, TimeUnit.MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

}

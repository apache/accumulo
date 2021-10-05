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

import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MicrometerMetricsFactory;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.ReadOnlyTStore;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public class FateMetrics implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(5);

  private final ServerContext context;
  private final ReadOnlyTStore<FateMetrics> zooStore;
  private final String fateRootPath;
  private final long refreshDelay;

  private AtomicLong currentOpsGauge;
  private AtomicLong totalOpsGauge;
  private AtomicLong fateErrorsGauge;
  private AtomicLong newTxGauge;
  private AtomicLong inProgressTxGauge;
  private AtomicLong failedInProgressTxGauge;
  private AtomicLong failedTxGauge;
  private AtomicLong successfulTxGauge;
  private AtomicLong unknownTxGauge;

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

  }

  /**
   * For testing only: force refresh delay, over riding the enforced minimum.
   */
  public void overrideRefresh() {
    update();
  }

  public void update() {

    FateMetricValues metricValues =
        FateMetricValues.getFromZooKeeper(context, fateRootPath, zooStore);

    currentOpsGauge.set(metricValues.getCurrentFateOps());
    totalOpsGauge.set(metricValues.getZkFateChildOpsTotal());
    fateErrorsGauge.set(metricValues.getZkConnectionErrors());

    for (Entry<String,Long> vals : metricValues.getTxStateCounters().entrySet()) {
      switch (ReadOnlyTStore.TStatus.valueOf(vals.getKey())) {
        case NEW:
          newTxGauge.set(vals.getValue());
          break;
        case IN_PROGRESS:
          inProgressTxGauge.set(vals.getValue());
          break;
        case FAILED_IN_PROGRESS:
          failedInProgressTxGauge.set(vals.getValue());
          break;
        case FAILED:
          failedTxGauge.set(vals.getValue());
          break;
        case SUCCESSFUL:
          successfulTxGauge.set(vals.getValue());
          break;
        case UNKNOWN:
          unknownTxGauge.set(vals.getValue());
          break;
        default:
          log.warn("Unhandled status type: {}", vals.getKey());
      }
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    currentOpsGauge = registry.gauge(METRICS_FATE_CURRENT_OPS,
        MicrometerMetricsFactory.getCommonTags(), new AtomicLong(0));
    totalOpsGauge = registry.gauge(METRICS_FATE_TOTAL_OPS, MicrometerMetricsFactory.getCommonTags(),
        new AtomicLong(0));
    fateErrorsGauge = registry.gauge(METRICS_FATE_ERRORS,
        Tags.concat(MicrometerMetricsFactory.getCommonTags(), "type", "zk.connection"),
        new AtomicLong(0));
    newTxGauge =
        registry.gauge(METRICS_FATE_TX, Tags.concat(MicrometerMetricsFactory.getCommonTags(),
            "state", ReadOnlyTStore.TStatus.NEW.name()), new AtomicLong(0));
    inProgressTxGauge =
        registry.gauge(METRICS_FATE_TX, Tags.concat(MicrometerMetricsFactory.getCommonTags(),
            "state", ReadOnlyTStore.TStatus.IN_PROGRESS.name()), new AtomicLong(0));
    failedInProgressTxGauge =
        registry.gauge(METRICS_FATE_TX, Tags.concat(MicrometerMetricsFactory.getCommonTags(),
            "state", ReadOnlyTStore.TStatus.FAILED_IN_PROGRESS.name()), new AtomicLong(0));
    failedTxGauge =
        registry.gauge(METRICS_FATE_TX, Tags.concat(MicrometerMetricsFactory.getCommonTags(),
            "state", ReadOnlyTStore.TStatus.FAILED.name()), new AtomicLong(0));
    successfulTxGauge =
        registry.gauge(METRICS_FATE_TX, Tags.concat(MicrometerMetricsFactory.getCommonTags(),
            "state", ReadOnlyTStore.TStatus.SUCCESSFUL.name()), new AtomicLong(0));
    unknownTxGauge =
        registry.gauge(METRICS_FATE_TX, Tags.concat(MicrometerMetricsFactory.getCommonTags(),
            "state", ReadOnlyTStore.TStatus.UNKNOWN.name()), new AtomicLong(0));

    update();

    // get fate status is read only operation - no reason to be nice on shutdown.
    ScheduledExecutorService scheduler =
        ThreadPools.createScheduledExecutorService(1, "fateMetricsPoller", false);
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));

    scheduler.scheduleAtFixedRate(() -> {
      try {
        update();
      } catch (Exception ex) {
        log.info("Failed to update fate metrics due to exception", ex);
      }
    }, refreshDelay, refreshDelay, TimeUnit.MILLISECONDS);

  }

}

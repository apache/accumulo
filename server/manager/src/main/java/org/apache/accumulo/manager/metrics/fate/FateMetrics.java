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

import io.micrometer.core.instrument.Tags;

public class FateMetrics implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(5);

  private final ServerContext context;
  private final ReadOnlyTStore<FateMetrics> zooStore;
  private final String fateRootPath;

  private final AtomicLong currentOpsGauge;
  private final AtomicLong totalOpsGauge;
  private final AtomicLong fateErrorsGauge;
  private final AtomicLong newTxGauge;
  private final AtomicLong inProgressTxGauge;
  private final AtomicLong failedInProgressTxGauge;
  private final AtomicLong failedTxGauge;
  private final AtomicLong successfulTxGauge;
  private final AtomicLong unknownTxGauge;

  public FateMetrics(final ServerContext context, final long minimumRefreshDelay) {

    this.context = context;
    this.fateRootPath = context.getZooKeeperRoot() + Constants.ZFATE;

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

    final String txMetricName = getMetricsPrefix() + "tx";

    currentOpsGauge =
        MicrometerMetricsFactory.getRegistry().gauge(getMetricsPrefix() + "ops.current",
            MicrometerMetricsFactory.getCommonTags(), new AtomicLong(0));
    totalOpsGauge = MicrometerMetricsFactory.getRegistry().gauge(getMetricsPrefix() + "ops.total",
        MicrometerMetricsFactory.getCommonTags(), new AtomicLong(0));
    fateErrorsGauge = MicrometerMetricsFactory.getRegistry().gauge(getMetricsPrefix() + "errors",
        Tags.concat(MicrometerMetricsFactory.getCommonTags(), "type", "zk.connection"),
        new AtomicLong(0));
    newTxGauge = MicrometerMetricsFactory.getRegistry().gauge(txMetricName,
        Tags.concat(MicrometerMetricsFactory.getCommonTags(), "state",
            ReadOnlyTStore.TStatus.NEW.name()),
        new AtomicLong(0));
    inProgressTxGauge = MicrometerMetricsFactory.getRegistry().gauge(txMetricName,
        Tags.concat(MicrometerMetricsFactory.getCommonTags(), "state",
            ReadOnlyTStore.TStatus.IN_PROGRESS.name()),
        new AtomicLong(0));
    failedInProgressTxGauge = MicrometerMetricsFactory.getRegistry().gauge(txMetricName,
        Tags.concat(MicrometerMetricsFactory.getCommonTags(), "state",
            ReadOnlyTStore.TStatus.FAILED_IN_PROGRESS.name()),
        new AtomicLong(0));
    failedTxGauge = MicrometerMetricsFactory.getRegistry().gauge(txMetricName,
        Tags.concat(MicrometerMetricsFactory.getCommonTags(), "state",
            ReadOnlyTStore.TStatus.FAILED.name()),
        new AtomicLong(0));
    successfulTxGauge = MicrometerMetricsFactory.getRegistry().gauge(txMetricName,
        Tags.concat(MicrometerMetricsFactory.getCommonTags(), "state",
            ReadOnlyTStore.TStatus.SUCCESSFUL.name()),
        new AtomicLong(0));
    unknownTxGauge = MicrometerMetricsFactory.getRegistry().gauge(txMetricName,
        Tags.concat(MicrometerMetricsFactory.getCommonTags(), "state",
            ReadOnlyTStore.TStatus.UNKNOWN.name()),
        new AtomicLong(0));

    long delay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);

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
    }, delay, delay, TimeUnit.MILLISECONDS);

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
      AtomicLong ref = null;
      switch (ReadOnlyTStore.TStatus.valueOf(vals.getKey())) {
        case NEW:
          ref = newTxGauge;
          break;
        case IN_PROGRESS:
          ref = inProgressTxGauge;
          break;
        case FAILED_IN_PROGRESS:
          ref = failedInProgressTxGauge;
          break;
        case FAILED:
          ref = failedTxGauge;
          break;
        case SUCCESSFUL:
          ref = successfulTxGauge;
          break;
        case UNKNOWN:
          ref = unknownTxGauge;
          break;
        default:
          log.warn("Unhandled status type: {}", vals.getKey());
      }
      ref.set(vals.getValue());
    }
  }

  @Override
  public String getMetricsPrefix() {
    return "accumulo.fate.";
  }

}

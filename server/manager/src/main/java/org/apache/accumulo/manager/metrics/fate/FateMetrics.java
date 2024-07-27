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

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public abstract class FateMetrics<T extends FateMetricValues> implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(5);

  private static final String OP_TYPE_TAG = "op.type";

  protected final ServerContext context;
  protected final ReadOnlyFateStore<FateMetrics<T>> fateStore;
  protected final long refreshDelay;

  protected final AtomicLong totalCurrentOpsGauge = new AtomicLong(0);
  protected final AtomicLong newTxGauge = new AtomicLong(0);
  protected final AtomicLong submittedTxGauge = new AtomicLong(0);
  protected final AtomicLong inProgressTxGauge = new AtomicLong(0);
  protected final AtomicLong failedInProgressTxGauge = new AtomicLong(0);
  protected final AtomicLong failedTxGauge = new AtomicLong(0);
  protected final AtomicLong successfulTxGauge = new AtomicLong(0);
  protected final AtomicLong unknownTxGauge = new AtomicLong(0);

  public FateMetrics(final ServerContext context, final long minimumRefreshDelay) {
    this.context = context;
    this.refreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);
    this.fateStore = Objects.requireNonNull(buildStore(context));
  }

  protected abstract ReadOnlyFateStore<FateMetrics<T>> buildStore(ServerContext context);

  protected abstract T getMetricValues();

  protected void update() {
    update(getMetricValues());
  }

  protected void update(T metricValues) {
    totalCurrentOpsGauge.set(metricValues.getCurrentFateOps());

    for (Entry<String,Long> vals : metricValues.getTxStateCounters().entrySet()) {
      switch (ReadOnlyFateStore.TStatus.valueOf(vals.getKey())) {
        case NEW:
          newTxGauge.set(vals.getValue());
          break;
        case SUBMITTED:
          submittedTxGauge.set(vals.getValue());
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
    metricValues.getOpTypeCounters().forEach((name, count) -> {
      Metrics.gauge(METRICS_FATE_TYPE_IN_PROGRESS, Tags.of(OP_TYPE_TAG, name), count);
    });
  }

  @Override
  public void registerMetrics(final MeterRegistry registry) {
    var type = fateStore.type().name().toLowerCase();
    var instanceTypeTag = Tag.of("instanceType", type);

    registry.gauge(METRICS_FATE_OPS, totalCurrentOpsGauge);

    registry.gauge(METRICS_FATE_TX, List
        .of(Tag.of("state", ReadOnlyFateStore.TStatus.NEW.name().toLowerCase()), instanceTypeTag),
        newTxGauge);
    registry.gauge(METRICS_FATE_TX,
        List.of(Tag.of("state", ReadOnlyFateStore.TStatus.SUBMITTED.name().toLowerCase()),
            instanceTypeTag),
        submittedTxGauge);
    registry.gauge(METRICS_FATE_TX,
        List.of(Tag.of("state", ReadOnlyFateStore.TStatus.IN_PROGRESS.name().toLowerCase()),
            instanceTypeTag),
        inProgressTxGauge);
    registry.gauge(METRICS_FATE_TX,
        List.of(Tag.of("state", ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS.name().toLowerCase()),
            instanceTypeTag),
        failedInProgressTxGauge);
    registry.gauge(METRICS_FATE_TX,
        List.of(Tag.of("state", ReadOnlyFateStore.TStatus.FAILED.name().toLowerCase()),
            instanceTypeTag),
        failedTxGauge);
    registry.gauge(METRICS_FATE_TX,
        List.of(Tag.of("state", ReadOnlyFateStore.TStatus.SUCCESSFUL.name().toLowerCase()),
            instanceTypeTag),
        successfulTxGauge);
    registry.gauge(METRICS_FATE_TX,
        List.of(Tag.of("state", ReadOnlyFateStore.TStatus.UNKNOWN.name().toLowerCase()),
            instanceTypeTag),
        unknownTxGauge);

    // get fate status is read only operation - no reason to be nice on shutdown.
    ScheduledExecutorService scheduler = ThreadPools.getServerThreadPools()
        .createScheduledExecutorService(1, type + "FateMetricsPoller");
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));

    // Only update as part of the scheduler thread.
    // We have to call update() in a new thread because this method to
    // register metrics is called on start up in the Manager before it's finished
    // initializing, so we can't scan the User fate store until after startup is done.
    // If we called update() here in this method directly we would get stuck forever.
    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
      try {
        update();
      } catch (Exception ex) {
        log.info("Failed to update {}fate metrics due to exception", type, ex);
      }
    }, 0, refreshDelay, TimeUnit.MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

}

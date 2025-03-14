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

import static org.apache.accumulo.core.metrics.Metric.FATE_OPS;
import static org.apache.accumulo.core.metrics.Metric.FATE_TX;
import static org.apache.accumulo.core.metrics.Metric.FATE_TYPE_IN_PROGRESS;

import java.util.EnumMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

public abstract class FateMetrics<T extends FateMetricValues> implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(5);

  private static final String OP_TYPE_TAG = "op.type";

  protected final ServerContext context;
  protected final ReadOnlyFateStore<FateMetrics<T>> readOnlyFateStore;
  protected final long refreshDelay;

  protected final AtomicLong totalCurrentOpsCount = new AtomicLong(0);
  private final EnumMap<TStatus,AtomicLong> txStatusCounters = new EnumMap<>(TStatus.class);

  public FateMetrics(final ServerContext context, final long minimumRefreshDelay) {
    this.context = context;
    this.refreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);
    this.readOnlyFateStore = Objects.requireNonNull(buildReadOnlyStore(context));

    for (TStatus status : TStatus.values()) {
      txStatusCounters.put(status, new AtomicLong(0));
    }
  }

  protected abstract ReadOnlyFateStore<FateMetrics<T>> buildReadOnlyStore(ServerContext context);

  protected abstract T getMetricValues();

  protected void update() {
    update(getMetricValues());
  }

  protected void update(T metricValues) {
    totalCurrentOpsCount.set(metricValues.getCurrentFateOps());

    for (Entry<TStatus,Long> entry : metricValues.getTxStateCounters().entrySet()) {
      AtomicLong counter = txStatusCounters.get(entry.getKey());
      if (counter != null) {
        counter.set(entry.getValue());
      } else {
        log.warn("Unhandled TStatus: {}", entry.getKey());
      }
    }

    metricValues.getOpTypeCounters().forEach((name, count) -> Metrics
        .gauge(FATE_TYPE_IN_PROGRESS.getName(), Tags.of(OP_TYPE_TAG, name), count));
  }

  @Override
  public void registerMetrics(final MeterRegistry registry) {
    String type = readOnlyFateStore.type().name().toLowerCase();

    Gauge.builder(FATE_OPS.getName(), totalCurrentOpsCount, AtomicLong::get)
        .description(FATE_OPS.getDescription()).register(registry);

    txStatusCounters.forEach((status, counter) -> Gauge
        .builder(FATE_TX.getName(), counter, AtomicLong::get).description(FATE_TX.getDescription())
        .tags("state", status.name().toLowerCase(), "instanceType", type).register(registry));

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

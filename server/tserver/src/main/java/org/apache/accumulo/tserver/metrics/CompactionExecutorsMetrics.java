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
package org.apache.accumulo.tserver.metrics;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.tserver.compactions.CompactionManager.ExtCompMetric;

import com.google.common.collect.Sets;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public class CompactionExecutorsMetrics implements MetricsProducer {

  private volatile Supplier<Collection<ExtCompMetric>> externalMetricsSupplier;

  private volatile List<CeMetrics> ceMetricsList = List.of();
  private final Map<CompactionExecutorId,CeMetrics> ceMetricsMap = new HashMap<>();
  private final Map<CompactionExecutorId,ExMetrics> exCeMetricsMap = new HashMap<>();
  private volatile MeterRegistry registry = null;

  // public so it can be closed by outside callers
  public class CeMetrics implements AutoCloseable {
    private final AtomicInteger queued;
    private final AtomicInteger running;

    private IntSupplier runningSupplier;
    private IntSupplier queuedSupplier;

    private CeMetrics(CompactionExecutorId ceid) {
      if (registry != null) {
        this.queued = registry.gauge(METRICS_MAJC_QUEUED, Tags.of("id", ceid.canonical()),
            new AtomicInteger(0));
        this.running = registry.gauge(METRICS_MAJC_RUNNING, Tags.of("id", ceid.canonical()),
            new AtomicInteger(0));
      } else {
        // these vars have no effect on metrics in this case - just avoids NPEs
        this.queued = new AtomicInteger(0);
        this.running = new AtomicInteger(0);
      }
    }

    @Override
    public void close() {
      runningSupplier = () -> 0;
      queuedSupplier = () -> 0;
      running.set(0);
      queued.set(0);
    }
  }

  private class ExMetrics {
    private final AtomicInteger queued;
    private final AtomicInteger running;

    private ExMetrics(CompactionExecutorId ceid) {
      if (registry != null) {
        this.queued = registry.gauge(METRICS_MAJC_QUEUED, Tags.of("id", ceid.canonical()),
            new AtomicInteger(0));
        this.running = registry.gauge(METRICS_MAJC_RUNNING, Tags.of("id", ceid.canonical()),
            new AtomicInteger(0));
      } else {
        // these vars have no effect on metrics in this case - just avoids NPEs
        this.queued = new AtomicInteger(0);
        this.running = new AtomicInteger(0);
      }
    }
  }

  public CompactionExecutorsMetrics() {
    startUpdateThread();
  }

  protected void startUpdateThread() {
    ScheduledExecutorService scheduler = ThreadPools.getServerThreadPools()
        .createScheduledExecutorService(1, "compactionExecutorsMetricsPoller");
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));
    long minimumRefreshDelay = TimeUnit.SECONDS.toMillis(5);
    ThreadPools.watchNonCriticalScheduledTask(scheduler.scheduleAtFixedRate(this::update,
        minimumRefreshDelay, minimumRefreshDelay, TimeUnit.MILLISECONDS));
  }

  public synchronized CeMetrics addExecutor(CompactionExecutorId ceid, IntSupplier runningSupplier,
      IntSupplier queuedSupplier) {

    synchronized (ceMetricsMap) {

      CeMetrics cem = ceMetricsMap.computeIfAbsent(ceid, id -> new CeMetrics(ceid));

      cem.runningSupplier = runningSupplier;
      cem.queuedSupplier = queuedSupplier;

      ceMetricsList = List.copyOf(ceMetricsMap.values());

      return cem;
    }

  }

  public void update() {

    if (externalMetricsSupplier != null) {

      Set<CompactionExecutorId> seenIds = new HashSet<>();

      synchronized (exCeMetricsMap) {
        externalMetricsSupplier.get().forEach(ecm -> {
          seenIds.add(ecm.ceid);

          ExMetrics exm = exCeMetricsMap.computeIfAbsent(ecm.ceid, id -> new ExMetrics(ecm.ceid));

          exm.queued.set(ecm.queued);
          exm.running.set(ecm.running);

        });

        Sets.difference(exCeMetricsMap.keySet(), seenIds).forEach(unusedId -> {
          ExMetrics exm = exCeMetricsMap.get(unusedId);
          exm.queued.set(0);
          exm.running.set(0);
        });

      }
    }
    ceMetricsList.forEach(cem -> {
      cem.running.set(cem.runningSupplier.getAsInt());
      cem.queued.set(cem.queuedSupplier.getAsInt());
    });
  }

  public void setExternalMetricsSupplier(Supplier<Collection<ExtCompMetric>> ems) {
    this.externalMetricsSupplier = ems;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // Meters are registered dynamically. Save off the reference to the
    // registry to use at that time.
    this.registry = registry;
  }

}

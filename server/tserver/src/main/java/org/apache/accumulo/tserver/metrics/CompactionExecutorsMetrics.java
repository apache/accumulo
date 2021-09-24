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

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.tserver.compactions.CompactionManager.ExtCompMetric;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

import com.google.common.collect.Sets;

import io.micrometer.core.instrument.MeterRegistry;

public class CompactionExecutorsMetrics extends TServerMetrics {

  private volatile Supplier<Collection<ExtCompMetric>> externalMetricsSupplier;

  private volatile List<CeMetrics> ceMetricsList = List.of();
  private final Map<CompactionExecutorId,CeMetrics> ceMetricsMap = new HashMap<>();
  private final Map<CompactionExecutorId,ExMetrics> exCeMetricsMap = new HashMap<>();
  private final MeterRegistry micrometerRegistry;

  CompactionExecutorsMetricsHadoop hadoopMetrics;

  private static class CeMetrics {
    AtomicInteger queued;
    AtomicInteger running;

    IntSupplier runningSupplier;
    IntSupplier queuedSupplier;
  }

  private static class ExMetrics {
    AtomicInteger queued;
    AtomicInteger running;
  }

  public CompactionExecutorsMetrics(MeterRegistry registryMicrometer) {
    super("compactionExecutors");
    this.micrometerRegistry = registryMicrometer;

    ScheduledExecutorService scheduler =
        ThreadPools.createScheduledExecutorService(1, "compactionExecutorsMetricsPoller", false);
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));
    long minimumRefreshDelay = TimeUnit.SECONDS.toMillis(5);
    scheduler.scheduleAtFixedRate(this::update, minimumRefreshDelay, minimumRefreshDelay,
        TimeUnit.MILLISECONDS);

    hadoopMetrics = new CompactionExecutorsMetricsHadoop(super.getRegistry());
  }

  public synchronized AutoCloseable addExecutor(CompactionExecutorId ceid,
      IntSupplier runningSupplier, IntSupplier queuedSupplier) {

    hadoopMetrics.addExecutor(ceid, runningSupplier, queuedSupplier);

    synchronized (ceMetricsMap) {

      CeMetrics cem = ceMetricsMap.computeIfAbsent(ceid, id -> {
        CeMetrics m = new CeMetrics();
        m.queued = micrometerRegistry.gauge(id.canonical().replace('.', '_') + "_queued",
            new AtomicInteger(0));
        m.running = micrometerRegistry.gauge(id.canonical().replace('.', '_') + "_running",
            new AtomicInteger(0));
        return m;
      });

      cem.runningSupplier = runningSupplier;
      cem.queuedSupplier = queuedSupplier;

      ceMetricsList = List.copyOf(ceMetricsMap.values());

      return () -> {

        cem.runningSupplier = () -> 0;
        cem.queuedSupplier = () -> 0;

        cem.running.set(0);
        cem.queued.set(0);
      };
    }

  }

  @Override
  public void prepareMetrics() {
    hadoopMetrics.prepareMetrics();
  }

  public void update() {

    if (externalMetricsSupplier != null) {

      Set<CompactionExecutorId> seenIds = new HashSet<>();

      synchronized (exCeMetricsMap) {
        externalMetricsSupplier.get().forEach(ecm -> {
          seenIds.add(ecm.ceid);

          ExMetrics exm = exCeMetricsMap.computeIfAbsent(ecm.ceid, id -> {
            ExMetrics m = new ExMetrics();
            m.queued = micrometerRegistry.gauge(id.canonical().replace('.', '_') + "_queued",
                new AtomicInteger(0));
            m.running = micrometerRegistry.gauge(id.canonical().replace('.', '_') + "_running",
                new AtomicInteger(0));
            return m;
          });

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

  private class CompactionExecutorsMetricsHadoop {

    private volatile List<CeMetrics> ceml = List.of();
    private final Map<CompactionExecutorId,CeMetrics> metrics = new HashMap<>();
    private final Map<CompactionExecutorId,ExMetrics> exMetrics = new HashMap<>();

    MetricsRegistry registry;

    private class CeMetrics {
      MutableGaugeInt queuedGauge;
      MutableGaugeInt runningGauge;

      IntSupplier runningSupplier;
      IntSupplier queuedSupplier;
    }

    private class ExMetrics {
      MutableGaugeInt queuedGauge;
      MutableGaugeInt runningGauge;
    }

    public CompactionExecutorsMetricsHadoop(MetricsRegistry registry) {
      this.registry = registry;
    }

    public synchronized AutoCloseable addExecutor(CompactionExecutorId ceid,
        IntSupplier runningSupplier, IntSupplier queuedSupplier) {

      synchronized (metrics) {
        CeMetrics cem = metrics.computeIfAbsent(ceid, id -> {
          CeMetrics m = new CeMetrics();
          m.queuedGauge = registry.newGauge(id.canonical().replace('.', '_') + "_queued",
              "Queued compactions for executor " + id, 0);
          m.runningGauge = registry.newGauge(id.canonical().replace('.', '_') + "_running",
              "Running compactions for executor " + id, 0);
          return m;
        });

        cem.runningSupplier = runningSupplier;
        cem.queuedSupplier = queuedSupplier;

        ceml = List.copyOf(metrics.values());

        return () -> {
          cem.runningSupplier = () -> 0;
          cem.queuedSupplier = () -> 0;

          cem.runningGauge.set(0);
          cem.queuedGauge.set(0);
        };
      }
    }

    public void prepareMetrics() {

      if (externalMetricsSupplier != null) {

        Set<CompactionExecutorId> seenIds = new HashSet<>();

        synchronized (exMetrics) {
          externalMetricsSupplier.get().forEach(ecm -> {
            seenIds.add(ecm.ceid);

            ExMetrics exm = exMetrics.computeIfAbsent(ecm.ceid, id -> {
              ExMetrics m = new ExMetrics();
              m.queuedGauge = registry.newGauge(id.canonical().replace('.', '_') + "_queued",
                  "Queued compactions for executor " + id, 0);
              m.runningGauge = registry.newGauge(id.canonical().replace('.', '_') + "_running",
                  "Running compactions for executor " + id, 0);
              return m;
            });

            exm.queuedGauge.set(ecm.queued);
            exm.runningGauge.set(ecm.running);

          });

          Sets.difference(exMetrics.keySet(), seenIds).forEach(unusedId -> {
            ExMetrics exm = exMetrics.get(unusedId);
            exm.queuedGauge.set(0);
            exm.runningGauge.set(0);
          });

        }
      }
      ceml.forEach(cem -> {
        cem.runningGauge.set(cem.runningSupplier.getAsInt());
        cem.queuedGauge.set(cem.queuedSupplier.getAsInt());
      });
    }

  }

}

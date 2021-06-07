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
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.tserver.compactions.CompactionManager.ExtCompMetric;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

import com.google.common.collect.Sets;

public class CompactionExecutorsMetrics extends TServerMetrics {

  private volatile List<CeMetrics> ceml = List.of();
  private Map<CompactionExecutorId,CeMetrics> metrics = new HashMap<>();
  private Map<CompactionExecutorId,ExMetrics> exMetrics = new HashMap<>();
  private volatile Supplier<Collection<ExtCompMetric>> externalMetricsSupplier;

  private static class CeMetrics {
    MutableGaugeInt queuedGauge;
    MutableGaugeInt runningGauge;

    IntSupplier runningSupplier;
    IntSupplier queuedSupplier;
  }

  private static class ExMetrics {
    MutableGaugeInt queuedGauge;
    MutableGaugeInt runningGauge;

  }

  public CompactionExecutorsMetrics() {
    super("compactionExecutors");
  }

  public synchronized AutoCloseable addExecutor(CompactionExecutorId ceid,
      IntSupplier runningSupplier, IntSupplier queuedSupplier) {

    MetricsRegistry registry = super.getRegistry();

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

  @Override
  public void prepareMetrics() {

    if (externalMetricsSupplier != null) {

      Set<CompactionExecutorId> seenIds = new HashSet<>();

      MetricsRegistry registry = super.getRegistry();

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

  public void setExternalMetricsSupplier(Supplier<Collection<ExtCompMetric>> ems) {
    this.externalMetricsSupplier = ems;
  }

}

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

public class CompactionExecutorsMetrics extends TServerMetrics {

  private volatile List<CeMetrics> ceml = List.of();
  private Map<CompactionExecutorId,CeMetrics> metrics = new HashMap<>();

  private static class CeMetrics {
    MutableGaugeInt queuedGauge;
    MutableGaugeInt runningGauge;

    IntSupplier runningSupplier;
    IntSupplier queuedSupplier;
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
        m.queuedGauge = registry.newGauge(ceid.canonical().replace('.', '_') + "_queued",
            "Queued compactions for executor " + ceid, 0);
        m.runningGauge = registry.newGauge(ceid.canonical().replace('.', '_') + "_running",
            "Running compactions for executor " + ceid, 0);
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
    ceml.forEach(cem -> {
      cem.runningGauge.set(cem.runningSupplier.getAsInt());
      cem.queuedGauge.set(cem.queuedSupplier.getAsInt());
    });
  }

}

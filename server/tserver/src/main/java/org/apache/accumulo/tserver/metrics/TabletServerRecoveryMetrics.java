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

import static org.apache.accumulo.core.metrics.Metric.RECOVERIES_TABLETS_COMPLETED;
import static org.apache.accumulo.core.metrics.Metric.RECOVERIES_TABLETS_FAILED;
import static org.apache.accumulo.core.metrics.Metric.RECOVERIES_TABLETS_IN_PROGRESS;
import static org.apache.accumulo.core.metrics.Metric.RECOVERIES_TABLETS_MUTATIONS_REPLAYED;
import static org.apache.accumulo.core.metrics.Metric.RECOVERIES_TABLETS_STARTED;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class TabletServerRecoveryMetrics implements MetricsProducer {

  private final AtomicLong recoveriesStarted = new AtomicLong(0);
  private final AtomicLong recoveriesCompleted = new AtomicLong(0);
  private final AtomicLong recoveriesFailed = new AtomicLong(0);
  private final AtomicLong concurrentRecoveries = new AtomicLong(0);
  private final AtomicLong mutationsReplayed = new AtomicLong(0);

  public void recoveryStarted() {
    recoveriesStarted.incrementAndGet();
    concurrentRecoveries.incrementAndGet();
  }

  public void recoveryCompleted() {
    recoveriesCompleted.incrementAndGet();
    concurrentRecoveries.decrementAndGet();
  }

  public void recoveryFailed() {
    recoveriesFailed.incrementAndGet();
    concurrentRecoveries.decrementAndGet();
  }

  public void incrementMutationsReplayed() {
    mutationsReplayed.incrementAndGet();
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(RECOVERIES_TABLETS_STARTED.getName(), recoveriesStarted, AtomicLong::get)
        .description(RECOVERIES_TABLETS_STARTED.getDescription()).register(registry);
    Gauge.builder(RECOVERIES_TABLETS_COMPLETED.getName(), recoveriesCompleted, AtomicLong::get)
        .description(RECOVERIES_TABLETS_COMPLETED.getDescription()).register(registry);
    Gauge.builder(RECOVERIES_TABLETS_FAILED.getName(), recoveriesFailed, AtomicLong::get)
        .description(RECOVERIES_TABLETS_FAILED.getDescription()).register(registry);
    Gauge.builder(RECOVERIES_TABLETS_IN_PROGRESS.getName(), concurrentRecoveries, AtomicLong::get)
        .description(RECOVERIES_TABLETS_IN_PROGRESS.getDescription()).register(registry);
    Gauge
        .builder(RECOVERIES_TABLETS_MUTATIONS_REPLAYED.getName(), mutationsReplayed,
            AtomicLong::get)
        .description(RECOVERIES_TABLETS_MUTATIONS_REPLAYED.getDescription()).register(registry);
  }
}

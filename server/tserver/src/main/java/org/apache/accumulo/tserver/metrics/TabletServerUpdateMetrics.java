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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.metrics.NoopMetrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerUpdateMetrics implements MetricsProducer {

  private final AtomicLong permissionErrorsCount = new AtomicLong();
  private final AtomicLong unknownTabletErrorsCount = new AtomicLong();
  private final AtomicLong constraintViolationsCount = new AtomicLong();
  private Timer commitPrepStat = NoopMetrics.useNoopTimer();
  private Timer walogWriteTimeStat = NoopMetrics.useNoopTimer();
  private Timer commitTimeStat = NoopMetrics.useNoopTimer();
  private DistributionSummary mutationArraySizeStat = NoopMetrics.useNoopDistributionSummary();

  public void addPermissionErrors(long value) {
    permissionErrorsCount.addAndGet(value);
  }

  public void addUnknownTabletErrors(long value) {
    unknownTabletErrorsCount.addAndGet(value);
  }

  public void addConstraintViolations(long value) {
    constraintViolationsCount.addAndGet(value);
  }

  public void addCommitPrep(long value) {
    commitPrepStat.record(Duration.ofMillis(value));
  }

  public void addWalogWriteTime(long value) {
    walogWriteTimeStat.record(Duration.ofMillis(value));
  }

  public void addCommitTime(long value) {
    commitTimeStat.record(Duration.ofMillis(value));
  }

  public void addMutationArraySize(long value) {
    mutationArraySizeStat.record(value);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    FunctionCounter.builder(METRICS_UPDATE_ERRORS, permissionErrorsCount, AtomicLong::get)
        .tags("type", "permission").description("Counts permission errors").register(registry);
    FunctionCounter.builder(METRICS_UPDATE_ERRORS, unknownTabletErrorsCount, AtomicLong::get)
        .tags("type", "unknown.tablet").description("Counts unknown tablet errors")
        .register(registry);
    FunctionCounter.builder(METRICS_UPDATE_ERRORS, constraintViolationsCount, AtomicLong::get)
        .tags("type", "constraint.violation").description("Counts constraint violations")
        .register(registry);
    commitPrepStat = Timer.builder(METRICS_UPDATE_COMMIT_PREP)
        .description("preparing to commit mutations").register(registry);
    walogWriteTimeStat = Timer.builder(METRICS_UPDATE_WALOG_WRITE)
        .description("writing mutations to WAL").register(registry);
    commitTimeStat =
        Timer.builder(METRICS_UPDATE_COMMIT).description("committing mutations").register(registry);
    mutationArraySizeStat = DistributionSummary.builder(METRICS_UPDATE_MUTATION_ARRAY_SIZE)
        .description("mutation array").register(registry);
  }

}

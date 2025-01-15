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

import static org.apache.accumulo.core.metrics.Metric.UPDATE_COMMIT;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_COMMIT_PREP;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_MUTATION_ARRAY_SIZE;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_WALOG_WRITE;

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
    FunctionCounter.builder(UPDATE_ERRORS.getName(), permissionErrorsCount, AtomicLong::get)
        .tags("type", "permission").description(UPDATE_ERRORS.getDescription()).register(registry);
    FunctionCounter.builder(UPDATE_ERRORS.getName(), unknownTabletErrorsCount, AtomicLong::get)
        .tags("type", "unknown.tablet").description(UPDATE_ERRORS.getDescription())
        .register(registry);
    FunctionCounter.builder(UPDATE_ERRORS.getName(), constraintViolationsCount, AtomicLong::get)
        .tags("type", "constraint.violation").description(UPDATE_ERRORS.getDescription())
        .register(registry);
    commitPrepStat = Timer.builder(UPDATE_COMMIT_PREP.getName())
        .description(UPDATE_COMMIT_PREP.getDescription()).register(registry);
    walogWriteTimeStat = Timer.builder(UPDATE_WALOG_WRITE.getName())
        .description(UPDATE_WALOG_WRITE.getDescription()).register(registry);
    commitTimeStat = Timer.builder(UPDATE_COMMIT.getName())
        .description(UPDATE_COMMIT.getDescription()).register(registry);
    mutationArraySizeStat = DistributionSummary.builder(UPDATE_MUTATION_ARRAY_SIZE.getName())
        .description(UPDATE_MUTATION_ARRAY_SIZE.getDescription()).register(registry);
  }

}

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

import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerUpdateMetrics implements MetricsProducer {

  private Counter permissionErrorsCounter;
  private Counter unknownTabletErrorsCounter;
  private Counter constraintViolationsCounter;
  private Timer commitPrepStat;
  private Timer walogWriteTimeStat;
  private Timer commitTimeStat;
  private DistributionSummary mutationArraySizeStat;

  public void addPermissionErrors(long value) {
    permissionErrorsCounter.increment(value);
  }

  public void addUnknownTabletErrors(long value) {
    unknownTabletErrorsCounter.increment(value);
  }

  public void addConstraintViolations(long value) {
    constraintViolationsCounter.increment(value);
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
    permissionErrorsCounter = registry.counter(METRICS_UPDATE_ERRORS, "type", "permission");
    unknownTabletErrorsCounter = registry.counter(METRICS_UPDATE_ERRORS, "type", "unknown.tablet");
    constraintViolationsCounter =
        registry.counter(METRICS_UPDATE_ERRORS, "type", "constraint.violation");
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

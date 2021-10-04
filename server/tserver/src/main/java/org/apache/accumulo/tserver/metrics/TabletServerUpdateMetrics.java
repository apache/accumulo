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

import java.time.Duration;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MicrometerMetricsFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;

public class TabletServerUpdateMetrics implements MetricsProducer {

  private final Counter permissionErrorsCounter;
  private final Counter unknownTabletErrorsCounter;
  private final Counter constraintViolationsCounter;

  private final Timer commitPrepStat;
  private final Timer walogWriteTimeStat;
  private final Timer commitTimeStat;
  private final DistributionSummary mutationArraySizeStat;

  public TabletServerUpdateMetrics() {

    permissionErrorsCounter = MicrometerMetricsFactory.getRegistry()
        .counter(getMetricsPrefix() + "error", "type", "permission");
    unknownTabletErrorsCounter = MicrometerMetricsFactory.getRegistry()
        .counter(getMetricsPrefix() + "error", "type", "unknown.tablet");
    constraintViolationsCounter = MicrometerMetricsFactory.getRegistry()
        .counter(getMetricsPrefix() + "error", "type", "constraint.violation");

    commitPrepStat = Timer.builder(getMetricsPrefix() + "commit.prep")
        .description("preparing to commit mutations")
        .register(MicrometerMetricsFactory.getRegistry());
    walogWriteTimeStat = Timer.builder(getMetricsPrefix() + "walog.write")
        .description("writing mutations to WAL").register(MicrometerMetricsFactory.getRegistry());
    commitTimeStat = Timer.builder(getMetricsPrefix() + "commit")
        .description("committing mutations").register(MicrometerMetricsFactory.getRegistry());
    mutationArraySizeStat = DistributionSummary.builder(getMetricsPrefix() + "mutation.arrays.size")
        .description("mutation array").register(MicrometerMetricsFactory.getRegistry());

  }

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
  public String getMetricsPrefix() {
    return "accumulo.tserver.updates.";
  }

}

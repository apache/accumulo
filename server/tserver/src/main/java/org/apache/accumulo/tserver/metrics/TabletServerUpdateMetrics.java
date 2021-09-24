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

import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableStat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerUpdateMetrics extends TServerMetrics {

  private final Counter permissionErrorsCounter;
  private final Counter unknownTabletErrorsCounter;
  private final Counter constraintViolationsCounter;

  private final Timer commitPrepStat;
  private final Timer walogWriteTimeStat;
  private final Timer commitTimeStat;
  private final Timer mutationArraySizeStat;

  private final TabletServerUpdateMetricsHadoop hadoopMetrics;

  public TabletServerUpdateMetrics(MeterRegistry meterRegistry) {
    super("Updates");

    permissionErrorsCounter = meterRegistry.counter("permissionErrors");
    unknownTabletErrorsCounter = meterRegistry.counter("unknownTabletErrors");
    constraintViolationsCounter = meterRegistry.counter("constraintViolations");

    commitPrepStat = Timer.builder("commitPrep").description("preparing to commit mutations")
        .register(meterRegistry);
    walogWriteTimeStat = Timer.builder("waLogWriteTime").description("writing mutations to WAL")
        .register(meterRegistry);
    commitTimeStat =
        Timer.builder("commitTime").description("committing mutations").register(meterRegistry);
    mutationArraySizeStat =
        Timer.builder("mutationArraysSize").description("mutation array").register(meterRegistry);

    hadoopMetrics = new TabletServerUpdateMetricsHadoop(super.getRegistry());
  }

  public void addPermissionErrors(long value) {
    permissionErrorsCounter.increment(value);
    hadoopMetrics.addPermissionErrors(value);
  }

  public void addUnknownTabletErrors(long value) {
    unknownTabletErrorsCounter.increment(value);
    hadoopMetrics.addUnknownTabletErrors(value);
  }

  public void addConstraintViolations(long value) {
    constraintViolationsCounter.increment(value);
    hadoopMetrics.addConstraintViolations(value);
  }

  public void addCommitPrep(long value) {
    commitPrepStat.record(Duration.ofMillis(value));
    hadoopMetrics.addCommitPrep(value);
  }

  public void addWalogWriteTime(long value) {
    walogWriteTimeStat.record(Duration.ofMillis(value));
    hadoopMetrics.addWalogWriteTime(value);
  }

  public void addCommitTime(long value) {
    commitTimeStat.record(Duration.ofMillis(value));
    hadoopMetrics.addCommitTime(value);
  }

  public void addMutationArraySize(long value) {
    mutationArraySizeStat.record(Duration.ofMillis(value));
    hadoopMetrics.addMutationArraySize(value);
  }

  private static class TabletServerUpdateMetricsHadoop {

    private final MutableCounterLong permissionErrorsCounter;
    private final MutableCounterLong unknownTabletErrorsCounter;
    private final MutableCounterLong constraintViolationsCounter;

    private final MutableStat commitPrepStat;
    private final MutableStat walogWriteTimeStat;
    private final MutableStat commitTimeStat;
    private final MutableStat mutationArraySizeStat;

    TabletServerUpdateMetricsHadoop(MetricsRegistry metricsRegistry) {
      permissionErrorsCounter =
          metricsRegistry.newCounter("permissionErrors", "Permission Errors", 0L);
      unknownTabletErrorsCounter =
          metricsRegistry.newCounter("unknownTabletErrors", "Unknown Tablet Errors", 0L);
      constraintViolationsCounter =
          metricsRegistry.newCounter("constraintViolations", "Table Constraint Violations", 0L);

      commitPrepStat = metricsRegistry.newStat("commitPrep", "preparing to commit mutations", "Ops",
          "Time", true);
      walogWriteTimeStat = metricsRegistry.newStat("waLogWriteTime", "writing mutations to WAL",
          "Ops", "Time", true);
      commitTimeStat =
          metricsRegistry.newStat("commitTime", "committing mutations", "Ops", "Time", true);
      mutationArraySizeStat =
          metricsRegistry.newStat("mutationArraysSize", "mutation array", "ops", "Size", true);

    }

    private void addPermissionErrors(long value) {
      permissionErrorsCounter.incr(value);
    }

    private void addUnknownTabletErrors(long value) {
      unknownTabletErrorsCounter.incr(value);
    }

    private void addConstraintViolations(long value) {
      constraintViolationsCounter.incr(value);
    }

    private void addCommitPrep(long value) {
      commitPrepStat.add(value);
    }

    private void addWalogWriteTime(long value) {
      walogWriteTimeStat.add(value);
    }

    private void addCommitTime(long value) {
      commitTimeStat.add(value);
    }

    private void addMutationArraySize(long value) {
      mutationArraySizeStat.add(value);
    }

  }

}

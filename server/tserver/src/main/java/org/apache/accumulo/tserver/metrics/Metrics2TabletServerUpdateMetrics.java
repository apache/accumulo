/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.tserver.metrics;

import org.apache.accumulo.server.metrics.Metrics;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 *
 */
public class Metrics2TabletServerUpdateMetrics implements Metrics, MetricsSource, TabletServerUpdateMetricsKeys {
  public static final String NAME = TSERVER_NAME + ",sub=Updates", DESCRIPTION = "TabletServer Update Metrics", CONTEXT = "tserver", RECORD = "Updates";

  private final MetricsSystem system;
  private final MetricsRegistry registry;

  private final MutableCounterLong permissionErrorsCounter, unknownTabletErrorsCounter, constraintViolationsCounter;
  private final MutableStat commitPrepStat, walogWriteTimeStat, commitTimeStat, mutationArraySizeStat;

  // Use TabletServerMetricsFactory
  Metrics2TabletServerUpdateMetrics(MetricsSystem system) {
    this.system = system;
    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));

    permissionErrorsCounter = registry.newCounter(Interns.info(PERMISSION_ERRORS, "Permission Errors"), 0l);
    unknownTabletErrorsCounter = registry.newCounter(Interns.info(UNKNOWN_TABLET_ERRORS, "Unknown Tablet Errors"), 0l);
    constraintViolationsCounter = registry.newCounter(Interns.info(CONSTRAINT_VIOLATIONS, "Table Constraint Violations"), 0l);

    commitPrepStat = registry.newStat(COMMIT_PREP, "preparing to commit mutations", "Ops", "Time", true);
    walogWriteTimeStat = registry.newStat(WALOG_WRITE_TIME, "writing mutations to WAL", "Ops", "Time", true);
    commitTimeStat = registry.newStat(COMMIT_TIME, "committing mutations", "Ops", "Time", true);
    mutationArraySizeStat = registry.newStat(MUTATION_ARRAY_SIZE, "mutation array", "ops", "Size", true);
  }

  @Override
  public void add(String name, long value) {
    if (PERMISSION_ERRORS.equals(name)) {
      permissionErrorsCounter.incr(value);
    } else if (UNKNOWN_TABLET_ERRORS.equals(name)) {
      unknownTabletErrorsCounter.incr(value);
    } else if (MUTATION_ARRAY_SIZE.equals(name)) {
      mutationArraySizeStat.add(value);
    } else if (COMMIT_PREP.equals(name)) {
      commitPrepStat.add(value);
    } else if (CONSTRAINT_VIOLATIONS.equals(name)) {
      constraintViolationsCounter.incr(value);
    } else if (WALOG_WRITE_TIME.equals(name)) {
      walogWriteTimeStat.add(value);
    } else if (COMMIT_TIME.equals(name)) {
      commitTimeStat.add(value);
    } else {
      throw new RuntimeException("Cannot process metric with name " + name);
    }
  }

  @Override
  public void register() {
    system.register(NAME, DESCRIPTION, this);
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);

    registry.snapshot(builder, all);
  }

}

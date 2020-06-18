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

import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableStat;

public class TabletServerUpdateMetrics extends TServerMetrics {

  private final MutableCounterLong permissionErrorsCounter;
  private final MutableCounterLong unknownTabletErrorsCounter;
  private final MutableCounterLong constraintViolationsCounter;

  private final MutableStat commitPrepStat;
  private final MutableStat walogWriteTimeStat;
  private final MutableStat commitTimeStat;
  private final MutableStat mutationArraySizeStat;

  public TabletServerUpdateMetrics() {
    super("Updates");

    MetricsRegistry registry = super.getRegistry();
    permissionErrorsCounter = registry.newCounter("permissionErrors", "Permission Errors", 0L);
    unknownTabletErrorsCounter =
        registry.newCounter("unknownTabletErrors", "Unknown Tablet Errors", 0L);
    constraintViolationsCounter =
        registry.newCounter("constraintViolations", "Table Constraint Violations", 0L);

    commitPrepStat =
        registry.newStat("commitPrep", "preparing to commit mutations", "Ops", "Time", true);
    walogWriteTimeStat =
        registry.newStat("waLogWriteTime", "writing mutations to WAL", "Ops", "Time", true);
    commitTimeStat = registry.newStat("commitTime", "committing mutations", "Ops", "Time", true);
    mutationArraySizeStat =
        registry.newStat("mutationArraysSize", "mutation array", "ops", "Size", true);
  }

  public void addPermissionErrors(long value) {
    permissionErrorsCounter.incr(value);
  }

  public void addUnknownTabletErrors(long value) {
    unknownTabletErrorsCounter.incr(value);
  }

  public void addMutationArraySize(long value) {
    mutationArraySizeStat.add(value);
  }

  public void addCommitPrep(long value) {
    commitPrepStat.add(value);
  }

  public void addConstraintViolations(long value) {
    constraintViolationsCounter.incr(value);
  }

  public void addWalogWriteTime(long value) {
    walogWriteTimeStat.add(value);
  }

  public void addCommitTime(long value) {
    commitTimeStat.add(value);
  }

}

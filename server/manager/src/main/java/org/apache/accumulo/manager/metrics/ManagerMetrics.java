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
package org.apache.accumulo.manager.metrics;

import static org.apache.accumulo.core.metrics.Metric.MANAGER_GOAL_STATE;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class ManagerMetrics implements MetricsProducer {

  private final AtomicInteger goalState = new AtomicInteger(-1);

  public void updateManagerGoalState(ManagerGoalState goal) {
    int newValue = switch (goal) {
      case CLEAN_STOP -> 0;
      case SAFE_MODE -> 1;
      case NORMAL -> 2;
    };
    goalState.set(newValue);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(MANAGER_GOAL_STATE.getName(), goalState, AtomicInteger::get)
        .description(MANAGER_GOAL_STATE.getDescription()).register(registry);
  }
}

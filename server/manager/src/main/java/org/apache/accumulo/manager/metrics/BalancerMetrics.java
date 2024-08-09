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

import java.util.function.LongSupplier;

import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class BalancerMetrics implements MetricsProducer {

  LongSupplier migratingCount;

  public void assignMigratingCount(LongSupplier f) {
    migratingCount = f;
  }

  public long getMigratingCount() {
    // Handle inital NaN value state when balance has never been called
    if (migratingCount == null) {
      return 0;
    }
    return migratingCount.getAsLong();
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge
        .builder(METRICS_MANAGER_BALANCER_MIGRATIONS_NEEDED, this,
            BalancerMetrics::getMigratingCount)
        .description("Overall total migrations that need to complete").register(registry);
  }
}

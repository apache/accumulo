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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class BalancerMetrics implements MetricsProducer {

  AtomicLong migratingCount = new AtomicLong();
  AtomicLong needMigrationCount = new AtomicLong();

  public long incrementMigratingCount() {
    return migratingCount.incrementAndGet();
  }

  public long getMigratingCount() {
    return migratingCount.get();
  }

  public void setMigratingCount(final long migratingCount) {
    this.migratingCount.set(migratingCount);
  }

  public long getNeedMigrationCount() {
    return needMigrationCount.get();
  }

  public void setNeedMigrationCount(final long needMigrationCount) {
    this.needMigrationCount.set(needMigrationCount);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge
        .builder(METRICS_MANAGER_BALANCER_MIGRATIONS_IN_PROGRESS, this,
            BalancerMetrics::getMigratingCount)
        .description("Count of migrations in progress from last balancer call").register(registry);
    Gauge
        .builder(METRICS_MANAGER_BALANCER_MIGRATIONS_NEEDED, this,
            BalancerMetrics::getNeedMigrationCount)
        .description("Overall migrations that need to be completed").register(registry);
  }
}

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

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.metrics.Metric.COMPACTION_SVC_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.MANAGER_META_TGW_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.MANAGER_ROOT_TGW_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.MANAGER_USER_TGW_ERRORS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.metrics.fate.FateMetrics;
import org.apache.accumulo.manager.metrics.fate.meta.MetaFateMetrics;
import org.apache.accumulo.manager.metrics.fate.user.UserFateMetrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class ManagerMetrics implements MetricsProducer {

  private final List<FateMetrics<?>> fateMetrics;

  private final AtomicLong rootTGWErrorsGauge = new AtomicLong(0);
  private final AtomicLong metadataTGWErrorsGauge = new AtomicLong(0);
  private final AtomicLong userTGWErrorsGauge = new AtomicLong(0);
  private final AtomicInteger compactionConfigurationError = new AtomicInteger(0);

  public ManagerMetrics(final AccumuloConfiguration conf, final Manager manager) {
    requireNonNull(conf, "AccumuloConfiguration must not be null");
    requireNonNull(conf, "Manager must not be null");
    fateMetrics = List.of(
        new MetaFateMetrics(manager.getContext(),
            conf.getTimeInMillis(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL)),
        new UserFateMetrics(manager.getContext(),
            conf.getTimeInMillis(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL)));
  }

  public void incrementTabletGroupWatcherError(DataLevel level) {
    switch (level) {
      case METADATA:
        metadataTGWErrorsGauge.incrementAndGet();
        break;
      case ROOT:
        rootTGWErrorsGauge.incrementAndGet();
        break;
      case USER:
        userTGWErrorsGauge.incrementAndGet();
        break;
      default:
        throw new IllegalStateException("Unhandled DataLevel: " + level);
    }
  }

  public void setCompactionServiceConfigurationError() {
    this.compactionConfigurationError.set(1);
  }

  public void clearCompactionServiceConfigurationError() {
    this.compactionConfigurationError.set(0);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    fateMetrics.forEach(fm -> fm.registerMetrics(registry));
    Gauge.builder(MANAGER_ROOT_TGW_ERRORS.getName(), rootTGWErrorsGauge, AtomicLong::get)
        .description(MANAGER_ROOT_TGW_ERRORS.getDescription()).register(registry);
    Gauge.builder(MANAGER_META_TGW_ERRORS.getName(), metadataTGWErrorsGauge, AtomicLong::get)
        .description(MANAGER_META_TGW_ERRORS.getDescription()).register(registry);
    Gauge.builder(MANAGER_USER_TGW_ERRORS.getName(), userTGWErrorsGauge, AtomicLong::get)
        .description(MANAGER_USER_TGW_ERRORS.getDescription()).register(registry);
    Gauge.builder(COMPACTION_SVC_ERRORS.getName(), compactionConfigurationError, AtomicInteger::get)
        .description(COMPACTION_SVC_ERRORS.getDescription()).register(registry);
  }

  public List<MetricsProducer> getProducers(AccumuloConfiguration conf, Manager manager) {
    ArrayList<MetricsProducer> producers = new ArrayList<>();
    producers.add(this);
    producers.addAll(fateMetrics);
    producers.add(manager.getCompactionCoordinator());
    return producers;
  }
}

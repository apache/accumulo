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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.metrics.fate.FateMetrics;

import io.micrometer.core.instrument.MeterRegistry;

public class ManagerMetrics implements MetricsProducer {

  private final FateMetrics fateMetrics;

  private AtomicLong rootTGWErrorsGauge;
  private AtomicLong metadataTGWErrorsGauge;
  private AtomicLong userTGWErrorsGauge;

  public ManagerMetrics(final AccumuloConfiguration conf, final Manager manager) {
    requireNonNull(conf, "AccumuloConfiguration must not be null");
    requireNonNull(conf, "Manager must not be null");
    fateMetrics = new FateMetrics(manager.getContext(),
        conf.getTimeInMillis(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL));
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

  @Override
  public void registerMetrics(MeterRegistry registry) {
    fateMetrics.registerMetrics(registry);
    rootTGWErrorsGauge = registry.gauge(METRICS_MANAGER_ROOT_TGW_ERRORS,
        MetricsUtil.getCommonTags(), new AtomicLong(0));
    metadataTGWErrorsGauge = registry.gauge(METRICS_MANAGER_META_TGW_ERRORS,
        MetricsUtil.getCommonTags(), new AtomicLong(0));
    userTGWErrorsGauge = registry.gauge(METRICS_MANAGER_USER_TGW_ERRORS,
        MetricsUtil.getCommonTags(), new AtomicLong(0));
  }
}

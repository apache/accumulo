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
package org.apache.accumulo.manager.metrics.fate.meta;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.MetaFateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.manager.metrics.fate.FateMetrics;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

public class MetaFateMetrics extends FateMetrics<MetaFateMetricValues> {

  private final String fateRootPath;
  private final AtomicLong totalOpsGauge = new AtomicLong(0);
  private final AtomicLong fateErrorsGauge = new AtomicLong(0);

  public MetaFateMetrics(ServerContext context, long minimumRefreshDelay) {
    super(context, minimumRefreshDelay);
    this.fateRootPath = getFateRootPath(context);
  }

  @Override
  protected void update(MetaFateMetricValues metricValues) {
    super.update(metricValues);
    totalOpsGauge.set(metricValues.getZkFateChildOpsTotal());
    fateErrorsGauge.set(metricValues.getZkConnectionErrors());
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    super.registerMetrics(registry);
    registry.gauge(METRICS_FATE_OPS_ACTIVITY, totalOpsGauge);
    registry.gauge(METRICS_FATE_ERRORS, List.of(Tag.of("type", "zk.connection")), fateErrorsGauge);
  }

  @Override
  protected ReadOnlyFateStore<FateMetrics<MetaFateMetricValues>> buildStore(ServerContext context) {
    try {
      return new MetaFateStore<>(getFateRootPath(context), context.getZooReaderWriter());
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "FATE Metrics - Failed to create zoo store - metrics unavailable", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "FATE Metrics - Interrupt received while initializing zoo store");
    }
  }

  @Override
  protected MetaFateMetricValues getMetricValues() {
    return MetaFateMetricValues.getMetaStoreMetrics(context, fateRootPath, fateStore);
  }

  private static String getFateRootPath(ServerContext context) {
    return context.getZooKeeperRoot() + Constants.ZFATE;
  }
}

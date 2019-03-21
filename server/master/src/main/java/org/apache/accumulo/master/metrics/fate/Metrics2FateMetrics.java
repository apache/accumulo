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
package org.apache.accumulo.master.metrics.fate;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics2FateMetrics implements Metrics, MetricsSource {

  private static final Logger log = LoggerFactory.getLogger(Metrics2FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(10);

  private volatile long minimumRefreshDelay;

  public static final String NAME = MASTER_NAME + ",sub=Fate";
  public static final String DESCRIPTION = "Fate Metrics";
  public static final String CONTEXT = "master";
  public static final String RECORD = "fate";
  public static final String CUR_FATE_OPS = "currentFateOps";
  public static final String TOTAL_FATE_OPS = "totalFateOps";
  public static final String TOTAL_ZK_CONN_ERRORS = "totalZkConnErrors";

  private final Instance instance;
  private final MetricsSystem metricsSystem;
  private final MetricsRegistry registry;
  private final MutableGaugeLong currentFateOps;
  private final MutableGaugeLong zkChildFateOpsTotal;
  private final MutableGaugeLong zkConnectionErrorsTotal;

  private final AtomicReference<FateMetricValues> metricValues;

  private volatile long lastUpdate = 0;

  public Metrics2FateMetrics(final Instance instance, MetricsSystem metricsSystem,
      final long minimumRefreshDelay) {

    this.instance = instance;

    this.minimumRefreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);

    metricValues = new AtomicReference<>(FateMetricValues.updateFromZookeeper(instance, null));

    this.metricsSystem = metricsSystem;
    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    this.registry.tag(MsInfo.ProcessName, MetricsSystemHelper.getProcessName());

    currentFateOps = registry.newGauge(CUR_FATE_OPS, "Current number of FATE Ops", 0L);
    zkChildFateOpsTotal = registry.newGauge(TOTAL_FATE_OPS, "Total FATE Ops", 0L);
    zkConnectionErrorsTotal = registry.newGauge(TOTAL_ZK_CONN_ERRORS, "Total ZK Connection Errors",
        0L);

  }

  @Override
  public void register() throws Exception {
    metricsSystem.register(NAME, DESCRIPTION, this);
  }

  @Override
  public void add(String name, long time) {
    throw new UnsupportedOperationException("add() is not implemented");
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {

    log.trace("getMetrics called with collector: {}", collector);

    FateMetricValues fateMetrics = metricValues.get();

    long now = System.currentTimeMillis();

    if ((lastUpdate + minimumRefreshDelay) < now) {

      fateMetrics = FateMetricValues.updateFromZookeeper(instance, fateMetrics);

      metricValues.set(fateMetrics);

      lastUpdate = now;

      // update individual gauges that are reported.
      currentFateOps.set(fateMetrics.getCurrentFateOps());
      zkChildFateOpsTotal.set(fateMetrics.getZkFateChildOpsTotal());
      zkConnectionErrorsTotal.set(fateMetrics.getZkConnectionErrors());

    }

    // create the metrics record and publish to the registry.
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);
    registry.snapshot(builder, all);

  }
}

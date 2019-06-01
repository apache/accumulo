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

import org.apache.accumulo.master.metrics.MasterMetrics;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

public class FateMetrics extends MasterMetrics {

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(10);

  private volatile long minimumRefreshDelay;

  private final ServerContext context;

  private final MutableGaugeLong currentFateOps;
  private final MutableGaugeLong zkChildFateOpsTotal;
  private final MutableGaugeLong zkConnectionErrorsTotal;

  private final AtomicReference<FateMetricValues> metricValues;

  private volatile long lastUpdate = 0;

  public FateMetrics(final ServerContext context, final long minimumRefreshDelay) {
    super("Fate", "Fate Metrics", "fate");

    this.context = context;

    this.minimumRefreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);

    metricValues = new AtomicReference<>(FateMetricValues.updateFromZookeeper(context, null));

    MetricsRegistry registry = super.getRegistry();
    currentFateOps = registry.newGauge("currentFateOps", "Current number of FATE Ops", 0L);
    zkChildFateOpsTotal = registry.newGauge("totalFateOps", "Total FATE Ops", 0L);
    zkConnectionErrorsTotal =
        registry.newGauge("totalZkConnErrors", "Total ZK Connection Errors", 0L);

  }

  @Override
  protected void prepareMetrics() {
    FateMetricValues fateMetrics = metricValues.get();
    long now = System.currentTimeMillis();
    if ((lastUpdate + minimumRefreshDelay) < now) {
      fateMetrics = FateMetricValues.updateFromZookeeper(context, fateMetrics);
      metricValues.set(fateMetrics);
      lastUpdate = now;
      // update individual gauges that are reported.
      currentFateOps.set(fateMetrics.getCurrentFateOps());
      zkChildFateOpsTotal.set(fateMetrics.getZkFateChildOpsTotal());
      zkConnectionErrorsTotal.set(fateMetrics.getZkConnectionErrors());
    }
  }

}

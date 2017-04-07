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

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.metrics2.MetricsSystem;

/**
 * Factory to create Metrics instances for various TabletServer functions.
 *
 * Necessary shim to support both the custom JMX metrics from &lt;1.7.0 and the new Hadoop Metrics2 implementations.
 */
public class TabletServerMetricsFactory {

  private final boolean useOldMetrics;
  private final MetricsSystem metricsSystem;

  public TabletServerMetricsFactory(AccumuloConfiguration conf) {
    requireNonNull(conf);
    useOldMetrics = conf.getBoolean(Property.GENERAL_LEGACY_METRICS);

    if (useOldMetrics) {
      metricsSystem = null;
    } else {
      metricsSystem = MetricsSystemHelper.getInstance();
    }
  }

  /**
   * Create Metrics to track MinorCompactions
   */
  public Metrics createMincMetrics() {
    if (useOldMetrics) {
      return new TabletServerMinCMetrics();
    }

    return new Metrics2TabletServerMinCMetrics(metricsSystem);
  }

  /**
   * Create Metrics to track TabletServer state
   */
  public Metrics createTabletServerMetrics(TabletServer tserver) {
    if (useOldMetrics) {
      return new TabletServerMBeanImpl(tserver);
    }

    return new Metrics2TabletServerMetrics(tserver, metricsSystem);
  }

  /**
   * Create Metrics to track scans
   */
  public Metrics createScanMetrics() {
    if (useOldMetrics) {
      return new TabletServerScanMetrics();
    }

    return new Metrics2TabletServerScanMetrics(metricsSystem);
  }

  /**
   * Create Metrics to track updates (writes)
   */
  public Metrics createUpdateMetrics() {
    if (useOldMetrics) {
      return new TabletServerUpdateMetrics();
    }

    return new Metrics2TabletServerUpdateMetrics(metricsSystem);
  }
}

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
package org.apache.accumulo.master.metrics;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.metrics.fate.FateMetrics;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide master metrics configuration. Currently this is replication and FATE metrics. Metrics can
 * be configured using hadoop metrics2 Fate metrics must be enabled via configuration file (default
 * is disabled)
 */
public class MasterMetricsFactory {

  private final static Logger log = LoggerFactory.getLogger(MasterMetricsFactory.class);

  private final boolean enableFateMetrics;
  private final long fateMinUpdateInterval;

  public MasterMetricsFactory(AccumuloConfiguration conf) {
    requireNonNull(conf, "AccumuloConfiguration must not be null");
    enableFateMetrics = conf.getBoolean(Property.MASTER_FATE_METRICS_ENABLED);
    fateMinUpdateInterval = conf.getTimeInMillis(Property.MASTER_FATE_METRICS_MIN_UPDATE_INTERVAL);
  }

  public int register(Master master) {
    MetricsSystem metricsSystem = master.getMetricsSystem();

    int failureCount = 0;

    try {
      new ReplicationMetrics(master).register(metricsSystem);
      log.info("Registered replication metrics module");
    } catch (Exception ex) {
      failureCount++;
      log.error("Failed to register replication metrics", ex);
    }

    try {
      if (enableFateMetrics) {
        new FateMetrics(master.getContext(), fateMinUpdateInterval).register(metricsSystem);
        log.info("Registered FATE metrics module");
      }
    } catch (Exception ex) {
      failureCount++;
      log.error("Failed to register fate metrics", ex);
    }
    return failureCount;
  }

}

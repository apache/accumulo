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
package org.apache.accumulo.master.metrics;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.metrics.fate.FateMetrics;
import org.apache.accumulo.master.metrics.fate.Metrics2FateMetrics;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MasterMetricsFactory {

  private final static Logger log = LoggerFactory.getLogger(MasterMetricsFactory.class);

  private final boolean useOldMetrics;
  private final MetricsSystem metricsSystem;
  private final Master master;

  public MasterMetricsFactory(AccumuloConfiguration conf, Master master) {
    requireNonNull(conf, "AccumuloConfiguration must not be null");

    // if(true) { throw new IllegalStateException("OOPPSe"); }

    useOldMetrics = conf.getBoolean(Property.GENERAL_LEGACY_METRICS);
    this.master = master;

    if (useOldMetrics) {
      metricsSystem = null;
    } else {
      metricsSystem = MetricsSystemHelper.getInstance();
    }
  }

  public int register() {

    int failureCount = 0;

    try {

      Metrics replicationMetrics = createReplicationMetrics();

      replicationMetrics.register();

      log.info("Registered replication metrics module");

    } catch (Exception ex) {
      failureCount++;
      log.error("Failed to register replication metrics", ex);
    }

    try {

      Metrics fateMetrics = createFateMetrics();

      fateMetrics.register();

      log.info("Registered FATE metrics module");

    } catch (Exception ex) {
      failureCount++;
      log.error("Failed to register fate metrics", ex);
    }

    return failureCount;
  }

  private Metrics createReplicationMetrics() {
    if (useOldMetrics) {
      return new ReplicationMetrics(master);
    }

    return new Metrics2ReplicationMetrics(master, metricsSystem);
  }

  private Metrics createFateMetrics() {
    if (useOldMetrics) {
      return new FateMetrics(master);
    }

    return new Metrics2FateMetrics(master, metricsSystem);
  }

}

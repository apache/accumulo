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
package org.apache.accumulo.test.metrics;

import java.time.Duration;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class LoggingMetricsIT extends MetricsIT {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(2);
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL, "1s");

    // Tell the server processes to use a StatsDMeterRegistry that will be configured
    // to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY,
        TestLoggingRegistryFactory.class.getName());
  }

  @Test
  @Disabled
  @Override
  public void confirmMetricsPublished() throws Exception {
    // Override does nothing
  }

  @Test
  @Disabled
  @Override
  public void metricTags() throws Exception {
    // Override does nothing
  }

  @Test
  public void doWorkToGenerateLogs() throws Exception {
    // Metrics are pushed every 1m using a logger with the name LoggingMeterRegistry
    // we need the test to take longer than 1m
    for (int i = 0; i < 10; i++) {
      doWorkToGenerateMetrics();
      Thread.sleep(10_000);
    }
  }

}

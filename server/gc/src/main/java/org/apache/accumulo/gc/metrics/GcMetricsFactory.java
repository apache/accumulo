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
package org.apache.accumulo.gc.metrics;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcMetricsFactory {

  private final static Logger log = LoggerFactory.getLogger(GcMetricsFactory.class);

  private boolean enableMetrics;

  public GcMetricsFactory(AccumuloConfiguration conf) {
    requireNonNull(conf, "AccumuloConfiguration must not be null");
    enableMetrics = conf.getBoolean(Property.GC_METRICS_ENABLED);
  }

  public boolean register(SimpleGarbageCollector gc) {

    if (!enableMetrics) {
      log.info("Accumulo gc metrics are disabled.  To enable, set {} in configuration",
          Property.GC_METRICS_ENABLED);
      return false;
    }

    try {

      MetricsSystem metricsSystem = gc.getMetricsSystem();

      new GcMetrics(gc).register(metricsSystem);

      return true;

    } catch (Exception ex) {
      log.error("Failed to register accumulo gc metrics", ex);
      return false;
    }
  }
}

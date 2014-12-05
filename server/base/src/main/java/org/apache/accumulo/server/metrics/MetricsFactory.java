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
package org.apache.accumulo.server.metrics;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import com.google.common.base.Preconditions;

/**
 *
 */
public class MetricsFactory {

  private final boolean useOldMetrics;
  private final MetricsSystem metricsSystem;

  public MetricsFactory(AccumuloConfiguration conf) {
    Preconditions.checkNotNull(conf);
    useOldMetrics = conf.getBoolean(Property.GENERAL_LEGACY_METRICS);

    if (useOldMetrics) {
      metricsSystem = null;
    } else {
      metricsSystem = DefaultMetricsSystem.initialize(Metrics.PREFIX);
    }
  }

  public Metrics createThriftMetrics(String serverName, String threadName) {
    if (useOldMetrics) {
      return new ThriftMetrics(serverName, threadName);
    }

    return new Metrics2ThriftMetrics(metricsSystem, serverName, threadName);
  }

}

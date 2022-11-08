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
package org.apache.accumulo.server.metrics;

import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;

public class ThriftMetrics implements MetricsProducer {

  private final String serverName;
  private final String threadName;
  private DistributionSummary idle;
  private DistributionSummary execute;

  public ThriftMetrics(String serverName, String threadName) {
    this.serverName = serverName;
    this.threadName = threadName;
  }

  public void addIdle(long time) {
    idle.record(time);
  }

  public void addExecute(long time) {
    execute.record(time);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    idle = DistributionSummary.builder(METRICS_THRIFT_IDLE).baseUnit("ms")
        .tags("server", serverName, "thread", threadName).register(registry);
    execute = DistributionSummary.builder(METRICS_THRIFT_EXECUTE).baseUnit("ms")
        .tags("server", serverName, "thread", threadName).register(registry);
  }

}

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

import static org.apache.accumulo.core.metrics.Metric.THRIFT_EXECUTE;
import static org.apache.accumulo.core.metrics.Metric.THRIFT_IDLE;

import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;

public class ThriftMetrics implements MetricsProducer {

  private DistributionSummary idle = NoopMetrics.useNoopDistributionSummary();
  private DistributionSummary execute = NoopMetrics.useNoopDistributionSummary();

  public ThriftMetrics() {}

  public void addIdle(long time) {
    idle.record(time);
  }

  public void addExecute(long time) {
    execute.record(time);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    idle = DistributionSummary.builder(THRIFT_IDLE.getName()).baseUnit("ms")
        .description(THRIFT_IDLE.getDescription()).register(registry);
    execute = DistributionSummary.builder(THRIFT_EXECUTE.getName()).baseUnit("ms")
        .description(THRIFT_EXECUTE.getDescription()).register(registry);
  }

}

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

import static org.apache.accumulo.core.metrics.Metric.LOW_MEMORY;
import static org.apache.accumulo.core.metrics.Metric.SERVER_IDLE;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.ServerContext;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class ProcessMetrics implements MetricsProducer {

  private final ServerContext context;
  private final AtomicBoolean isIdle;

  public ProcessMetrics(final ServerContext context) {
    this.context = context;
    this.isIdle = new AtomicBoolean(true);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(LOW_MEMORY.getName(), this, this::lowMemDetected)
        .description(LOW_MEMORY.getDescription()).register(registry);
    Gauge.builder(SERVER_IDLE.getName(), this, this::getIdleAsDouble)
        .description(SERVER_IDLE.getDescription()).register(registry);
  }

  private int lowMemDetected(ProcessMetrics processMetrics) {
    return context.getLowMemoryDetector().isRunningLowOnMemory() ? 1 : 0;
  }

  public void setIdleValue(boolean isIdle) {
    this.isIdle.set(isIdle);
  }

  public double getIdleAsDouble(ProcessMetrics processMetrics) {
    return processMetrics.isIdle.get() ? 1 : 0;
  }

}

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
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.server.ServerContext;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

public class ProcessMetrics implements MetricsProducer {

  private final ServerContext context;
  private Counter idleCounter;

  public ProcessMetrics(final ServerContext context) {
    this.context = context;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    registry.gauge(METRICS_LOW_MEMORY, MetricsUtil.getCommonTags(), this, this::lowMemDetected);
    idleCounter = registry.counter(METRICS_SERVER_IDLE, MetricsUtil.getCommonTags());
  }

  public void incrementIdleCounter() {
    if (idleCounter != null) {
      idleCounter.increment();
    }
  }

  private int lowMemDetected(ProcessMetrics processMetrics) {
    return context.getLowMemoryDetector().isRunningLowOnMemory() ? 1 : 0;
  }
}

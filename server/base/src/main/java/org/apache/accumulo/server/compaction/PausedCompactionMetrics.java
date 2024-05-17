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
package org.apache.accumulo.server.compaction;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;

public class PausedCompactionMetrics implements MetricsProducer {

  private final AtomicLong majcPauseCount = new AtomicLong(0);
  private final AtomicLong mincPauseCount = new AtomicLong(0);

  public void incrementMinCPause() {
    mincPauseCount.incrementAndGet();
  }

  public void incrementMajCPause() {
    majcPauseCount.incrementAndGet();
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    FunctionCounter.builder(METRICS_MAJC_PAUSED, majcPauseCount, AtomicLong::get)
        .description("major compaction pause count").register(registry);
    FunctionCounter.builder(METRICS_MINC_PAUSED, mincPauseCount, AtomicLong::get)
        .description("minor compactor pause count").register(registry);
  }

}

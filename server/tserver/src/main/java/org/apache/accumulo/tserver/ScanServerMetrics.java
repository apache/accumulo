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
package org.apache.accumulo.tserver;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class ScanServerMetrics implements MetricsProducer {

  private Timer reservationTimer;
  private Counter busyCounter;

  @Override
  public void registerMetrics(MeterRegistry registry) {
    reservationTimer = Timer.builder(MetricsProducer.METRICS_SSERVER_REGISTRATION_TIMER)
        .description("Time to reserve a tablets files for scan").tags(MetricsUtil.getCommonTags())
        .register(registry);
    busyCounter = Counter.builder(MetricsProducer.METRICS_SSERVER_BUSY_COUNTER)
        .description("The number of scans where a busy timeout happened")
        .tags(MetricsUtil.getCommonTags()).register(registry);
  }

  public Timer getReservationTimer() {
    return reservationTimer;
  }

  public void incrementBusy() {
    busyCounter.increment();
  }
}

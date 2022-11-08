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
package org.apache.accumulo.tserver.metrics;

import java.time.Duration;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerMinCMetrics implements MetricsProducer {

  private Timer activeMinc;
  private Timer queuedMinc;

  public void addActive(long value) {
    activeMinc.record(Duration.ofMillis(value));
  }

  public void addQueued(long value) {
    queuedMinc.record(Duration.ofMillis(value));
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    activeMinc = Timer.builder(METRICS_MINC_RUNNING).description("Minor compactions time active")
        .tags(MetricsUtil.getCommonTags()).register(registry);

    queuedMinc =
        Timer.builder(METRICS_MINC_QUEUED).description("Queued minor compactions time queued")
            .tags(MetricsUtil.getCommonTags()).register(registry);
  }

}

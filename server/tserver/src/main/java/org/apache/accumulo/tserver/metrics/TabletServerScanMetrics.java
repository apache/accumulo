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
package org.apache.accumulo.tserver.metrics;

import java.time.Duration;

import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerScanMetrics implements MetricsProducer {

  private Timer scans;
  private DistributionSummary resultsPerScan;
  private DistributionSummary yields;

  public void addScan(long value) {
    scans.record(Duration.ofMillis(value));
  }

  public void addResult(long value) {
    resultsPerScan.record(value);
  }

  public void addYield(long value) {
    yields.record(value);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    scans = Timer.builder(METRICS_SCAN).description("Scans").register(registry);
    resultsPerScan = DistributionSummary.builder(METRICS_SCAN_RESULTS)
        .description("Results per scan").register(registry);
    yields =
        DistributionSummary.builder(METRICS_SCAN_YIELDS).description("yields").register(registry);
  }

}

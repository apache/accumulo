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

import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableStat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerScanMetrics extends TServerMetrics {

  private final Timer scans;
  private final Timer resultsPerScan;
  private final Timer yields;

  private final TabletServerScanHadoopMetrics hadoopMetrics;

  public TabletServerScanMetrics(MeterRegistry meterRegistry) {
    super("Scans");

    scans = Timer.builder("scans").description("Scans").register(meterRegistry);
    resultsPerScan =
        Timer.builder("result").description("Results per scan").register(meterRegistry);
    yields = Timer.builder("yields").description("yields").register(meterRegistry);

    hadoopMetrics = new TabletServerScanHadoopMetrics(super.getRegistry());
  }

  public void addScan(long value) {
    scans.record(Duration.ofMillis(value));
    hadoopMetrics.addScan(value);
  }

  public void addResult(long value) {
    resultsPerScan.record(Duration.ofMillis(value));
    hadoopMetrics.addResult(value);
  }

  public void addYield(long value) {
    yields.record(Duration.ofMillis(value));
    hadoopMetrics.addYield(value);
  }

  private static class TabletServerScanHadoopMetrics {

    private final MutableStat scans;
    private final MutableStat resultsPerScan;
    private final MutableStat yields;

    TabletServerScanHadoopMetrics(MetricsRegistry metricsRegistry) {
      scans = metricsRegistry.newStat("scan", "Scans", "Ops", "Count", true);
      resultsPerScan = metricsRegistry.newStat("result", "Results per scan", "Ops", "Count", true);
      yields = metricsRegistry.newStat("yield", "Yields", "Ops", "Count", true);
    }

    public void addScan(long value) {
      scans.add(value);
    }

    public void addResult(long value) {
      resultsPerScan.add(value);
    }

    public void addYield(long value) {
      yields.add(value);
    }
  }
}

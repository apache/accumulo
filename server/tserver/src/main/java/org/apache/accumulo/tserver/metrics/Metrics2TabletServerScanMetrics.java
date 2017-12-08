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
package org.apache.accumulo.tserver.metrics;

import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 *
 */
public class Metrics2TabletServerScanMetrics implements Metrics, MetricsSource, TabletServerScanMetricsKeys {
  public static final String NAME = TSERVER_NAME + ",sub=Scans", DESCRIPTION = "TabletServer Scan Metrics", CONTEXT = "tserver", RECORD = "Scans";

  private final MetricsSystem system;
  private final MetricsRegistry registry;
  private final MutableStat scans, resultsPerScan, yields;

  // Use TabletServerMetricsFactory
  Metrics2TabletServerScanMetrics(MetricsSystem system) {
    this.system = system;
    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    this.registry.tag(MsInfo.ProcessName, MetricsSystemHelper.getProcessName());

    scans = registry.newStat(SCAN, "Scans", "Ops", "Count", true);
    resultsPerScan = registry.newStat(RESULT_SIZE, "Results per scan", "Ops", "Count", true);
    yields = registry.newStat(YIELD, "Yields", "Ops", "Count", true);
  }

  @Override
  public void add(String name, long value) {
    if (SCAN.equals(name)) {
      scans.add(value);
    } else if (RESULT_SIZE.equals(name)) {
      resultsPerScan.add(value);
    } else if (YIELD.equals(name)) {
      yields.add(value);
    } else {
      throw new RuntimeException("Could not find metric to update for name " + name);
    }
  }

  @Override
  public void register() {
    system.register(NAME, DESCRIPTION, this);
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);

    registry.snapshot(builder, all);
  }

}

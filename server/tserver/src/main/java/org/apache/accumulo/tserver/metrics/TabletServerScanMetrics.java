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

import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableStat;

public class TabletServerScanMetrics extends TServerMetrics {

  private final MutableStat scans;
  private final MutableStat resultsPerScan;
  private final MutableStat yields;

  public TabletServerScanMetrics() {
    super("Scans");

    MetricsRegistry registry = super.getRegistry();
    scans = registry.newStat("scan", "Scans", "Ops", "Count", true);
    resultsPerScan = registry.newStat("result", "Results per scan", "Ops", "Count", true);
    yields = registry.newStat("yield", "Yields", "Ops", "Count", true);
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

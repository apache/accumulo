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
package org.apache.accumulo.master.metrics.fate;

import org.apache.accumulo.master.Master;
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

public class Metrics2FateMetrics implements Metrics, MetricsSource {
  public static final String NAME = MASTER_NAME + ",sub=Fate";
  public static final String DESCRIPTION = "Fate Metrics";
  public static final String CONTEXT = "master";
  public static final String RECORD = "fate";
  public static final String CUR_FATE_OPS = "currentFateOps";
  public static final String TOTAL_FATE_OPS = "totalFateOps";
  public static final String TOTAL_ZK_CONN_ERRORS = "totalZkConnErrors";

  private final Master master;
  private final MetricsSystem metricsSystem;
  private final MetricsRegistry registry;
  private final MutableStat currentFateOps;
  private final MutableStat fateOpsTotal;
  private final MutableStat zkConnectionErrorsTotal;

  public Metrics2FateMetrics(Master master, MetricsSystem metricsSystem) {
    this.master = master;
    this.metricsSystem = metricsSystem;
    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    this.registry.tag(MsInfo.ProcessName, MetricsSystemHelper.getProcessName());
    currentFateOps = registry.newStat(CUR_FATE_OPS, "Current number of FATE Ops", "Ops", "Count",
        true);
    fateOpsTotal = registry.newStat(TOTAL_FATE_OPS, "Total FATE Ops", "Ops", "Count", true);
    zkConnectionErrorsTotal = registry.newStat(TOTAL_ZK_CONN_ERRORS, "Total ZK Connection Errors",
        "Ops", "Count", true);
  }

  @Override
  public void register() throws Exception {
    metricsSystem.register(NAME, DESCRIPTION, this);
  }

  @Override
  public void add(String name, long time) {
    throw new UnsupportedOperationException("add() is not implemented");
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);
    // get snapshot here or use add()?
    registry.snapshot(builder, all);
  }
}

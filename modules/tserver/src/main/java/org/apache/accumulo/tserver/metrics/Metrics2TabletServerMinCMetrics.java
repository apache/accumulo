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
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 *
 */
public class Metrics2TabletServerMinCMetrics implements Metrics, MetricsSource, TabletServerMinCMetricsKeys {
  public static final String NAME = TSERVER_NAME + ",sub=MinorCompactions", DESCRIPTION = "TabletServer Minor Compaction Metrics", CONTEXT = "tserver",
      RECORD = "MinorCompactions";

  private final MetricsSystem system;
  private final MetricsRegistry registry;
  private final MutableStat activeMinc, queuedMinc;

  // Use TabletServerMetricsFactory
  Metrics2TabletServerMinCMetrics(MetricsSystem system) {
    this.system = system;
    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));

    activeMinc = registry.newStat(MINC, "Minor compactions", "Ops", "Count", true);
    queuedMinc = registry.newStat(QUEUE, "Queued minor compactions", "Ops", "Count", true);
  }

  @Override
  public void add(String name, long value) {
    if (MINC.equals(name)) {
      activeMinc.add(value);
    } else if (QUEUE.equals(name)) {
      queuedMinc.add(value);
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

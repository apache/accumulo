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
package org.apache.accumulo.gc.metrics2;

import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

/**
 * Expected to be instantiated with GcMetricsFactory. This will configure both jmx and the hadoop
 * metrics2 systems.
 */
public class GcMetrics extends Metrics {

  // use common prefix, different that just gc, to prevent confusion with jvm gc metrics.
  public static final String GC_METRIC_PREFIX = "AccGc";

  private static final String jmxName = "GarbageCollector";
  private static final String description = "Accumulo garbage collection metrics";
  private static final String record = "AccGcCycleMetrics";

  private final SimpleGarbageCollector gc;

  private final GcHadoopMetrics2 gcMetrics;

  GcMetrics(final SimpleGarbageCollector gc) {
    super(jmxName + ",sub=" + gc.getClass().getSimpleName(), description, "accgc", record);
    this.gc = gc;

    MetricsRegistry registry = super.getRegistry();
    gcMetrics = new GcHadoopMetrics2(registry);

  }

  @Override
  protected void prepareMetrics() {

    GcCycleMetrics values = gc.getGcCycleMetrics();
    gcMetrics.prepare(values);

  }
}

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
package org.apache.accumulo.gc.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Expected to be instantiated with GcMetricsFactory. This will configure both jmx and the hadoop
 * metrics systems. The naming convention, in hadoop metrics2, the records will appear as
 * CONTEXT.RECORD (accgc.AccGcCycleMetrics). The value for context is also used by the configuration
 * file for sink configuration.
 */
public class GcMetrics implements Metrics, MetricsSource {

  // use common prefix, different than just gc, to prevent confusion with jvm gc metrics.
  public static final String GC_METRIC_PREFIX = "AccGc";

  public static final String CONTEXT = "accgc";
  private static final String jmxName = "GarbageCollector";
  // private final String NAME = "AccGC" + ",sub=AccGcRunStats";
  private final String DESCRIPTION = "Accumulo garbage collection metrics";
  private final String RECORD = "AccGcCycleMetrics";

  private final SimpleGarbageCollector gc;

  private final MetricsSystem metricsSystem;
  private final MetricsRegistry registry;

  // metrics gauges / counters.
  private final MutableGaugeLong gcStarted;
  private final MutableGaugeLong gcFinished;
  private final MutableGaugeLong gcCandidates;
  private final MutableGaugeLong gcInUse;
  private final MutableGaugeLong gcDeleted;
  private final MutableGaugeLong gcErrors;

  private final MutableGaugeLong walStarted;
  private final MutableGaugeLong walFinished;
  private final MutableGaugeLong walCandidates;
  private final MutableGaugeLong walInUse;
  private final MutableGaugeLong walDeleted;
  private final MutableGaugeLong walErrors;

  private final MutableGaugeLong postOpDuration;
  private final MutableGaugeLong runCycleCount;

  private final boolean metrics2enabled;

  public GcMetrics(final SimpleGarbageCollector gc, final boolean metrics2enabled) {

    this.gc = gc;
    this.metrics2enabled = metrics2enabled;

    // NAME = jmxName + ",sub=" + gc.getClass().getSimpleName();
    metricsSystem = MetricsSystemHelper.getInstance();
    this.registry = new MetricsRegistry(
        Interns.info(jmxName + ",sub=" + gc.getClass().getSimpleName(), DESCRIPTION));

    this.registry.tag(MsInfo.ProcessName, MetricsSystemHelper.getProcessName());

    gcStarted = registry.newGauge(GC_METRIC_PREFIX + "Started",
        "Timestamp GC file collection cycle started", 0L);
    gcFinished = registry.newGauge(GC_METRIC_PREFIX + "Finished",
        "Timestamp GC file collect cycle finished", 0L);
    gcCandidates = registry.newGauge(GC_METRIC_PREFIX + "Candidates",
        "Number of files that are candidates for deletion", 0L);
    gcInUse =
        registry.newGauge(GC_METRIC_PREFIX + "InUse", "Number of candidate files still in use", 0L);
    gcDeleted =
        registry.newGauge(GC_METRIC_PREFIX + "Deleted", "Number of candidate files deleted", 0L);
    gcErrors =
        registry.newGauge(GC_METRIC_PREFIX + "Errors", "Number of candidate deletion errors", 0L);

    walStarted = registry.newGauge(GC_METRIC_PREFIX + "WalStarted",
        "Timestamp GC WAL collection started", 0L);
    walFinished = registry.newGauge(GC_METRIC_PREFIX + "WalFinished",
        "Timestamp GC WAL collection finished", 0L);
    walCandidates = registry.newGauge(GC_METRIC_PREFIX + "WalCandidates",
        "Number of files that are candidates for deletion", 0L);
    walInUse = registry.newGauge(GC_METRIC_PREFIX + "WalInUse",
        "Number of wal file candidates that are still in use", 0L);
    walDeleted = registry.newGauge(GC_METRIC_PREFIX + "WalDeleted",
        "Number of candidate wal files deleted", 0L);
    walErrors = registry.newGauge(GC_METRIC_PREFIX + "WalErrors",
        "Number candidate wal file deletion errors", 0L);

    postOpDuration = registry.newGauge(GC_METRIC_PREFIX + "PostOpDuration",
        "GC metadata table post operation duration in milliseconds", 0L);

    runCycleCount = registry.newGauge(GC_METRIC_PREFIX + "RunCycleCount",
        "gauge incremented each gc cycle run, rest on process start", 0L);

  }

  @Override
  public void register() throws Exception {
    if (metrics2enabled) {
      metricsSystem.register(jmxName + ",sub=" + gc.getClass().getSimpleName(), DESCRIPTION, this);
    }
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

    GcCycleMetrics values = gc.getGcCycleMetrics();

    GcCycleStats lastFileCollect = values.getLastCollect();

    gcStarted.set(lastFileCollect.getStarted());
    gcFinished.set(lastFileCollect.getFinished());
    gcCandidates.set(lastFileCollect.getCandidates());
    gcInUse.set(lastFileCollect.getInUse());
    gcDeleted.set(lastFileCollect.getDeleted());
    gcErrors.set(lastFileCollect.getErrors());

    GcCycleStats lastWalCollect = values.getLastWalCollect();

    walStarted.set(lastWalCollect.getStarted());
    walFinished.set(lastWalCollect.getFinished());
    walCandidates.set(lastWalCollect.getCandidates());
    walInUse.set(lastWalCollect.getInUse());
    walDeleted.set(lastWalCollect.getDeleted());
    walErrors.set(lastWalCollect.getErrors());

    postOpDuration.set(TimeUnit.NANOSECONDS.toMillis(values.getPostOpDurationNanos()));
    runCycleCount.set(values.getRunCycleCount());

    // create the metrics record and publish to the registry.
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);
    registry.snapshot(builder, all);

  }
}

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
package org.apache.accumulo.gc.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Expected to be instantiated with GcMetricsFactory. This will configure both jmx and the hadoop
 * metrics systems. The naming convention, in hadoop metrics2, the records will appear as
 * CONTEXT.RECORD (accgc.AccGcCycleMetrics). The value for context is also used by the configuration
 * file for sink configuration.
 */
public class GcMetrics extends Metrics {

  private final GcHadoopMetrics hadoopMetrics;
  private AtomicReference<GcCycleMetrics> metricValues;

  // use common prefix, different that just gc, to prevent confusion with jvm gc metrics.
  public static final String GC_METRIC_PREFIX = "AccGc";

  private static final String jmxName = "GarbageCollector";
  private static final String description = "Accumulo garbage collection metrics";
  private static final String record = "AccGcCycleMetrics";

  private final SimpleGarbageCollector gc;

  public GcMetrics(SimpleGarbageCollector gc) {

    super(jmxName + ",sub=" + gc.getClass().getSimpleName(), description, "accgc", record);
    this.gc = gc;

    MetricsSystem metricsSystem = gc.getMetricsSystem();

    register(metricsSystem);

    // Updated during each cycle of SimpleGC
    metricValues = new AtomicReference<>(this.gc.getGcCycleMetrics());

    MeterRegistry registry = gc.getMicrometerMetrics().getRegistry();

    Gauge gcStarted = Gauge
        .builder(GC_METRIC_PREFIX + ".started", metricValues,
            v -> v.get().getLastCollect().getStarted())
        .description("Timestamp GC file collection cycle started").register(registry);
    Gauge gcFinished = Gauge
        .builder(GC_METRIC_PREFIX + ".finished", metricValues,
            v -> v.get().getLastCollect().getFinished())
        .description("Timestamp GC file collect cycle finished").register(registry);
    Gauge gcCandidates = Gauge
        .builder(GC_METRIC_PREFIX + ".candidates", metricValues,
            v -> v.get().getLastCollect().getCandidates())
        .description("Number of files that are candidates for deletion").register(registry);
    Gauge gcInUse = Gauge
        .builder(GC_METRIC_PREFIX + ".in.use", metricValues,
            v -> v.get().getLastCollect().getInUse())
        .description("Number of candidate files still in use").register(registry);
    Gauge gcDeleted = Gauge
        .builder(GC_METRIC_PREFIX + ".deleted", metricValues,
            v -> v.get().getLastCollect().getDeleted())
        .description("Number of candidate files deleted").register(registry);
    Gauge gcErrors = Gauge
        .builder(GC_METRIC_PREFIX + ".errors", metricValues,
            v -> v.get().getLastCollect().getErrors())
        .description("Number of candidate deletion errors").register(registry);

    Gauge walStarted = Gauge
        .builder(GC_METRIC_PREFIX + ".wal.started", metricValues,
            v -> v.get().getLastWalCollect().getStarted())
        .description("Timestamp GC WAL collection cycle started").register(registry);
    Gauge walFinished = Gauge
        .builder(GC_METRIC_PREFIX + ".wal.finished", metricValues,
            v -> v.get().getLastWalCollect().getFinished())
        .description("Timestamp GC WAL collect cycle finished").register(registry);
    Gauge walCandidates = Gauge
        .builder(GC_METRIC_PREFIX + ".wal.candidates", metricValues,
            v -> v.get().getLastWalCollect().getCandidates())
        .description("Number of files that are candidates for deletion").register(registry);
    Gauge walInUse = Gauge
        .builder(GC_METRIC_PREFIX + ".wal.in.use", metricValues,
            v -> v.get().getLastWalCollect().getInUse())
        .description("Number of wal file candidates that are still in use").register(registry);
    Gauge walDeleted = Gauge
        .builder(GC_METRIC_PREFIX + ".wal.deleted", metricValues,
            v -> v.get().getLastWalCollect().getDeleted())
        .description("Number of candidate wal files deleted").register(registry);
    Gauge walErrors = Gauge
        .builder(GC_METRIC_PREFIX + ".wal.errors", metricValues,
            v -> v.get().getLastWalCollect().getErrors())
        .description("Number candidate wal file deletion errors").register(registry);

    Gauge postOpDuration = Gauge
        .builder(GC_METRIC_PREFIX + ".post.op.duration", metricValues,
            v -> TimeUnit.NANOSECONDS.toMillis(v.get().getPostOpDurationNanos()))
        .description("GC metadata table post operation duration in milliseconds")
        .register(registry);
    Gauge runCycleCount = Gauge
        .builder(GC_METRIC_PREFIX + ".run.cycle.count", metricValues.get(),
            GcCycleMetrics::getRunCycleCount)
        .description("gauge incremented each gc cycle run, rest on process start")
        .register(registry);

    hadoopMetrics = new GcHadoopMetrics(super.getRegistry());
  }

  @Override
  public void prepareMetrics() {
    hadoopMetrics.prepareMetrics(metricValues.get());
  }

  public void updateMetrics(GcCycleMetrics cycleMetrics) {
    metricValues.set(cycleMetrics);
  }

  private static class GcHadoopMetrics {

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

    GcHadoopMetrics(MetricsRegistry registry) {

      gcStarted = registry.newGauge(GC_METRIC_PREFIX + "Started",
          "Timestamp GC file collection cycle started", 0L);
      gcFinished = registry.newGauge(GC_METRIC_PREFIX + "Finished",
          "Timestamp GC file collect cycle finished", 0L);
      gcCandidates = registry.newGauge(GC_METRIC_PREFIX + "Candidates",
          "Number of files that are candidates for deletion", 0L);
      gcInUse = registry.newGauge(GC_METRIC_PREFIX + "InUse",
          "Number of candidate files still in use", 0L);
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

    /**
     * Update the metrics gauges from the measured values
     */
    public void prepareMetrics(GcCycleMetrics values) {

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
    }
  }
}

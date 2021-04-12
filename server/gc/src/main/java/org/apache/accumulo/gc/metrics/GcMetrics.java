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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.server.metrics.Metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;

/**
 * Expected to be instantiated with GcMetricsFactory. This will configure both jmx and the hadoop
 * metrics systems. The naming convention, in hadoop metrics2, the records will appear as
 * CONTEXT.RECORD (accgc.AccGcCycleMetrics). The value for context is also used by the configuration
 * file for sink configuration.
 */
public class GcMetrics extends Metrics {

  // use common prefix, different that just gc, to prevent confusion with jvm gc metrics.
  public static final String GC_METRIC_PREFIX = "acc.gc.";

  private static final String jmxName = "GarbageCollector";
  private static final String description = "Accumulo garbage collection metrics";
  private static final String record = "AccGcCycleMetrics";

  private final SimpleGarbageCollector gc;

  // metrics gauges / counters.
  /*
   * private final MutableGaugeLong gcStarted; private final MutableGaugeLong gcFinished; private
   * final MutableGaugeLong gcCandidates; private final MutableGaugeLong gcInUse; private final
   * MutableGaugeLong gcDeleted; private final MutableGaugeLong gcErrors; private final
   * MutableGaugeLong walStarted; private final MutableGaugeLong walFinished; private final
   * MutableGaugeLong walCandidates; private final MutableGaugeLong walInUse; private final
   * MutableGaugeLong walDeleted; private final MutableGaugeLong walErrors; private final
   * MutableGaugeLong postOpDuration; private final MutableGaugeLong runCycleCount;
   */
  private GcCycleMetrics values;

  GcMetrics(final SimpleGarbageCollector gc) {
    super(jmxName + ",sub=" + gc.getClass().getSimpleName(), description, "accgc", record);
    this.gc = gc;

    prepareMetrics(); // initialize values

    // config which returns the duration between logging the metrics
    LoggingRegistryConfig config = c -> {
      if (c.equals("logging.step")) {
        return "5s";
      }
      return null;
    };

    List<String> list = new ArrayList<>(); // to hold metrics values

    // writing the metrics to the same file that hadoop metrics writes to
    // proof of concept. For production, a smarter way would be used to do this
    Consumer<String> consumer = v -> {
      // report the meters' value to a list
      list.add(v.replace("{} value", ""));
      // once all meters have reported, write the line to file
      // there is probably a better way to do this
      if (list.size() >= 14) {
        try (FileWriter writer = new FileWriter("./target/accgc.metrics", UTF_8, true);
            PrintWriter printWriter = new PrintWriter(writer)) {
          printWriter.println(
              System.currentTimeMillis() + ", " + list.toString().replaceAll("[\\[|\\]]", ""));
          printWriter.flush();
        } catch (IOException e) {
          e.printStackTrace();
        }
        list.clear();
      }
    };

    LoggingMeterRegistry registry =
        LoggingMeterRegistry.builder(config).loggingSink(consumer).build();
    // need to use this prefix since accumulo metrics package is used in this class
    io.micrometer.core.instrument.Metrics.globalRegistry.add(registry);

    Gauge gcStarted =
        Gauge.builder(GC_METRIC_PREFIX + "started", values, v -> v.getLastCollect().getStarted())
            .description("Timestamp GC file collection cycle started").register(registry);
    Gauge gcFinished =
        Gauge.builder(GC_METRIC_PREFIX + "finished", values, v -> v.getLastCollect().getFinished())
            .description("Timestamp GC file collect cycle finished").register(registry);
    Gauge gcCandidates = Gauge
        .builder(GC_METRIC_PREFIX + "candidates", values, v -> v.getLastCollect().getCandidates())
        .description("Number of files that are candidates for deletion").register(registry);
    Gauge gcInUse =
        Gauge.builder(GC_METRIC_PREFIX + "in.use", values, v -> v.getLastCollect().getInUse())
            .description("Number of candidate files still in use").register(registry);
    Gauge gcDeleted =
        Gauge.builder(GC_METRIC_PREFIX + "deleted", values, v -> v.getLastCollect().getDeleted())
            .description("Number of candidate files deleted").register(registry);
    Gauge gcErrors =
        Gauge.builder(GC_METRIC_PREFIX + "errors", values, v -> v.getLastCollect().getErrors())
            .description("Number of candidate deletion errors").register(registry);

    Gauge walStarted = Gauge
        .builder(GC_METRIC_PREFIX + "wal.started", values, v -> v.getLastWalCollect().getStarted())
        .description("Timestamp GC WAL collection cycle started").register(registry);
    Gauge walFinished = Gauge
        .builder(GC_METRIC_PREFIX + "wal.finished", values,
            v -> v.getLastWalCollect().getFinished())
        .description("Timestamp GC WAL collect cycle finished").register(registry);
    Gauge walCandidates = Gauge
        .builder(GC_METRIC_PREFIX + "wal.candidates", values,
            v -> v.getLastWalCollect().getCandidates())
        .description("Number of files that are candidates for deletion").register(registry);
    Gauge walInUse = Gauge
        .builder(GC_METRIC_PREFIX + "wal.in.use", values, v -> v.getLastWalCollect().getInUse())
        .description("Number of wal file candidates that are still in use").register(registry);
    Gauge walDeleted = Gauge
        .builder(GC_METRIC_PREFIX + "wal.deleted", values, v -> v.getLastWalCollect().getDeleted())
        .description("Number of candidate wal files deleted").register(registry);
    Gauge walErrors = Gauge
        .builder(GC_METRIC_PREFIX + "wal.errors", values, v -> v.getLastWalCollect().getErrors())
        .description("Number candidate wal file deletion errors").register(registry);

    Gauge postOpDuration = Gauge
        .builder(GC_METRIC_PREFIX + "post.op.duration", values,
            v -> TimeUnit.NANOSECONDS.toMillis(v.getPostOpDurationNanos()))
        .description("GC metadata table post operation duration in milliseconds")
        .register(registry);
    Gauge runCycleCount = Gauge
        .builder(GC_METRIC_PREFIX + "run.cycle.count", values, GcCycleMetrics::getRunCycleCount)
        .description("gauge incremented each gc cycle run, rest on process start")
        .register(registry);

    /*
     * gcStarted = registry.newGauge(GC_METRIC_PREFIX + "Started",
     * "Timestamp GC file collection cycle started", 0L); gcFinished =
     * registry.newGauge(GC_METRIC_PREFIX + "Finished", "Timestamp GC file collect cycle finished",
     * 0L); gcCandidates = registry.newGauge(GC_METRIC_PREFIX + "Candidates",
     * "Number of files that are candidates for deletion", 0L); gcInUse =
     * registry.newGauge(GC_METRIC_PREFIX + "InUse", "Number of candidate files still in use", 0L);
     * gcDeleted = registry.newGauge(GC_METRIC_PREFIX + "Deleted",
     * "Number of candidate files deleted", 0L); gcErrors = registry.newGauge(GC_METRIC_PREFIX +
     * "Errors", "Number of candidate deletion errors", 0L); walStarted =
     * registry.newGauge(GC_METRIC_PREFIX + "WalStarted", "Timestamp GC WAL collection started",
     * 0L); walFinished = registry.newGauge(GC_METRIC_PREFIX + "WalFinished",
     * "Timestamp GC WAL collection finished", 0L); walCandidates =
     * registry.newGauge(GC_METRIC_PREFIX + "WalCandidates",
     * "Number of files that are candidates for deletion", 0L); walInUse =
     * registry.newGauge(GC_METRIC_PREFIX + "WalInUse",
     * "Number of wal file candidates that are still in use", 0L); walDeleted =
     * registry.newGauge(GC_METRIC_PREFIX + "WalDeleted", "Number of candidate wal files deleted",
     * 0L); walErrors = registry.newGauge(GC_METRIC_PREFIX + "WalErrors",
     * "Number candidate wal file deletion errors", 0L); postOpDuration =
     * registry.newGauge(GC_METRIC_PREFIX + "PostOpDuration",
     * "GC metadata table post operation duration in milliseconds", 0L); runCycleCount =
     * registry.newGauge(GC_METRIC_PREFIX + "RunCycleCount",
     * "gauge incremented each gc cycle run, rest on process start", 0L);
     */

  }

  @Override
  protected void prepareMetrics() {
    // only this needs to be updated since the meters recompute value from this variable when it is
    // requested
    values = gc.getGcCycleMetrics();
    /*
     * lastFileCollect = values.getLastCollect(); lastWalCollect = values.getLastWalCollect();
     * gcStarted.set(lastFileCollect.getStarted()); gcFinished.set(lastFileCollect.getFinished());
     * gcCandidates.set(lastFileCollect.getCandidates()); gcInUse.set(lastFileCollect.getInUse());
     * gcDeleted.set(lastFileCollect.getDeleted()); gcErrors.set(lastFileCollect.getErrors());
     * walStarted.set(lastWalCollect.getStarted()); walFinished.set(lastWalCollect.getFinished());
     * walCandidates.set(lastWalCollect.getCandidates()); walInUse.set(lastWalCollect.getInUse());
     * walDeleted.set(lastWalCollect.getDeleted()); walErrors.set(lastWalCollect.getErrors());
     * postOpDuration.set(TimeUnit.NANOSECONDS.toMillis(values.getPostOpDurationNanos()));
     * runCycleCount.set(values.getRunCycleCount());
     */
  }
}

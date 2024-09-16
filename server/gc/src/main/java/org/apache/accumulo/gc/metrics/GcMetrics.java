/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.gc.metrics;

import static org.apache.accumulo.core.metrics.Metric.GC_CANDIDATES;
import static org.apache.accumulo.core.metrics.Metric.GC_DELETED;
import static org.apache.accumulo.core.metrics.Metric.GC_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.GC_FINISHED;
import static org.apache.accumulo.core.metrics.Metric.GC_IN_USE;
import static org.apache.accumulo.core.metrics.Metric.GC_POST_OP_DURATION;
import static org.apache.accumulo.core.metrics.Metric.GC_RUN_CYCLE;
import static org.apache.accumulo.core.metrics.Metric.GC_STARTED;
import static org.apache.accumulo.core.metrics.Metric.GC_WAL_CANDIDATES;
import static org.apache.accumulo.core.metrics.Metric.GC_WAL_DELETED;
import static org.apache.accumulo.core.metrics.Metric.GC_WAL_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.GC_WAL_FINISHED;
import static org.apache.accumulo.core.metrics.Metric.GC_WAL_IN_USE;
import static org.apache.accumulo.core.metrics.Metric.GC_WAL_STARTED;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.gc.SimpleGarbageCollector;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class GcMetrics implements MetricsProducer {

  private final GcCycleMetrics metricValues;

  public GcMetrics(SimpleGarbageCollector gc) {
    // Updated during each cycle of SimpleGC
    metricValues = gc.getGcCycleMetrics();
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(GC_STARTED.getName(), metricValues, v -> v.getLastCollect().getStarted())
        .description("Timestamp GC file collection cycle started").register(registry);
    Gauge.builder(GC_FINISHED.getName(), metricValues, v -> v.getLastCollect().getFinished())
        .description("Timestamp GC file collect cycle finished").register(registry);
    Gauge.builder(GC_CANDIDATES.getName(), metricValues, v -> v.getLastCollect().getCandidates())
        .description("Number of files that are candidates for deletion").register(registry);
    Gauge.builder(GC_IN_USE.getName(), metricValues, v -> v.getLastCollect().getInUse())
        .description("Number of candidate files still in use").register(registry);
    Gauge.builder(GC_DELETED.getName(), metricValues, v -> v.getLastCollect().getDeleted())
        .description("Number of candidate files deleted").register(registry);
    Gauge.builder(GC_ERRORS.getName(), metricValues, v -> v.getLastCollect().getErrors())
        .description("Number of candidate deletion errors").register(registry);

    // WAL metrics Gauges
    Gauge.builder(GC_WAL_STARTED.getName(), metricValues, v -> v.getLastWalCollect().getStarted())
        .description("Timestamp GC WAL collection cycle started").register(registry);
    Gauge.builder(GC_WAL_FINISHED.getName(), metricValues, v -> v.getLastWalCollect().getFinished())
        .description("Timestamp GC WAL collect cycle finished").register(registry);
    Gauge
        .builder(GC_WAL_CANDIDATES.getName(), metricValues,
            v -> v.getLastWalCollect().getCandidates())
        .description("Number of files that are candidates for deletion").register(registry);
    Gauge.builder(GC_WAL_IN_USE.getName(), metricValues, v -> v.getLastWalCollect().getInUse())
        .description("Number of wal file candidates that are still in use").register(registry);
    Gauge.builder(GC_WAL_DELETED.getName(), metricValues, v -> v.getLastWalCollect().getDeleted())
        .description("Number of candidate wal files deleted").register(registry);
    Gauge.builder(GC_WAL_ERRORS.getName(), metricValues, v -> v.getLastWalCollect().getErrors())
        .description("Number candidate wal file deletion errors").register(registry);
    Gauge
        .builder(GC_POST_OP_DURATION.getName(), metricValues,
            v -> TimeUnit.NANOSECONDS.toMillis(v.getPostOpDurationNanos()))
        .description("GC metadata table post operation duration in milliseconds")
        .register(registry);
    Gauge.builder(GC_RUN_CYCLE.getName(), metricValues, GcCycleMetrics::getRunCycleCount)
        .description("gauge incremented each gc cycle run, rest on process start")
        .register(registry);

  }

}

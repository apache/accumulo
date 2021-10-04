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

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MicrometerMetricsFactory;
import org.apache.accumulo.gc.SimpleGarbageCollector;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class GcMetrics implements MetricsProducer {

  public GcMetrics(SimpleGarbageCollector gc) {

    // Updated during each cycle of SimpleGC
    final GcCycleMetrics metricValues = gc.getGcCycleMetrics();

    MeterRegistry registry = MicrometerMetricsFactory.getRegistry();

    Gauge
        .builder(getMetricsPrefix() + "started", metricValues, v -> v.getLastCollect().getStarted())
        .description("Timestamp GC file collection cycle started").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "finished", metricValues,
            v -> v.getLastCollect().getFinished())
        .description("Timestamp GC file collect cycle finished").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "candidates", metricValues,
            v -> v.getLastCollect().getCandidates())
        .description("Number of files that are candidates for deletion").register(registry);
    Gauge.builder(getMetricsPrefix() + "in.use", metricValues, v -> v.getLastCollect().getInUse())
        .description("Number of candidate files still in use").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "deleted", metricValues, v -> v.getLastCollect().getDeleted())
        .description("Number of candidate files deleted").register(registry);
    Gauge.builder(getMetricsPrefix() + "errors", metricValues, v -> v.getLastCollect().getErrors())
        .description("Number of candidate deletion errors").register(registry);

    // WAL metrics Gauges
    Gauge
        .builder(getMetricsPrefix() + "wal.started", metricValues,
            v -> v.getLastWalCollect().getStarted())
        .description("Timestamp GC WAL collection cycle started").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "wal.finished", metricValues,
            v -> v.getLastWalCollect().getFinished())
        .description("Timestamp GC WAL collect cycle finished").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "wal.candidates", metricValues,
            v -> v.getLastWalCollect().getCandidates())
        .description("Number of files that are candidates for deletion").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "wal.in.use", metricValues,
            v -> v.getLastWalCollect().getInUse())
        .description("Number of wal file candidates that are still in use").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "wal.deleted", metricValues,
            v -> v.getLastWalCollect().getDeleted())
        .description("Number of candidate wal files deleted").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "wal.errors", metricValues,
            v -> v.getLastWalCollect().getErrors())
        .description("Number candidate wal file deletion errors").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "post.op.duration", metricValues,
            v -> TimeUnit.NANOSECONDS.toMillis(v.getPostOpDurationNanos()))
        .description("GC metadata table post operation duration in milliseconds")
        .register(registry);
    Gauge
        .builder(getMetricsPrefix() + "run.cycle.count", metricValues,
            GcCycleMetrics::getRunCycleCount)
        .description("gauge incremented each gc cycle run, rest on process start")
        .register(registry);

  }

  @Override
  public String getMetricsPrefix() {
    return "accumulo.gc.";
  }

}

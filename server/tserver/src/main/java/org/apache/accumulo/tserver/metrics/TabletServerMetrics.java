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
package org.apache.accumulo.tserver.metrics;

import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_ENTRIES_READ;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_ENTRIES_WRITTEN;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_HOLD;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_INGEST_BYTES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_INGEST_MUTATIONS;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MAJC_QUEUED;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MAJC_RUNNING;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MAJC_STUCK;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MEM_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MINC_QUEUED;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MINC_RUNNING;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MINC_TOTAL;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_ASSIGNMENTS_WARNING;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_FILES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_ONLINE;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_OPENING;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_UNOPENED;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.compaction.CompactionWatcher;
import org.apache.accumulo.server.compaction.FileCompactor;
import org.apache.accumulo.tserver.TabletServer;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;

public class TabletServerMetrics implements MetricsProducer {

  private final TabletServerMetricsUtil util;

  public TabletServerMetrics(TabletServer tserver) {
    util = new TabletServerMetricsUtil(tserver);
  }

  private long getTotalEntriesRead() {
    return FileCompactor.getTotalEntriesRead();
  }

  private long getTotalEntriesWritten() {
    return FileCompactor.getTotalEntriesWritten();
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    FunctionCounter
        .builder(COMPACTOR_ENTRIES_READ.getName(), this, TabletServerMetrics::getTotalEntriesRead)
        .description("Number of entries read by all compactions that have run on this tserver")
        .register(registry);
    FunctionCounter
        .builder(COMPACTOR_ENTRIES_WRITTEN.getName(), this,
            TabletServerMetrics::getTotalEntriesWritten)
        .description("Number of entries written by all compactions that have run on this tserver")
        .register(registry);
    LongTaskTimer timer = LongTaskTimer.builder(TSERVER_MAJC_STUCK.getName())
        .description("Number and duration of stuck major compactions").register(registry);
    CompactionWatcher.setTimer(timer);
    Gauge
        .builder(TSERVER_TABLETS_ASSIGNMENTS_WARNING.getName(), util,
            TabletServerMetricsUtil::getLongTabletAssignments)
        .description("Number of tablet assignments that are taking a long time").register(registry);

    Gauge.builder(TSERVER_ENTRIES.getName(), util, TabletServerMetricsUtil::getEntries)
        .description("Number of entries").register(registry);
    Gauge.builder(TSERVER_MEM_ENTRIES.getName(), util, TabletServerMetricsUtil::getEntriesInMemory)
        .description("Number of entries in memory").register(registry);
    Gauge
        .builder(TSERVER_MAJC_RUNNING.getName(), util, TabletServerMetricsUtil::getMajorCompactions)
        .description("Number of active major compactions").register(registry);
    Gauge
        .builder(TSERVER_MAJC_QUEUED.getName(), util,
            TabletServerMetricsUtil::getMajorCompactionsQueued)
        .description("Number of queued major compactions").register(registry);
    Gauge
        .builder(TSERVER_MINC_RUNNING.getName(), util, TabletServerMetricsUtil::getMinorCompactions)
        .description("Number of active minor compactions").register(registry);
    Gauge
        .builder(TSERVER_MINC_QUEUED.getName(), util,
            TabletServerMetricsUtil::getMinorCompactionsQueued)
        .description("Number of queued minor compactions").register(registry);
    Gauge.builder(TSERVER_TABLETS_ONLINE.getName(), util, TabletServerMetricsUtil::getOnlineCount)
        .description("Number of online tablets").register(registry);
    Gauge.builder(TSERVER_TABLETS_OPENING.getName(), util, TabletServerMetricsUtil::getOpeningCount)
        .description("Number of opening tablets").register(registry);
    Gauge
        .builder(TSERVER_TABLETS_UNOPENED.getName(), util,
            TabletServerMetricsUtil::getUnopenedCount)
        .description("Number of unopened tablets").register(registry);
    Gauge
        .builder(TSERVER_MINC_TOTAL.getName(), util,
            TabletServerMetricsUtil::getTotalMinorCompactions)
        .description("Total number of minor compactions performed").register(registry);

    Gauge
        .builder(TSERVER_TABLETS_FILES.getName(), util,
            TabletServerMetricsUtil::getAverageFilesPerTablet)
        .description("Number of files per tablet").register(registry);
    Gauge.builder(TSERVER_HOLD.getName(), util, TabletServerMetricsUtil::getHoldTime)
        .description("Time commits held").register(registry);
    Gauge.builder(TSERVER_INGEST_MUTATIONS.getName(), util, TabletServerMetricsUtil::getIngestCount)
        .description("Ingest rate (entries/sec)").register(registry);
    Gauge.builder(TSERVER_INGEST_BYTES.getName(), util, TabletServerMetricsUtil::getIngestByteCount)
        .description("Ingest rate (bytes/sec)").register(registry);
  }
}

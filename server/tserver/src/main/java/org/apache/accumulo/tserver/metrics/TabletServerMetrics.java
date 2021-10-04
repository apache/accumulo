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

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MicrometerMetricsFactory;
import org.apache.accumulo.tserver.TabletServer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class TabletServerMetrics implements MetricsProducer {

  public TabletServerMetrics(TabletServer tserver) {

    final TabletServerMetricsUtil util = new TabletServerMetricsUtil(tserver);

    MeterRegistry registry = MicrometerMetricsFactory.getRegistry();

    Gauge.builder(getMetricsPrefix() + "entries", util, TabletServerMetricsUtil::getEntries)
        .description("Number of entries").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "entries.mem", util,
            TabletServerMetricsUtil::getEntriesInMemory)
        .description("Number of entries in memory").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "majc.running", util,
            TabletServerMetricsUtil::getMajorCompactions)
        .description("Number of active major compactions").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "majc.queued", util,
            TabletServerMetricsUtil::getMajorCompactionsQueued)
        .description("Number of queued major compactions").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "minc.running", util,
            TabletServerMetricsUtil::getMinorCompactions)
        .description("Number of active minor compactions").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "minc.queued", util,
            TabletServerMetricsUtil::getMinorCompactionsQueued)
        .description("Number of queued minor compactions").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "tablets.online", util,
            TabletServerMetricsUtil::getOnlineCount)
        .description("Number of online tablets").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "tablets.opening", util,
            TabletServerMetricsUtil::getOpeningCount)
        .description("Number of opening tablets").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "tablets.unopened", util,
            TabletServerMetricsUtil::getUnopenedCount)
        .description("Number of unopened tablets").register(registry);
    Gauge.builder(getMetricsPrefix() + "queries", util, TabletServerMetricsUtil::getQueries)
        .description("Number of queries").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "minc.total", util,
            TabletServerMetricsUtil::getTotalMinorCompactions)
        .description("Total number of minor compactions performed").register(registry);

    Gauge
        .builder(getMetricsPrefix() + "tablets.files", util,
            TabletServerMetricsUtil::getAverageFilesPerTablet)
        .description("Number of files per tablet").register(registry);
    Gauge.builder(getMetricsPrefix() + "hold", util, TabletServerMetricsUtil::getHoldTime)
        .description("Time commits held").register(registry);
    Gauge.builder(getMetricsPrefix() + "ingest.mutations", util, TabletServerMetricsUtil::getIngest)
        .description("Ingest rate (entries/sec)").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "ingest.bytes", util,
            TabletServerMetricsUtil::getIngestByteRate)
        .description("Ingest rate (bytes/sec)").register(registry);
    Gauge.builder(getMetricsPrefix() + "scan.results", util, TabletServerMetricsUtil::getQueries)
        .description("Query rate (entries/sec)").register(registry);
    Gauge
        .builder(getMetricsPrefix() + "scan.results.bytes", util,
            TabletServerMetricsUtil::getQueryByteRate)
        .description("Query rate (bytes/sec)").register(registry);
    Gauge.builder(getMetricsPrefix() + "scan.scanned.entries", util,
        TabletServerMetricsUtil::getScannedRate).description("Scanned rate").register(registry);

  }

  @Override
  public String getMetricsPrefix() {
    return "accumulo.tserver.";
  }

}

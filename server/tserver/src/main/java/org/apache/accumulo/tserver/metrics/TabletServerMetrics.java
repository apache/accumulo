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

import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class TabletServerMetrics extends TServerMetrics {

  private final TabletServerMetricsUtil util;
  private final TabletServerHadoopMetrics hadoopMetrics;

  public TabletServerMetrics(TabletServer tserver) {
    super("general");

    util = new TabletServerMetricsUtil(tserver);

    MeterRegistry registry = tserver.getMicrometerMetrics().getRegistry();

    Gauge.builder("entries", util, TabletServerMetricsUtil::getEntries)
        .description("Number of entries").register(registry);
    Gauge.builder("entriesInMem", util, TabletServerMetricsUtil::getEntriesInMemory)
        .description("Number of entries in memory").register(registry);
    Gauge.builder("activeMajCs", util, TabletServerMetricsUtil::getMajorCompactions)
        .description("Number of active major compactions").register(registry);
    Gauge.builder("queuedMajCs", util, TabletServerMetricsUtil::getMajorCompactionsQueued)
        .description("Number of queued major compactions").register(registry);
    Gauge.builder("activeMinCs", util, TabletServerMetricsUtil::getMinorCompactions)
        .description("Number of active minor compactions").register(registry);
    Gauge.builder("queuedMinCs", util, TabletServerMetricsUtil::getMinorCompactionsQueued)
        .description("Number of queued minor compactions").register(registry);
    Gauge.builder("onlineTablets", util, TabletServerMetricsUtil::getOnlineCount)
        .description("Number of online tablets").register(registry);
    Gauge.builder("openingTablets", util, TabletServerMetricsUtil::getOpeningCount)
        .description("Number of opening tablets").register(registry);
    Gauge.builder("unopenedTablets", util, TabletServerMetricsUtil::getUnopenedCount)
        .description("Number of unopened tablets").register(registry);
    Gauge.builder("queries", util, TabletServerMetricsUtil::getQueries)
        .description("Number of queries").register(registry);
    Gauge.builder("totalMinCs", util, TabletServerMetricsUtil::getTotalMinorCompactions)
        .description("Total number of minor compactions performed").register(registry);

    Gauge.builder("filesPerTablet", util, TabletServerMetricsUtil::getAverageFilesPerTablet)
        .description("Number of files per tablet").register(registry);
    Gauge.builder("holdTime", util, TabletServerMetricsUtil::getHoldTime)
        .description("Time commits held").register(registry);
    Gauge.builder("ingestRate", util, TabletServerMetricsUtil::getIngest)
        .description("Ingest rate (entries/sec)").register(registry);
    Gauge.builder("ingestByteRate", util, TabletServerMetricsUtil::getIngestByteRate)
        .description("Ingest rate (bytes/sec)").register(registry);
    Gauge.builder("queryRate", util, TabletServerMetricsUtil::getQueries)
        .description("Query rate (entries/sec)").register(registry);
    Gauge.builder("queryByteRate", util, TabletServerMetricsUtil::getQueryByteRate)
        .description("Query rate (bytes/sec)").register(registry);
    Gauge.builder("scannedRate", util, TabletServerMetricsUtil::getScannedRate)
        .description("Scanned rate").register(registry);

    hadoopMetrics = new TabletServerHadoopMetrics(super.getRegistry());
  }

  @Override
  public void prepareMetrics() {
    hadoopMetrics.prepareMetrics();
  }

  @Override
  public void getMoreMetrics(MetricsRecordBuilder builder, boolean all) {
    hadoopMetrics.getMoreMetrics(builder, all);
  }

  private class TabletServerHadoopMetrics {

    private final MutableGaugeLong entries;
    private final MutableGaugeLong entriesInMemory;
    private final MutableGaugeLong activeMajcs;
    private final MutableGaugeLong queuedMajcs;
    private final MutableGaugeLong activeMincs;
    private final MutableGaugeLong queuedMincs;
    private final MutableGaugeLong onlineTablets;
    private final MutableGaugeLong openingTablets;
    private final MutableGaugeLong unopenedTablets;
    private final MutableGaugeLong queries;
    private final MutableGaugeLong totalMincs;

    public TabletServerHadoopMetrics(MetricsRegistry registry) {
      entries = registry.newGauge("entries", "Number of entries", 0L);
      entriesInMemory = registry.newGauge("entriesInMem", "Number of entries in memory", 0L);
      activeMajcs = registry.newGauge("activeMajCs", "Number of active major compactions", 0L);
      queuedMajcs = registry.newGauge("queuedMajCs", "Number of queued major compactions", 0L);
      activeMincs = registry.newGauge("activeMinCs", "Number of active minor compactions", 0L);
      queuedMincs = registry.newGauge("queuedMinCs", "Number of queued minor compactions", 0L);
      onlineTablets = registry.newGauge("onlineTablets", "Number of online tablets", 0L);
      openingTablets = registry.newGauge("openingTablets", "Number of opening tablets", 0L);
      unopenedTablets = registry.newGauge("unopenedTablets", "Number of unopened tablets", 0L);
      queries = registry.newGauge("queries", "Number of queries", 0L);
      totalMincs =
          registry.newGauge("totalMinCs", "Total number of minor compactions performed", 0L);
    }

    protected void prepareMetrics() {
      entries.set(util.getEntries());
      entriesInMemory.set(util.getEntriesInMemory());
      activeMajcs.set(util.getMajorCompactions());
      queuedMajcs.set(util.getMajorCompactionsQueued());
      activeMincs.set(util.getMinorCompactions());
      queuedMincs.set(util.getMinorCompactionsQueued());
      onlineTablets.set(util.getOnlineCount());
      openingTablets.set(util.getOpeningCount());
      unopenedTablets.set(util.getUnopenedCount());
      queries.set(util.getQueries());
      totalMincs.set(util.getTotalMinorCompactions());
    }

    protected void getMoreMetrics(MetricsRecordBuilder builder, boolean all) {
      // TODO Some day, MetricsRegistry will also support the MetricsGaugeDouble or allow us to
      // instantiate it directly
      builder.addGauge(Interns.info("filesPerTablet", "Number of files per tablet"),
          util.getAverageFilesPerTablet());
      builder.addGauge(Interns.info("holdTime", "Time commits held"), util.getHoldTime());
      builder.addGauge(Interns.info("ingestRate", "Ingest rate (entries/sec)"), util.getIngest());
      builder.addGauge(Interns.info("ingestByteRate", "Ingest rate (bytes/sec)"),
          util.getIngestByteRate());
      builder.addGauge(Interns.info("queryRate", "Query rate (entries/sec)"), util.getQueryRate());
      builder.addGauge(Interns.info("queryByteRate", "Query rate (bytes/sec)"),
          util.getQueryByteRate());
      builder.addGauge(Interns.info("scannedRate", "Scanned rate"), util.getScannedRate());
    }
  }
}

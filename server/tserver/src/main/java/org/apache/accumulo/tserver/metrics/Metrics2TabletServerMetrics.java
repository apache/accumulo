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
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 *
 */
public class Metrics2TabletServerMetrics implements Metrics, MetricsSource, TabletServerMetricsKeys {
  public static final String NAME = TSERVER_NAME + ",sub=General", DESCRIPTION = "General TabletServer Metrics", CONTEXT = "tserver", RECORD = "general";

  private final TabletServerMetricsUtil util;
  private final MetricsSystem system;
  private final MetricsRegistry registry;

  private final MutableGaugeLong entries, entriesInMemory, activeMajcs, queuedMajcs, activeMincs, queuedMincs, onlineTablets, openingTablets, unopenedTablets,
      queries, totalMincs;

  // Use TabletServerMetricsFactory
  Metrics2TabletServerMetrics(TabletServer tserver, MetricsSystem system) {
    util = new TabletServerMetricsUtil(tserver);
    this.system = system;
    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    this.registry.tag(MsInfo.ProcessName, MetricsSystemHelper.getProcessName());

    entries = registry.newGauge(Interns.info(ENTRIES, "Number of entries"), 0l);
    entriesInMemory = registry.newGauge(Interns.info(ENTRIES_IN_MEM, "Number of entries in memory"), 0l);
    activeMajcs = registry.newGauge(Interns.info(ACTIVE_MAJCS, "Number of active major compactions"), 0l);
    queuedMajcs = registry.newGauge(Interns.info(QUEUED_MAJCS, "Number of queued major compactions"), 0l);
    activeMincs = registry.newGauge(Interns.info(ACTIVE_MINCS, "Number of active minor compactions"), 0l);
    queuedMincs = registry.newGauge(Interns.info(QUEUED_MINCS, "Number of queued minor compactions"), 0l);
    onlineTablets = registry.newGauge(Interns.info(ONLINE_TABLETS, "Number of online tablets"), 0l);
    openingTablets = registry.newGauge(Interns.info(OPENING_TABLETS, "Number of opening tablets"), 0l);
    unopenedTablets = registry.newGauge(Interns.info(UNOPENED_TABLETS, "Number of unopened tablets"), 0l);
    queries = registry.newGauge(Interns.info(QUERIES, "Number of queries"), 0l);
    totalMincs = registry.newGauge(Interns.info(TOTAL_MINCS, "Total number of minor compactions performed"), 0l);
  }

  @Override
  public void add(String name, long time) {
    throw new UnsupportedOperationException("add() is not implemented");
  }

  @Override
  public void register() {
    system.register(NAME, DESCRIPTION, this);
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  protected void snapshot() {

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

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);

    // Update each MutableMetric with the new value
    snapshot();

    // Add then all to the builder
    registry.snapshot(builder, all);

    // TODO Some day, MetricsRegistry will also support the MetricsGaugeDouble or allow us to instantiate it directly
    builder.addGauge(Interns.info(FILES_PER_TABLET, "Number of files per tablet"), util.getAverageFilesPerTablet());
    builder.addGauge(Interns.info(HOLD_TIME, "Time commits held"), util.getHoldTime());
  }

}

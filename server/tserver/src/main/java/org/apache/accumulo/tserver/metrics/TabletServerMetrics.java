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
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_STUCK;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_HOLD;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_INGEST_BYTES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_INGEST_MUTATIONS;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MEM_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MINC_QUEUED;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MINC_RUNNING;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MINC_TOTAL;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_FILES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_LONG_ASSIGNMENTS;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_ONDEMAND_UNLOADED_FOR_MEM;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_ONLINE;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_ONLINE_ONDEMAND;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_OPENING;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_UNOPENED;

import java.util.List;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.compaction.CompactionWatcher;
import org.apache.accumulo.server.compaction.FileCompactor;
import org.apache.accumulo.server.metrics.PerTableMetrics;
import org.apache.accumulo.tserver.TabletServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

public class TabletServerMetrics extends PerTableMetrics<TabletServerMetrics.TableMetrics>
    implements MetricsProducer {

  private final TabletServerMetricsUtil util;

  private static final Logger log = LoggerFactory.getLogger(TabletServerMetrics.class);

  @Override
  protected Logger getLog() {
    return log;
  }

  public TabletServerMetrics(TabletServer tserver) {
    super(tserver.getContext(), tserver::getOnlineTableIds);
    util = new TabletServerMetricsUtil(tserver);
  }

  private long getTotalEntriesRead() {
    return FileCompactor.getTotalEntriesRead();
  }

  private long getTotalEntriesWritten() {
    return FileCompactor.getTotalEntriesWritten();
  }

  public static class TableMetrics {
    TableMetrics(TabletServerMetricsUtil util, TableId tableId, MeterRegistry registry,
        Consumer<Meter> meters, List<Tag> tags) {

      meters.accept(Gauge.builder(TSERVER_ENTRIES.getName(), util, tsmu -> tsmu.getEntries(tableId))
          .description(TSERVER_ENTRIES.getDescription()).tags(tags).register(registry));
      meters.accept(Gauge
          .builder(TSERVER_MEM_ENTRIES.getName(), util, tsmu -> tsmu.getEntriesInMemory(tableId))
          .description(TSERVER_MEM_ENTRIES.getDescription()).tags(tags).register(registry));
      meters.accept(Gauge
          .builder(TSERVER_MINC_RUNNING.getName(), util, tsmu -> tsmu.getMinorCompactions(tableId))
          .description(TSERVER_MINC_RUNNING.getDescription()).tags(tags).register(registry));
      meters.accept(Gauge
          .builder(TSERVER_MINC_QUEUED.getName(), util,
              tsmu -> tsmu.getMinorCompactionsQueued(tableId))
          .description(TSERVER_MINC_QUEUED.getDescription()).tags(tags).register(registry));
      meters.accept(Gauge
          .builder(TSERVER_TABLETS_ONLINE_ONDEMAND.getName(), util,
              tsmu -> tsmu.getOnDemandOnlineCount(tableId))
          .description(TSERVER_TABLETS_ONLINE_ONDEMAND.getDescription()).tags(tags)
          .register(registry));
      meters.accept(Gauge
          .builder(TSERVER_TABLETS_FILES.getName(), util,
              tsmu -> tsmu.getAverageFilesPerTablet(tableId))
          .description(TSERVER_TABLETS_FILES.getDescription()).tags(tags).register(registry));
      meters.accept(Gauge
          .builder(TSERVER_INGEST_MUTATIONS.getName(), util, tsmu -> tsmu.getIngestCount(tableId))
          .description(TSERVER_INGEST_MUTATIONS.getDescription()).tags(tags).register(registry));
      meters.accept(Gauge
          .builder(TSERVER_INGEST_BYTES.getName(), util, tsmu -> tsmu.getIngestByteCount(tableId))
          .description(TSERVER_INGEST_BYTES.getDescription()).tags(tags).register(registry));
    }
  }

  @Override
  protected TableMetrics newAllTablesMetrics(MeterRegistry registry, Consumer<Meter> meters,
      List<Tag> tags) {
    return new TableMetrics(util, null, registry, meters, tags);
  }

  @Override
  protected TableMetrics newPerTableMetrics(MeterRegistry registry, TableId tableId,
      Consumer<Meter> meters, List<Tag> tags) {
    return new TableMetrics(util, tableId, registry, meters, tags);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    super.registerMetrics(registry);
    FunctionCounter
        .builder(COMPACTOR_ENTRIES_READ.getName(), this, TabletServerMetrics::getTotalEntriesRead)
        .description(COMPACTOR_ENTRIES_READ.getDescription()).register(registry);
    FunctionCounter
        .builder(COMPACTOR_ENTRIES_WRITTEN.getName(), this,
            TabletServerMetrics::getTotalEntriesWritten)
        .description(COMPACTOR_ENTRIES_WRITTEN.getDescription()).register(registry);
    LongTaskTimer timer = LongTaskTimer.builder(COMPACTOR_MAJC_STUCK.getName())
        .description(COMPACTOR_MAJC_STUCK.getDescription()).register(registry);
    CompactionWatcher.setTimer(timer);

    Gauge
        .builder(TSERVER_TABLETS_LONG_ASSIGNMENTS.getName(), util,
            TabletServerMetricsUtil::getLongTabletAssignments)
        .description(TSERVER_TABLETS_LONG_ASSIGNMENTS.getDescription()).register(registry);
    Gauge
        .builder(TSERVER_TABLETS_ONDEMAND_UNLOADED_FOR_MEM.getName(), util,
            TabletServerMetricsUtil::getOnDemandUnloadedLowMem)
        .description(TSERVER_TABLETS_ONDEMAND_UNLOADED_FOR_MEM.getDescription()).register(registry);
    Gauge.builder(TSERVER_TABLETS_ONLINE.getName(), util, TabletServerMetricsUtil::getOnlineCount)
        .description(TSERVER_TABLETS_ONLINE.getDescription()).register(registry);
    Gauge.builder(TSERVER_TABLETS_OPENING.getName(), util, TabletServerMetricsUtil::getOpeningCount)
        .description(TSERVER_TABLETS_OPENING.getDescription()).register(registry);
    Gauge
        .builder(TSERVER_TABLETS_UNOPENED.getName(), util,
            TabletServerMetricsUtil::getUnopenedCount)
        .description(TSERVER_TABLETS_UNOPENED.getDescription()).register(registry);
    Gauge
        .builder(TSERVER_MINC_TOTAL.getName(), util,
            TabletServerMetricsUtil::getTotalMinorCompactions)
        .description(TSERVER_MINC_TOTAL.getDescription()).register(registry);
    Gauge.builder(TSERVER_HOLD.getName(), util, TabletServerMetricsUtil::getHoldTime)
        .description(TSERVER_HOLD.getDescription()).register(registry);
  }
}

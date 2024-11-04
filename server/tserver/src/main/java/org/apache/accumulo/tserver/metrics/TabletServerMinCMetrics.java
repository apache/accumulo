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

import static org.apache.accumulo.core.metrics.Metric.MINC_QUEUED;
import static org.apache.accumulo.core.metrics.Metric.MINC_RUNNING;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metrics.PerTableMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

public class TabletServerMinCMetrics extends PerTableMetrics<TabletServerMinCMetrics.TableMetrics>
    implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(TabletServerMinCMetrics.class);

  @Override
  protected Logger getLog() {
    return log;
  }

  public TabletServerMinCMetrics(ServerContext context, ActiveTableIdTracker activeTableIdTracker) {
    super(context, activeTableIdTracker);
  }

  public static class TableMetrics {
    private final Timer activeMinc;
    private final Timer queuedMinc;

    TableMetrics(MeterRegistry registry, Consumer<Meter> meters, List<Tag> tags) {
      activeMinc = Timer.builder(MINC_RUNNING.getName()).description(MINC_RUNNING.getDescription())
          .tags(tags).register(registry);
      queuedMinc = Timer.builder(MINC_QUEUED.getName()).description(MINC_QUEUED.getDescription())
          .tags(tags).register(registry);
      meters.accept(activeMinc);
      meters.accept(queuedMinc);
    }
  }

  public void addActive(TableId tableId, long value) {
    getTableMetrics(tableId).activeMinc.record(Duration.ofMillis(value));
  }

  public void addQueued(TableId tableId, long value) {
    getTableMetrics(tableId).queuedMinc.record(Duration.ofMillis(value));
  }

  @Override
  protected TableMetrics newAllTablesMetrics(MeterRegistry registry, Consumer<Meter> meters,
      List<Tag> tags) {
    return new TableMetrics(registry, meters, tags);
  }

  @Override
  protected TableMetrics newPerTableMetrics(MeterRegistry registry, TableId tableId,
      Consumer<Meter> meters, List<Tag> tags) {
    return new TableMetrics(registry, meters, tags);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    super.registerMetrics(registry);
  }

}

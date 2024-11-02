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

import static org.apache.accumulo.core.metrics.Metric.UPDATE_COMMIT;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_COMMIT_PREP;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_MUTATION_ARRAY_SIZE;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_WALOG_WRITE;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metrics.NoopMetrics;
import org.apache.accumulo.server.metrics.PerTableMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

public class TabletServerUpdateMetrics
    extends PerTableMetrics<TabletServerUpdateMetrics.TableMetrics> implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(TabletServerUpdateMetrics.class);

  @Override
  protected Logger getLog() {
    return log;
  }

  private Timer commitPrepStat = NoopMetrics.useNoopTimer();
  private Timer walogWriteTimeStat = NoopMetrics.useNoopTimer();
  private Timer commitTimeStat = NoopMetrics.useNoopTimer();

  public TabletServerUpdateMetrics(ServerContext context,
      Supplier<Set<TableId>> activeTableSupplier) {
    super(context, activeTableSupplier);
  }

  public static class TableMetrics {

    private final AtomicLong permissionErrorsCount = new AtomicLong();
    private final AtomicLong unknownTabletErrorsCount = new AtomicLong();
    private final AtomicLong constraintViolationsCount = new AtomicLong();

    private DistributionSummary mutationArraySizeStat = NoopMetrics.useNoopDistributionSummary();

    TableMetrics(TableId tableId, MeterRegistry registry, Consumer<Meter> meters, List<Tag> tags) {
      meters.accept(
          FunctionCounter.builder(UPDATE_ERRORS.getName(), permissionErrorsCount, AtomicLong::get)
              .tags("type", "permission").tags(tags).description(UPDATE_ERRORS.getDescription())
              .register(registry));
      meters.accept(FunctionCounter
          .builder(UPDATE_ERRORS.getName(), unknownTabletErrorsCount, AtomicLong::get)
          .tags("type", "unknown.tablet").tags(tags).description(UPDATE_ERRORS.getDescription())
          .register(registry));
      meters.accept(FunctionCounter
          .builder(UPDATE_ERRORS.getName(), constraintViolationsCount, AtomicLong::get)
          .tags("type", "constraint.violation").tags(tags)
          .description(UPDATE_ERRORS.getDescription()).register(registry));
      mutationArraySizeStat = DistributionSummary.builder(UPDATE_MUTATION_ARRAY_SIZE.getName())
          .description(UPDATE_MUTATION_ARRAY_SIZE.getDescription()).tags(tags).register(registry);
      meters.accept(mutationArraySizeStat);
    }
  }

  public void addPermissionErrors(TableId tableId, long value) {
    getTableMetrics(tableId).permissionErrorsCount.addAndGet(value);
  }

  public void addUnknownTabletErrors(TableId tableId, long value) {
    getTableMetrics(tableId).unknownTabletErrorsCount.addAndGet(value);
  }

  public void addConstraintViolations(TableId tableId, long value) {
    getTableMetrics(tableId).constraintViolationsCount.addAndGet(value);
  }

  public void addCommitPrep(long value) {
    commitPrepStat.record(Duration.ofMillis(value));
  }

  public void addWalogWriteTime(long value) {
    walogWriteTimeStat.record(Duration.ofMillis(value));
  }

  public void addCommitTime(long value) {
    commitTimeStat.record(Duration.ofMillis(value));
  }

  public void addMutationArraySize(TableId tableId, long value) {
    getTableMetrics(tableId).mutationArraySizeStat.record(value);
  }

  @Override
  protected TableMetrics newAllTablesMetrics(MeterRegistry registry, Consumer<Meter> meters,
      List<Tag> tags) {
    return new TableMetrics(null, registry, meters, tags);
  }

  @Override
  protected TableMetrics newPerTableMetrics(MeterRegistry registry, TableId tableId,
      Consumer<Meter> meters, List<Tag> tags) {
    return new TableMetrics(tableId, registry, meters, tags);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    super.registerMetrics(registry);
    commitPrepStat = Timer.builder(UPDATE_COMMIT_PREP.getName())
        .description(UPDATE_COMMIT_PREP.getDescription()).register(registry);
    walogWriteTimeStat = Timer.builder(UPDATE_WALOG_WRITE.getName())
        .description(UPDATE_WALOG_WRITE.getDescription()).register(registry);
    commitTimeStat = Timer.builder(UPDATE_COMMIT.getName())
        .description(UPDATE_COMMIT.getDescription()).register(registry);
  }
}

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

import static org.apache.accumulo.core.metrics.Metric.SCAN_BUSY_TIMEOUT_COUNT;
import static org.apache.accumulo.core.metrics.Metric.SCAN_CLOSE;
import static org.apache.accumulo.core.metrics.Metric.SCAN_CONTINUE;
import static org.apache.accumulo.core.metrics.Metric.SCAN_OPEN_FILES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_PAUSED_FOR_MEM;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERIES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERY_SCAN_RESULTS;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERY_SCAN_RESULTS_BYTES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_RESULTS;
import static org.apache.accumulo.core.metrics.Metric.SCAN_RETURN_FOR_MEM;
import static org.apache.accumulo.core.metrics.Metric.SCAN_SCANNED_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_START;
import static org.apache.accumulo.core.metrics.Metric.SCAN_TIMES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_YIELDS;
import static org.apache.accumulo.core.metrics.Metric.SCAN_ZOMBIE_THREADS;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metrics.PerTableMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

public class TabletServerScanMetrics extends PerTableMetrics<TabletServerScanMetrics.TableMetrics> {

  private static final Logger log = LoggerFactory.getLogger(TabletServerScanMetrics.class);

  @Override
  protected Logger getLog() {
    return log;
  }

  private final IntSupplier openFiles;
  private final AtomicLong pausedForMemory = new AtomicLong(0);
  private final AtomicLong earlyReturnForMemory = new AtomicLong(0);
  private final AtomicLong zombieScanThreads = new AtomicLong(0);
  private final AtomicLong busyTimeoutCount = new AtomicLong(0);

  public static class TableMetrics {
    private final Timer scans;
    private final DistributionSummary resultsPerScan;
    private final DistributionSummary yields;
    private final AtomicLong startScanCalls = new AtomicLong(0);
    private final AtomicLong continueScanCalls = new AtomicLong(0);
    private final AtomicLong closeScanCalls = new AtomicLong(0);
    private final LongAdder lookupCount = new LongAdder();
    private final LongAdder queryResultCount = new LongAdder();
    private final LongAdder queryResultBytes = new LongAdder();
    private final LongAdder scannedCount = new LongAdder();

    TableMetrics(MeterRegistry registry, Consumer<Meter> meters, List<Tag> tags) {
      scans = Timer.builder(SCAN_TIMES.getName()).description(SCAN_TIMES.getDescription())
          .tags(tags).register(registry);
      resultsPerScan = DistributionSummary.builder(SCAN_RESULTS.getName())
          .description(SCAN_RESULTS.getDescription()).tags(tags).register(registry);
      yields = DistributionSummary.builder(SCAN_YIELDS.getName())
          .description(SCAN_YIELDS.getDescription()).tags(tags).register(registry);
      meters.accept(scans);
      meters.accept(resultsPerScan);
      meters.accept(yields);
      meters.accept(
          FunctionCounter.builder(SCAN_START.getName(), this.startScanCalls, AtomicLong::get)
              .description(SCAN_START.getDescription()).tags(tags).register(registry));
      meters.accept(
          FunctionCounter.builder(SCAN_CONTINUE.getName(), this.continueScanCalls, AtomicLong::get)
              .description(SCAN_CONTINUE.getDescription()).tags(tags).register(registry));
      meters.accept(
          FunctionCounter.builder(SCAN_CLOSE.getName(), this.closeScanCalls, AtomicLong::get)
              .description(SCAN_CLOSE.getDescription()).tags(tags).register(registry));
      meters
          .accept(FunctionCounter.builder(SCAN_QUERIES.getName(), this.lookupCount, LongAdder::sum)
              .description(SCAN_QUERIES.getDescription()).tags(tags).register(registry));
      meters.accept(
          FunctionCounter.builder(SCAN_SCANNED_ENTRIES.getName(), this.scannedCount, LongAdder::sum)
              .description(SCAN_SCANNED_ENTRIES.getDescription()).tags(tags).register(registry));
      meters.accept(
          Gauge.builder(SCAN_QUERY_SCAN_RESULTS.getName(), this.queryResultCount, LongAdder::sum)
              .description(SCAN_QUERY_SCAN_RESULTS.getDescription()).tags(tags).register(registry));
      meters.accept(Gauge
          .builder(SCAN_QUERY_SCAN_RESULTS_BYTES.getName(), this.queryResultBytes, LongAdder::sum)
          .description(SCAN_QUERY_SCAN_RESULTS_BYTES.getDescription()).tags(tags)
          .register(registry));
    }
  }

  public void incrementLookupCount(TableId tableId) {
    getTableMetrics(tableId).lookupCount.increment();
  }

  public void incrementQueryResultCount(TableId tableId, long amount) {
    getTableMetrics(tableId).queryResultCount.add(amount);
  }

  public void incrementQueryResultBytes(TableId tableId, long amount) {
    getTableMetrics(tableId).queryResultBytes.add(amount);
  }

  public LongAdder getScannedCounter(TableId tableId) {
    return getTableMetrics(tableId).scannedCount;
  }

  public void addScan(TableId tableId, long value) {
    getTableMetrics(tableId).scans.record(Duration.ofMillis(value));
  }

  public void addResult(TableId tableId, long value) {
    getTableMetrics(tableId).resultsPerScan.record(value);
  }

  public void addYield(TableId tableId, long value) {
    getTableMetrics(tableId).yields.record(value);
  }

  public void incrementStartScan(TableId tableId) {
    getTableMetrics(tableId).startScanCalls.incrementAndGet();
  }

  public void incrementContinueScan(TableId tableId) {
    getTableMetrics(tableId).continueScanCalls.incrementAndGet();
  }

  public void incrementCloseScan(TableId tableId) {
    getTableMetrics(tableId).closeScanCalls.incrementAndGet();
  }

  public void incrementBusy() {
    busyTimeoutCount.incrementAndGet();
  }

  public void incrementScanPausedForLowMemory() {
    pausedForMemory.incrementAndGet();
  }

  public void incrementEarlyReturnForLowMemory() {
    earlyReturnForMemory.incrementAndGet();
  }

  public void setZombieScanThreads(long count) {
    zombieScanThreads.set(count);
  }

  public long getZombieThreadsCount() {
    return zombieScanThreads.get();
  }

  public TabletServerScanMetrics(ServerContext serverContext,
      ActiveTableIdTracker activeTableIdTracker, IntSupplier openFileSupplier) {
    super(serverContext, activeTableIdTracker);
    openFiles = openFileSupplier;
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

    Gauge.builder(SCAN_OPEN_FILES.getName(), openFiles::getAsInt)
        .description(SCAN_OPEN_FILES.getDescription()).register(registry);
    FunctionCounter.builder(SCAN_PAUSED_FOR_MEM.getName(), this.pausedForMemory, AtomicLong::get)
        .description(SCAN_PAUSED_FOR_MEM.getDescription()).register(registry);
    FunctionCounter
        .builder(SCAN_RETURN_FOR_MEM.getName(), this.earlyReturnForMemory, AtomicLong::get)
        .description(SCAN_RETURN_FOR_MEM.getDescription()).register(registry);
    Gauge
        .builder(SCAN_ZOMBIE_THREADS.getName(), this,
            TabletServerScanMetrics::getZombieThreadsCount)
        .description(SCAN_ZOMBIE_THREADS.getDescription()).register(registry);
    FunctionCounter
        .builder(SCAN_BUSY_TIMEOUT_COUNT.getName(), this.busyTimeoutCount, AtomicLong::get)
        .description(SCAN_BUSY_TIMEOUT_COUNT.getDescription()).register(registry);
  }
}

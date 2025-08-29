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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.metrics.NoopMetrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerScanMetrics implements MetricsProducer {

  private final IntSupplier openFiles;
  private Timer scans = NoopMetrics.useNoopTimer();
  private DistributionSummary resultsPerScan = NoopMetrics.useNoopDistributionSummary();
  private DistributionSummary yields = NoopMetrics.useNoopDistributionSummary();
  private final AtomicLong startScanCalls = new AtomicLong(0);
  private final AtomicLong continueScanCalls = new AtomicLong(0);
  private final AtomicLong closeScanCalls = new AtomicLong(0);
  private final AtomicLong busyTimeoutCount = new AtomicLong(0);
  private final AtomicLong pausedForMemory = new AtomicLong(0);
  private final AtomicLong earlyReturnForMemory = new AtomicLong(0);
  private final AtomicLong zombieScanThreads = new AtomicLong(0);
  private final LongAdder lookupCount = new LongAdder();
  private final LongAdder queryResultCount = new LongAdder();
  private final LongAdder queryResultBytes = new LongAdder();
  private final LongAdder scannedCount = new LongAdder();

  public void incrementLookupCount() {
    this.lookupCount.increment();
  }

  public void incrementQueryResultCount(long amount) {
    this.queryResultCount.add(amount);
  }

  public void incrementQueryResultBytes(long amount) {
    this.queryResultBytes.add(amount);
  }

  public LongAdder getScannedCounter() {
    return this.scannedCount;
  }

  public void addScan(long value) {
    scans.record(Duration.ofMillis(value));
  }

  public void addResult(long value) {
    resultsPerScan.record(value);
  }

  public void addYield(long value) {
    yields.record(value);
  }

  public void incrementStartScan() {
    startScanCalls.incrementAndGet();
  }

  public void incrementContinueScan() {
    continueScanCalls.incrementAndGet();
  }

  public void incrementCloseScan() {
    closeScanCalls.incrementAndGet();
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

  public TabletServerScanMetrics(IntSupplier openFileSupplier) {
    openFiles = openFileSupplier;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(SCAN_OPEN_FILES.getName(), openFiles::getAsInt)
        .description(SCAN_OPEN_FILES.getDescription()).register(registry);
    scans = Timer.builder(SCAN_TIMES.getName()).description(SCAN_TIMES.getDescription())
        .register(registry);
    resultsPerScan = DistributionSummary.builder(SCAN_RESULTS.getName())
        .description(SCAN_RESULTS.getDescription()).register(registry);
    yields = DistributionSummary.builder(SCAN_YIELDS.getName())
        .description(SCAN_YIELDS.getDescription()).register(registry);
    FunctionCounter.builder(SCAN_START.getName(), this.startScanCalls, AtomicLong::get)
        .description(SCAN_START.getDescription()).register(registry);
    FunctionCounter.builder(SCAN_CONTINUE.getName(), this.continueScanCalls, AtomicLong::get)
        .description(SCAN_CONTINUE.getDescription()).register(registry);
    FunctionCounter.builder(SCAN_CLOSE.getName(), this.closeScanCalls, AtomicLong::get)
        .description(SCAN_CLOSE.getDescription()).register(registry);
    FunctionCounter
        .builder(SCAN_BUSY_TIMEOUT_COUNT.getName(), this.busyTimeoutCount, AtomicLong::get)
        .description(SCAN_BUSY_TIMEOUT_COUNT.getDescription()).register(registry);
    FunctionCounter.builder(SCAN_QUERIES.getName(), this.lookupCount, LongAdder::sum)
        .description(SCAN_QUERIES.getDescription()).register(registry);
    FunctionCounter.builder(SCAN_SCANNED_ENTRIES.getName(), this.scannedCount, LongAdder::sum)
        .description(SCAN_SCANNED_ENTRIES.getDescription()).register(registry);
    FunctionCounter.builder(SCAN_PAUSED_FOR_MEM.getName(), this.pausedForMemory, AtomicLong::get)
        .description(SCAN_PAUSED_FOR_MEM.getDescription()).register(registry);
    FunctionCounter
        .builder(SCAN_RETURN_FOR_MEM.getName(), this.earlyReturnForMemory, AtomicLong::get)
        .description(SCAN_RETURN_FOR_MEM.getDescription()).register(registry);
    Gauge.builder(SCAN_QUERY_SCAN_RESULTS.getName(), this.queryResultCount, LongAdder::sum)
        .description(SCAN_QUERY_SCAN_RESULTS.getDescription()).register(registry);
    Gauge.builder(SCAN_QUERY_SCAN_RESULTS_BYTES.getName(), this.queryResultBytes, LongAdder::sum)
        .description(SCAN_QUERY_SCAN_RESULTS_BYTES.getDescription()).register(registry);
    Gauge
        .builder(SCAN_ZOMBIE_THREADS.getName(), this,
            TabletServerScanMetrics::getZombieThreadsCount)
        .description(SCAN_ZOMBIE_THREADS.getDescription()).register(registry);
  }

}

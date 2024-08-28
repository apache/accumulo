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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.metrics.NoopMetrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerScanMetrics implements MetricsProducer {

  private final AtomicInteger openFiles = new AtomicInteger(0);

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

  public void incrementOpenFiles(int delta) {
    openFiles.addAndGet(Math.max(0, delta));
  }

  public void decrementOpenFiles(int delta) {
    openFiles.addAndGet(delta < 0 ? delta : delta * -1);
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

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(METRICS_SCAN_OPEN_FILES, openFiles::get)
        .description("Number of files open for scans").register(registry);
    scans = Timer.builder(METRICS_SCAN_TIMES).description("Scans").register(registry);
    resultsPerScan = DistributionSummary.builder(METRICS_SCAN_RESULTS)
        .description("Results per scan").register(registry);
    yields =
        DistributionSummary.builder(METRICS_SCAN_YIELDS).description("yields").register(registry);
    FunctionCounter.builder(METRICS_SCAN_START, this.startScanCalls, AtomicLong::get)
        .description("calls to start a scan / multiscan").register(registry);
    FunctionCounter.builder(METRICS_SCAN_CONTINUE, this.continueScanCalls, AtomicLong::get)
        .description("calls to continue a scan / multiscan").register(registry);
    FunctionCounter.builder(METRICS_SCAN_CLOSE, this.closeScanCalls, AtomicLong::get)
        .description("calls to close a scan / multiscan").register(registry);
    FunctionCounter
        .builder(METRICS_SCAN_BUSY_TIMEOUT_COUNTER, this.busyTimeoutCount, AtomicLong::get)
        .description("The number of scans where a busy timeout happened").register(registry);
    FunctionCounter.builder(METRICS_SCAN_QUERIES, this.lookupCount, LongAdder::sum)
        .description("Number of queries").register(registry);
    FunctionCounter.builder(METRICS_SCAN_SCANNED_ENTRIES, this.scannedCount, LongAdder::sum)
        .description("Scanned rate").register(registry);
    FunctionCounter.builder(METRICS_SCAN_PAUSED_FOR_MEM, this.pausedForMemory, AtomicLong::get)
        .description("scan paused due to server being low on memory").register(registry);
    FunctionCounter.builder(METRICS_SCAN_RETURN_FOR_MEM, this.earlyReturnForMemory, AtomicLong::get)
        .description("scan returned results early due to server being low on memory")
        .register(registry);
    Gauge.builder(METRICS_SCAN_QUERY_SCAN_RESULTS, this.queryResultCount, LongAdder::sum)
        .description("Query rate (entries/sec)").register(registry);
    Gauge.builder(METRICS_SCAN_QUERY_SCAN_RESULTS_BYTES, this.queryResultBytes, LongAdder::sum)
        .description("Query rate (bytes/sec)").register(registry);
    Gauge.builder(METRICS_SCAN_ZOMBIE_THREADS, this, TabletServerScanMetrics::getZombieThreadsCount)
        .description("Number of scan threads that have no associated client session")
        .register(registry);
  }

}

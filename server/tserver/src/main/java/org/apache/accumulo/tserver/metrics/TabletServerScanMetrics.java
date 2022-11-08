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
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class TabletServerScanMetrics implements MetricsProducer {

  private final AtomicInteger openFiles = new AtomicInteger(0);
  private Timer scans;
  private DistributionSummary resultsPerScan;
  private DistributionSummary yields;
  private Counter startScanCalls;
  private Counter continueScanCalls;
  private Counter closeScanCalls;
  private Counter busyTimeoutReturned;

  private final LongAdder lookupCount = new LongAdder();
  private final LongAdder queryResultCount = new LongAdder();
  private final LongAdder queryResultBytes = new LongAdder();
  private final LongAdder scannedCount = new LongAdder();

  public void incrementLookupCount(long amount) {
    this.lookupCount.add(amount);
  }

  public long getLookupCount() {
    return this.lookupCount.sum();
  }

  public void incrementQueryResultCount(long amount) {
    this.queryResultCount.add(amount);
  }

  public long getQueryResultCount() {
    return this.queryResultCount.sum();
  }

  public void incrementQueryResultBytes(long amount) {
    this.queryResultBytes.add(amount);
  }

  public long getQueryByteCount() {
    return this.queryResultBytes.sum();
  }

  public void incrementScannedCount(long amount) {
    this.scannedCount.add(amount);
  }

  public LongAdder getScannedCounter() {
    return this.scannedCount;
  }

  public long getScannedCount() {
    return this.scannedCount.sum();
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

  public void incrementStartScan(double value) {
    startScanCalls.increment(value);
  }

  public void incrementContinueScan(double value) {
    continueScanCalls.increment(value);
  }

  public void incrementCloseScan(double value) {
    closeScanCalls.increment(value);
  }

  public void incrementScanBusyTimeout(double value) {
    busyTimeoutReturned.increment(value);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(METRICS_SCAN_OPEN_FILES, openFiles::get)
        .description("Number of files open for scans").register(registry);
    scans = Timer.builder(METRICS_SCAN).description("Scans").register(registry);
    resultsPerScan = DistributionSummary.builder(METRICS_SCAN_RESULTS)
        .description("Results per scan").register(registry);
    yields =
        DistributionSummary.builder(METRICS_SCAN_YIELDS).description("yields").register(registry);
    startScanCalls =
        Counter.builder(METRICS_SCAN_START).description("calls to start a scan / multiscan")
            .tags(MetricsUtil.getCommonTags()).register(registry);
    continueScanCalls =
        Counter.builder(METRICS_SCAN_CONTINUE).description("calls to continue a scan / multiscan")
            .tags(MetricsUtil.getCommonTags()).register(registry);
    closeScanCalls =
        Counter.builder(METRICS_SCAN_CLOSE).description("calls to close a scan / multiscan")
            .tags(MetricsUtil.getCommonTags()).register(registry);
    busyTimeoutReturned = Counter.builder(METRICS_SCAN_BUSY_TIMEOUT)
        .description("times that a scan has timed out in the queue")
        .tags(MetricsUtil.getCommonTags()).register(registry);
    Gauge.builder(METRICS_TSERVER_QUERIES, this, TabletServerScanMetrics::getLookupCount)
        .description("Number of queries").register(registry);
    Gauge.builder(METRICS_TSERVER_SCAN_RESULTS, this, TabletServerScanMetrics::getQueryResultCount)
        .description("Query rate (entries/sec)").register(registry);
    Gauge
        .builder(METRICS_TSERVER_SCAN_RESULTS_BYTES, this,
            TabletServerScanMetrics::getQueryByteCount)
        .description("Query rate (bytes/sec)").register(registry);
    Gauge.builder(METRICS_TSERVER_SCANNED_ENTRIES, this, TabletServerScanMetrics::getScannedCount)
        .description("Scanned rate").register(registry);
  }

}

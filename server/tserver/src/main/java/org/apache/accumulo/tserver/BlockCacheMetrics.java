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
package org.apache.accumulo.tserver;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.util.threads.ThreadPools;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class BlockCacheMetrics implements MetricsProducer {

  BlockCache indexCache;
  BlockCache dataCache;
  BlockCache summaryCache;

  AtomicLong indexHitCount = new AtomicLong(0);
  AtomicLong indexRequestCount = new AtomicLong(0);

  AtomicLong dataHitCount = new AtomicLong(0);
  AtomicLong dataRequestCount = new AtomicLong(0);

  AtomicLong summaryHitCount = new AtomicLong(0);
  AtomicLong summaryRequestCount = new AtomicLong(0);

  public BlockCacheMetrics(BlockCache indexCache, BlockCache dataCache, BlockCache summaryCache) {
    this.indexCache = indexCache;
    this.dataCache = dataCache;
    this.summaryCache = summaryCache;
  }

  private void update() {
    indexHitCount.set(indexCache.getStats().hitCount());
    indexRequestCount.set(indexCache.getStats().requestCount());

    dataHitCount.set(dataCache.getStats().hitCount());
    dataRequestCount.set(dataCache.getStats().requestCount());

    summaryHitCount.set(summaryCache.getStats().hitCount());
    summaryRequestCount.set(summaryCache.getStats().requestCount());
  }

  /**
   * Register metrics for the data, index and summary block caches.
   */
  @Override
  public void registerMetrics(MeterRegistry registry) {
    Gauge.builder(METRICS_BLOCKCACHE_INDEX_HITCOUNT, indexHitCount::get)
        .description("Index block cache hit count").register(registry);
    Gauge.builder(METRICS_BLOCKCACHE_INDEX_REQUESTCOUNT, indexRequestCount::get)
        .description("Index block cache request count").register(registry);

    Gauge.builder(METRICS_BLOCKCACHE_DATA_HITCOUNT, dataHitCount::get)
        .description("Data block cache hit count").register(registry);
    Gauge.builder(METRICS_BLOCKCACHE_DATA_REQUESTCOUNT, dataRequestCount::get)
        .description("Data block cache request count").register(registry);

    Gauge.builder(METRICS_BLOCKCACHE_SUMMARY_HITCOUNT, summaryHitCount::get)
        .description("Summary block cache hit count").register(registry);
    Gauge.builder(METRICS_BLOCKCACHE_SUMMARY_REQUESTCOUNT, summaryRequestCount::get)
        .description("Summary block cache request count").register(registry);

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));
    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(this::update, 0, 5, TimeUnit.SECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }
}

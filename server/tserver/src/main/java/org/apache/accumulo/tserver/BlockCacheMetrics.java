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

import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_DATA_EVICTIONCOUNT;
import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_DATA_HITCOUNT;
import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_DATA_REQUESTCOUNT;
import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_INDEX_EVICTIONCOUNT;
import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_INDEX_HITCOUNT;
import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_INDEX_REQUESTCOUNT;
import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_SUMMARY_EVICTIONCOUNT;
import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_SUMMARY_HITCOUNT;
import static org.apache.accumulo.core.metrics.Metric.BLOCKCACHE_SUMMARY_REQUESTCOUNT;

import java.util.function.ToDoubleFunction;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.cache.BlockCache;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;

public class BlockCacheMetrics implements MetricsProducer {

  final BlockCache indexCache;
  final BlockCache dataCache;
  final BlockCache summaryCache;

  public BlockCacheMetrics(BlockCache indexCache, BlockCache dataCache, BlockCache summaryCache) {
    this.indexCache = indexCache;
    this.dataCache = dataCache;
    this.summaryCache = summaryCache;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    ToDoubleFunction<BlockCache> getHitCount = cache -> cache.getStats().hitCount();
    ToDoubleFunction<BlockCache> getRequestCount = cache -> cache.getStats().requestCount();
    ToDoubleFunction<BlockCache> getEvictionCount = cache -> cache.getStats().evictionCount();

    FunctionCounter.builder(BLOCKCACHE_INDEX_HITCOUNT.getName(), indexCache, getHitCount)
        .description(BLOCKCACHE_INDEX_HITCOUNT.getDescription()).register(registry);
    FunctionCounter.builder(BLOCKCACHE_INDEX_REQUESTCOUNT.getName(), indexCache, getRequestCount)
        .description(BLOCKCACHE_INDEX_REQUESTCOUNT.getDescription()).register(registry);
    FunctionCounter.builder(BLOCKCACHE_INDEX_EVICTIONCOUNT.getName(), indexCache, getEvictionCount)
        .description(BLOCKCACHE_INDEX_EVICTIONCOUNT.getDescription()).register(registry);

    FunctionCounter.builder(BLOCKCACHE_DATA_HITCOUNT.getName(), dataCache, getHitCount)
        .description(BLOCKCACHE_DATA_HITCOUNT.getDescription()).register(registry);
    FunctionCounter.builder(BLOCKCACHE_DATA_REQUESTCOUNT.getName(), dataCache, getRequestCount)
        .description(BLOCKCACHE_DATA_REQUESTCOUNT.getDescription()).register(registry);
    FunctionCounter.builder(BLOCKCACHE_DATA_EVICTIONCOUNT.getName(), dataCache, getEvictionCount)
        .description(BLOCKCACHE_DATA_EVICTIONCOUNT.getDescription()).register(registry);

    FunctionCounter.builder(BLOCKCACHE_SUMMARY_HITCOUNT.getName(), summaryCache, getHitCount)
        .description(BLOCKCACHE_SUMMARY_HITCOUNT.getDescription()).register(registry);
    FunctionCounter
        .builder(BLOCKCACHE_SUMMARY_REQUESTCOUNT.getName(), summaryCache, getRequestCount)
        .description(BLOCKCACHE_SUMMARY_REQUESTCOUNT.getDescription()).register(registry);
    FunctionCounter
        .builder(BLOCKCACHE_SUMMARY_EVICTIONCOUNT.getName(), summaryCache, getEvictionCount)
        .description(BLOCKCACHE_SUMMARY_EVICTIONCOUNT.getDescription()).register(registry);
  }
}

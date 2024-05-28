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

import java.util.function.ToDoubleFunction;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.cache.BlockCache;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;

public class BlockCacheMetrics implements MetricsProducer {

  BlockCache indexCache;
  BlockCache dataCache;
  BlockCache summaryCache;

  public BlockCacheMetrics(BlockCache indexCache, BlockCache dataCache, BlockCache summaryCache) {
    this.indexCache = indexCache;
    this.dataCache = dataCache;
    this.summaryCache = summaryCache;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    ToDoubleFunction<BlockCache> getHitCount = cache -> cache.getStats().hitCount();
    ToDoubleFunction<BlockCache> getRequestCount = cache -> cache.getStats().requestCount();

    FunctionCounter.builder(METRICS_BLOCKCACHE_INDEX_HITCOUNT, indexCache, getHitCount)
        .description("Index block cache hit count").register(registry);
    FunctionCounter.builder(METRICS_BLOCKCACHE_INDEX_REQUESTCOUNT, indexCache, getRequestCount)
        .description("Index block cache request count").register(registry);

    FunctionCounter.builder(METRICS_BLOCKCACHE_DATA_HITCOUNT, dataCache, getHitCount)
        .description("Data block cache hit count").register(registry);
    FunctionCounter.builder(METRICS_BLOCKCACHE_DATA_REQUESTCOUNT, dataCache, getRequestCount)
        .description("Data block cache request count").register(registry);

    FunctionCounter.builder(METRICS_BLOCKCACHE_SUMMARY_HITCOUNT, summaryCache, getHitCount)
        .description("Summary block cache hit count").register(registry);
    FunctionCounter.builder(METRICS_BLOCKCACHE_SUMMARY_REQUESTCOUNT, summaryCache, getRequestCount)
        .description("Summary block cache request count").register(registry);
  }
}

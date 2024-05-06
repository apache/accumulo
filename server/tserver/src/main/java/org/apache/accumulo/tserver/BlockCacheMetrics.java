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

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.cache.BlockCache;

import io.micrometer.core.instrument.Gauge;
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
    Gauge.builder("indexCacheHitCount", indexCache, cache -> cache.getStats().hitCount())
        .description("Index block cache hit count").register(registry);
    Gauge.builder("indexCacheRequestCount", indexCache, cache -> cache.getStats().requestCount())
        .description("Index block cache request count").register(registry);

    Gauge.builder("dataCacheHitCount", dataCache, cache -> cache.getStats().hitCount())
        .description("Data block cache hit count").register(registry);
    Gauge.builder("dataCacheRequestCount", dataCache, cache -> cache.getStats().requestCount())
        .description("Data block cache request count").register(registry);

    Gauge.builder("summaryCacheHitCount", summaryCache, cache -> cache.getStats().hitCount())
        .description("Summary block cache hit count").register(registry);
    Gauge
        .builder("summaryCacheRequestCount", summaryCache, cache -> cache.getStats().requestCount())
        .description("Summary block cache request count").register(registry);
  }
}

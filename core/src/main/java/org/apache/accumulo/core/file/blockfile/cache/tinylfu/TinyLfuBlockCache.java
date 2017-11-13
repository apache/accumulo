/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file.blockfile.cache.tinylfu;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager.Configuration;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.apache.accumulo.core.file.blockfile.cache.SoftIndexCacheEntry;
import org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize;
import org.apache.accumulo.core.file.blockfile.cache.impl.SizeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A block cache that is memory bounded using the W-TinyLFU eviction algorithm. This implementation delegates to a Caffeine cache to provide concurrent O(1)
 * read and write operations.
 * <ul>
 * <li>W-TinyLFU: http://arxiv.org/pdf/1512.00727.pdf</li>
 * <li>Caffeine: https://github.com/ben-manes/caffeine</li>
 * <li>Cache design: http://highscalability.com/blog/2016/1/25/design-of-a-modern-cache.html</li>
 * </ul>
 */
public final class TinyLfuBlockCache implements BlockCache {
  private static final Logger log = LoggerFactory.getLogger(TinyLfuBlockCache.class);
  private static final int STATS_PERIOD_SEC = 60;

  private Cache<String,Block> cache;
  private Policy.Eviction<String,Block> policy;
  private ScheduledExecutorService statsExecutor;

  public TinyLfuBlockCache(Configuration conf, CacheType type) {
    cache = Caffeine.newBuilder().initialCapacity((int) Math.ceil(1.2 * conf.getMaxSize(type) / conf.getBlockSize()))
        .weigher((String blockName, Block block) -> {
          int keyWeight = ClassSize.align(blockName.length()) + ClassSize.STRING;
          return keyWeight + block.weight();
        }).maximumWeight(conf.getMaxSize(type)).recordStats().build();
    policy = cache.policy().eviction().get();
    statsExecutor = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("TinyLfuBlockCacheStatsExecutor").setDaemon(true).build());
    statsExecutor.scheduleAtFixedRate(this::logStats, STATS_PERIOD_SEC, STATS_PERIOD_SEC, TimeUnit.SECONDS);
  }

  @Override
  public long getMaxHeapSize() {
    return getMaxSize();
  }

  @Override
  public long getMaxSize() {
    return policy.getMaximum();
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    return cache.getIfPresent(blockName);
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buffer) {
    return cache.asMap().compute(blockName, (key, block) -> {
      return new Block(buffer);
    });
  }

  @Override
  public BlockCache.Stats getStats() {
    CacheStats stats = cache.stats();
    return new BlockCache.Stats() {
      @Override
      public long hitCount() {
        return stats.hitCount();
      }

      @Override
      public long requestCount() {
        return stats.requestCount();
      }
    };
  }

  private void logStats() {
    double maxMB = ((double) policy.getMaximum()) / ((double) (1024 * 1024));
    double sizeMB = ((double) policy.weightedSize().getAsLong()) / ((double) (1024 * 1024));
    double freeMB = maxMB - sizeMB;
    log.debug("Cache Size={}MB, Free={}MB, Max={}MB, Blocks={}", sizeMB, freeMB, maxMB, cache.estimatedSize());
    log.debug(cache.stats().toString());
  }

  private static final class Block extends SoftIndexCacheEntry implements CacheEntry {

    Block(byte[] buffer) {
      super(buffer);
    }

    int weight() {
      return ClassSize.align(getBuffer().length) + SizeConstants.SIZEOF_LONG + ClassSize.REFERENCE + ClassSize.OBJECT + ClassSize.ARRAY;
    }
  }

  private Block load(String key, Loader loader, Map<String,byte[]> resolvedDeps) {
    byte[] data = loader.load((int) Math.min(Integer.MAX_VALUE, policy.getMaximum()), resolvedDeps);
    if (data == null) {
      return null;
    }

    return new Block(data);
  }

  private Map<String,byte[]> resolveDependencies(Map<String,Loader> deps) {
    if (deps.size() == 1) {
      Entry<String,Loader> entry = deps.entrySet().iterator().next();
      CacheEntry ce = getBlock(entry.getKey(), entry.getValue());
      if (ce == null) {
        return null;
      }
      return Collections.singletonMap(entry.getKey(), ce.getBuffer());
    } else {
      HashMap<String,byte[]> resolvedDeps = new HashMap<>();
      for (Entry<String,Loader> entry : deps.entrySet()) {
        CacheEntry ce = getBlock(entry.getKey(), entry.getValue());
        if (ce == null) {
          return null;
        }
        resolvedDeps.put(entry.getKey(), ce.getBuffer());
      }
      return resolvedDeps;
    }
  }

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    Map<String,Loader> deps = loader.getDependencies();

    if (deps.size() == 0) {
      return cache.get(blockName, k -> load(blockName, loader, Collections.emptyMap()));
    } else {
      // This code path exist to handle the case where dependencies may need to be loaded. Loading dependencies will access the cache. Cache load functions
      // should not access the cache.
      Block ce = cache.getIfPresent(blockName);

      if (ce == null) {
        // Load dependencies outside of cache load function.
        Map<String,byte[]> resolvedDeps = resolveDependencies(deps);
        if (resolvedDeps == null) {
          return null;
        }

        // Use asMap because it will not increment stats, getIfPresent recorded a miss above. Use computeIfAbsent because it is possible another thread loaded
        // the data since this thread called getIfPresent.
        ce = cache.asMap().computeIfAbsent(blockName, k -> load(blockName, loader, resolvedDeps));
      }

      return ce;
    }
  }
}

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
package org.apache.accumulo.core.file.blockfile.cache;

import java.util.Map;

import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.trace.ScanInstrumentation;

public class InstrumentedBlockCache implements BlockCache {

  private final BlockCache blockCache;
  private final ScanInstrumentation scanInstrumentation;
  private final CacheType cacheType;

  public InstrumentedBlockCache(CacheType cacheType, BlockCache blockCache,
      ScanInstrumentation scanInstrumentation) {
    this.blockCache = blockCache;
    this.scanInstrumentation = scanInstrumentation;
    this.cacheType = cacheType;
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return blockCache.cacheBlock(blockName, buf);
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    return blockCache.getBlock(blockName);
  }

  private final class CountingLoader implements Loader {

    private final Loader loader;
    int loadCount = 0;

    private CountingLoader(Loader loader) {
      this.loader = loader;
    }

    @Override
    public Map<String,Loader> getDependencies() {
      return loader.getDependencies();
    }

    @Override
    public byte[] load(int maxSize, Map<String,byte[]> dependencies) {
      loadCount++;
      return loader.load(maxSize, dependencies);
    }
  }

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    var cl = new CountingLoader(loader);
    var ce = blockCache.getBlock(blockName, cl);
    if (cl.loadCount == 0 && ce != null) {
      scanInstrumentation.incrementCacheHit(cacheType);
    } else {
      scanInstrumentation.incrementCacheMiss(cacheType);
    }
    return ce;
  }

  @Override
  public long getMaxHeapSize() {
    return blockCache.getMaxHeapSize();
  }

  @Override
  public long getMaxSize() {
    return blockCache.getMaxSize();
  }

  @Override
  public Stats getStats() {
    return blockCache.getStats();
  }

  public static BlockCache wrap(CacheType cacheType, BlockCache cache) {
    var si = ScanInstrumentation.get();
    if (cache != null && si != null) {
      return new InstrumentedBlockCache(cacheType, cache, si);
    } else {
      return cache;
    }
  }
}

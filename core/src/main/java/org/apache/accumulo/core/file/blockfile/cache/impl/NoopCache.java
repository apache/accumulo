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
package org.apache.accumulo.core.file.blockfile.cache.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;

// This cache exist as a hack to avoid leaking decompressors. When the RFile code is not given a
// cache it reads blocks directly from the decompressor. However if a user does not read all data
// for a scan this can leave a BCFile block open and a decompressor allocated.
//
// By providing a cache to the RFile code it forces each block to be read into memory. When a
// block is accessed the entire thing is read into memory immediately allocating and deallocating
// a decompressor. If the user does not read all data, no decompressors are left allocated.
public class NoopCache implements BlockCache {

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return null;
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    return null;
  }

  @Override
  public long getMaxHeapSize() {
    return getMaxSize();
  }

  @Override
  public long getMaxSize() {
    return Integer.MAX_VALUE;
  }

  @Override
  public Stats getStats() {
    return new BlockCache.Stats() {
      @Override
      public long hitCount() {
        return 0L;
      }

      @Override
      public long requestCount() {
        return 0L;
      }
    };
  }

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    Map<String,Loader> depLoaders = loader.getDependencies();
    Map<String,byte[]> depData;

    switch (depLoaders.size()) {
      case 0:
        depData = Collections.emptyMap();
        break;
      case 1:
        Entry<String,Loader> entry = depLoaders.entrySet().iterator().next();
        depData = Collections.singletonMap(entry.getKey(),
            getBlock(entry.getKey(), entry.getValue()).getBuffer());
        break;
      default:
        depData = new HashMap<>();
        depLoaders.forEach((k, v) -> depData.put(k, getBlock(k, v).getBuffer()));
    }

    byte[] data = loader.load(Integer.MAX_VALUE, depData);

    return new CacheEntry() {

      @Override
      public byte[] getBuffer() {
        return data;
      }

      @Override
      public <T extends Weighable> T getIndex(Supplier<T> supplier) {
        return null;
      }

      @Override
      public void indexWeightChanged() {}
    };
  }
}

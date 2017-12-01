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

package org.apache.accumulo.core.file.blockfile.cache.lru;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;

/**
 * This class implements loading in such a way that load operations for the same block will not run concurrently.
 */
public abstract class SynchronousLoadingBlockCache implements BlockCache {

  private final Lock[] loadLocks;

  /**
   * @param numLocks
   *          this controls how many load operations can run concurrently
   */
  SynchronousLoadingBlockCache(int numLocks) {
    loadLocks = new Lock[numLocks];
    for (int i = 0; i < loadLocks.length; i++) {
      loadLocks[i] = new ReentrantLock();
    }
  }

  public SynchronousLoadingBlockCache() {
    this(2017);
  }

  private Map<String,byte[]> resolveDependencies(Map<String,Loader> loaderDeps) {
    Map<String,byte[]> depData;

    switch (loaderDeps.size()) {
      case 0:
        depData = Collections.emptyMap();
        break;
      case 1: {
        Entry<String,Loader> entry = loaderDeps.entrySet().iterator().next();
        CacheEntry dce = getBlock(entry.getKey(), entry.getValue());
        if (dce == null) {
          depData = null;
        } else {
          depData = Collections.singletonMap(entry.getKey(), dce.getBuffer());
        }
        break;
      }
      default: {
        depData = new HashMap<>();
        Set<Entry<String,Loader>> es = loaderDeps.entrySet();
        for (Entry<String,Loader> entry : es) {
          CacheEntry dce = getBlock(entry.getKey(), entry.getValue());
          if (dce == null) {
            depData = null;
            break;
          }

          depData.put(entry.getKey(), dce.getBuffer());
        }
        break;
      }
    }

    return depData;
  }

  /**
   * Get the maximum size of an individual cache entry.
   */
  protected abstract int getMaxEntrySize();

  /**
   * Get a block from the cache without changing any stats the cache is keeping.
   */
  protected abstract CacheEntry getBlockNoStats(String blockName);

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {

    CacheEntry ce = getBlock(blockName);
    if (ce != null) {
      return ce;
    }

    // intentionally done before getting lock
    Map<String,byte[]> depData = resolveDependencies(loader.getDependencies());
    if (depData == null) {
      return null;
    }

    int lockIndex = (blockName.hashCode() & 0x7fffffff) % loadLocks.length;
    Lock loadLock = loadLocks[lockIndex];

    try {
      loadLock.lock();

      // check again after getting lock, could have loaded while waiting on lock
      ce = getBlockNoStats(blockName);
      if (ce != null) {
        return ce;
      }

      // not in cache so load data
      byte[] data = loader.load(getMaxEntrySize(), depData);
      if (data == null) {
        return null;
      }

      // attempt to add data to cache
      return cacheBlock(blockName, data);
    } finally {
      loadLock.unlock();
    }
  }

}

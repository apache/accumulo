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
package org.apache.accumulo.core.spi.cache;

import java.util.Map;

import org.apache.accumulo.core.file.blockfile.cache.lru.SynchronousLoadingBlockCache;

/**
 * Block cache interface.
 *
 * @since 2.0.0
 * @see org.apache.accumulo.core.spi
 */
public interface BlockCache {

  /**
   * Add block to cache.
   *
   * @param blockName Zero-based file block number.
   * @param buf The block contents wrapped in a ByteBuffer.
   */
  CacheEntry cacheBlock(String blockName, byte[] buf);

  /**
   * Fetch block from cache.
   *
   * @param blockName Block name to fetch.
   * @return Block or null if block is not in the cache.
   */
  CacheEntry getBlock(String blockName);

  interface Loader {
    /**
     * The cache blocks that this loader depends on. If a loader has no dependencies, then it should
     * return an empty map. All dependencies must be loaded before calling {@link #load(int, Map)}.
     */
    Map<String,Loader> getDependencies();

    /**
     * Loads a block. Anything returned by {@link #getDependencies()} should be loaded and passed.
     *
     * @param maxSize This is the maximum block size that will be cached.
     * @return The loaded block or null if loading the block would exceed maxSize.
     */
    byte[] load(int maxSize, Map<String,byte[]> dependencies);
  }

  /**
   * This method allows a cache to prevent concurrent loads of the same block. However a cache
   * implementation is not required to prevent concurrent loads.
   * {@link SynchronousLoadingBlockCache} is an abstract class that a cache can extent which does
   * prevent concurrent loading of the same block.
   *
   *
   * @param blockName Block name to fetch
   * @param loader If the block is not present in the cache, the loader can be called to load it.
   * @return Block or null if block is not in the cache or didn't load.
   */
  CacheEntry getBlock(String blockName, Loader loader);

  /**
   * Get the maximum amount of on heap memory this cache will use.
   */
  long getMaxHeapSize();

  /**
   * Get the maximum size of this cache.
   *
   * @return max size in bytes
   */
  long getMaxSize();

  /**
   * Get the statistics of this cache.
   *
   * @return statistics
   */
  Stats getStats();

  /** Cache statistics. */
  interface Stats {

    /**
     * Returns the number of lookups that have returned a cached value.
     *
     * @return the number of lookups that have returned a cached value
     */
    long hitCount();

    /**
     * Returns the number of times the lookup methods have returned either a cached or uncached
     * value.
     *
     * @return the number of lookups
     */
    long requestCount();
  }
}

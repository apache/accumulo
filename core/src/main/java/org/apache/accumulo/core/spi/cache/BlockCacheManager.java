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
package org.apache.accumulo.core.spi.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;

/**
 * @since 2.0.0
 */
public abstract class BlockCacheManager {

  private final Map<CacheType,BlockCache> caches = new HashMap<>();

  public static final String CACHE_PROPERTY_BASE = Property.TSERV_PREFIX + "cache.config.";

  public interface Configuration {

    /**
     * Before Accumulo's cache implementation was configurable, its built in caches had a
     * configurable size. These sizes were specified by the system properties
     * {@code tserver.cache.config.data.size}, {@code tserver.cache.config.index.size}, and {code
     * tserver.cache.config.summary.size}. This method returns the values of those settings. The
     * settings are made available, but cache implementations are under no obligation to use them.
     *
     */
    long getMaxSize(CacheType type);

    /**
     * Before Accumulo's cache implementation was configurable, its built in cache had a
     * configurable block size. This block size was specified by the system property
     * {@code tserver.default.blocksize}. This method returns the value of that setting. The setting
     * is made available, but cache implementations are under no obligation to use it.
     *
     */
    long getBlockSize();

    /**
     * This method provides a way for a cache implementation to access arbitrary configuration set
     * by a user.
     *
     * <p>
     * Returns all Accumulo properties that have a prefix of
     * {@code tserver.cache.config.<prefix>.<type>.} or
     * {@code tserver.cache.config.<prefix>.default.} with values for specific cache types
     * overriding defaults.
     *
     * <p>
     * For example assume the following data is in Accumulo's system config.
     *
     * <pre>
     * tserver.cache.config.lru.default.evictAfter=3600
     * tserver.cache.config.lru.default.loadFactor=.75
     * tserver.cache.config.lru.index.loadFactor=.55
     * tserver.cache.config.lru.data.loadFactor=.65
     * </pre>
     *
     * <p>
     * If this method is called with {@code prefix=lru} and {@code type=INDEX} then it would return
     * a map with the following key values. The load factor setting for index overrides the default
     * value.
     *
     * <pre>
     * evictAfter=3600
     * loadFactor=.55
     * </pre>
     *
     * @param prefix
     *          A unique identifier that corresponds to a particular BlockCacheManager
     *          implementation.
     */
    Map<String,String> getProperties(String prefix, CacheType type);
  }

  /**
   * Initialize the caches for each CacheType based on the configuration
   *
   * @param conf
   *          accumulo configuration
   */
  public void start(Configuration conf) {
    for (CacheType type : CacheType.values()) {
      BlockCache cache = this.createCache(conf, type);
      this.caches.put(type, cache);
    }
  }

  /**
   * Stop caches and release resources
   */
  public void stop() {
    this.caches.clear();
  }

  /**
   * Get the block cache of the given type
   *
   * @param type
   *          block cache type
   * @return BlockCache or null if not enabled
   */
  public BlockCache getBlockCache(CacheType type) {
    return caches.get(type);
  }

  /**
   * Create a block cache using the supplied configuration
   *
   * @param conf
   *          cache configuration
   * @return configured block cache
   */
  protected abstract BlockCache createCache(Configuration conf, CacheType type);

  /**
   * A convenience method that returns a string of the from
   * {@code tserver.cache.config.<prefix>.default.} this method is useful for configuring a cache
   * manager.
   *
   * @param prefix
   *          A unique identifier that corresponds to a particular BlockCacheManager implementation.
   * @see Configuration#getProperties(String, CacheType)
   */
  public static String getFullyQualifiedPropertyPrefix(String prefix) {
    return CACHE_PROPERTY_BASE + prefix + ".default.";
  }

  /**
   * A convenience method that returns a string of the from
   * {@code tserver.cache.config.<prefix>.<type>.} this method is useful for configuring a cache
   * manager.
   *
   * @param prefix
   *          A unique identifier that corresponds to a particular BlockCacheManager implementation.
   * @see Configuration#getProperties(String, CacheType)
   */
  public static String getFullyQualifiedPropertyPrefix(String prefix, CacheType type) {
    return CACHE_PROPERTY_BASE + prefix + "." + type.name().toLowerCase() + ".";
  }

}

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
package org.apache.accumulo.core.file.blockfile.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BlockCacheFactory<B extends BlockCache,C extends BlockCacheConfiguration> {

  public static final String CACHE_PROPERTY_BASE = Property.GENERAL_ARBITRARY_PROP_PREFIX + "cache.block.";

  private static final Logger LOG = LoggerFactory.getLogger(BlockCacheFactory.class);
  private static BlockCacheFactory<?,?> factory = null;

  private final Map<CacheType,B> caches = new HashMap<>();

  /**
   * Initialize the caches for each CacheType based on the configuration
   * 
   * @param conf
   *          accumulo configuration
   */
  public void start(AccumuloConfiguration conf) {
    for (CacheType type : CacheType.values()) {
      ConfigurationCopy props = type.getCacheProperties(conf, getCacheImplName());
      if (null != props) {
        C cc = this.createConfiguration(props, type, this);
        B cache = this.createCache(cc);
        LOG.info("Created {} cache with configuration {}", type, cc);
        this.caches.put(type, cache);
      }
    }
  }

  /**
   * Stop caches and release resources
   */
  public abstract void stop();

  /**
   * Get the block cache of the given type
   * 
   * @param type
   *          block cache type
   * @return BlockCache or null if not enabled
   */
  public B getBlockCache(CacheType type) {
    return caches.get(type);
  }

  /**
   * Parse and validate the configuration
   * 
   * @param conf
   *          accumulo configuration
   * @param type
   *          cache type
   * @param name
   *          cache implementation name
   * @return validated block cache configuration
   */
  protected abstract C createConfiguration(AccumuloConfiguration conf, CacheType type, BlockCacheFactory<B,C> factory);

  /**
   * Create a block cache using the supplied configuration
   * 
   * @param conf
   *          cache configuration
   * @return configured block cache
   */
  protected abstract B createCache(C conf);

  /**
   * Cache implementation name (e.g lru, tinylfu, etc)
   * 
   * @return name of cache implementation in lowercase
   */
  public abstract String getCacheImplName();

  /**
   * Get the BlockCacheFactory specified by the property 'tserver.cache.factory.class'
   * 
   * @param conf
   *          accumulo configuration
   * @return BlockCacheFactory instance
   * @throws Exception
   */
  public static synchronized BlockCacheFactory<?,?> getInstance(AccumuloConfiguration conf) throws Exception {
    if (null == factory) {
      String impl = conf.get(Property.TSERV_CACHE_FACTORY_IMPL);
      @SuppressWarnings("rawtypes")
      Class<? extends BlockCacheFactory> clazz = AccumuloVFSClassLoader.loadClass(impl, BlockCacheFactory.class);
      factory = (BlockCacheFactory<?,?>) clazz.newInstance();
      LOG.info("Created new block cache factory of type: {}", clazz.getSimpleName());
    }
    return factory;
  }

}

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

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockCacheManagerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(BlockCacheManager.class);

  /**
   * Get the BlockCacheFactory specified by the property 'tserver.cache.factory.class' using the
   * AccumuloVFSClassLoader
   *
   * @param conf accumulo configuration
   * @return block cache manager instance
   * @throws Exception error loading block cache manager implementation class
   */
  public static synchronized BlockCacheManager getInstance(AccumuloConfiguration conf)
      throws Exception {
    String impl = conf.get(Property.TSERV_CACHE_MANAGER_IMPL);
    Class<? extends BlockCacheManager> clazz =
        ClassLoaderUtil.loadClass(impl, BlockCacheManager.class);
    LOG.info("Created new block cache manager of type: {}", clazz.getSimpleName());
    return clazz.getDeclaredConstructor().newInstance();
  }

  /**
   * Get the BlockCacheFactory specified by the property 'tserver.cache.factory.class'
   *
   * @param conf accumulo configuration
   * @return block cache manager instance
   * @throws Exception error loading block cache manager implementation class
   */
  public static synchronized BlockCacheManager getClientInstance(AccumuloConfiguration conf)
      throws Exception {
    String impl = conf.get(Property.TSERV_CACHE_MANAGER_IMPL);
    Class<? extends BlockCacheManager> clazz =
        Class.forName(impl).asSubclass(BlockCacheManager.class);
    LOG.info("Created new block cache factory of type: {}", clazz.getSimpleName());
    return clazz.getDeclaredConstructor().newInstance();
  }
}

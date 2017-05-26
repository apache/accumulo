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

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheManagerFactory;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheManager;
import org.apache.accumulo.core.file.blockfile.cache.tinylfu.TinyLfuBlockCacheManager;
import org.junit.Assert;
import org.junit.Test;

public class BlockCacheFactoryTest {

  @Test
  public void testCreateLruBlockCacheFactory() throws Exception {
    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(dc);
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    BlockCacheManagerFactory.getInstance(cc);
  }

  @Test
  public void testCreateTinyLfuBlockCacheFactory() throws Exception {
    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(dc);
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, TinyLfuBlockCacheManager.class.getName());
    BlockCacheManagerFactory.getInstance(cc);
  }

  @Test
  public void testStartWithDefault() throws Exception {
    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    BlockCacheManager manager = BlockCacheManagerFactory.getInstance(dc);
    manager.start(new BlockCacheConfiguration(dc));
    Assert.assertNotNull(manager.getBlockCache(CacheType.INDEX));
  }
}

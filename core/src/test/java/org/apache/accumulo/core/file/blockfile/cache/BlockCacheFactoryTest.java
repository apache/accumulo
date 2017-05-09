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
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheFactory;
import org.apache.accumulo.core.file.blockfile.cache.tinylfu.TinyLfuBlockCacheFactory;
import org.junit.Assert;
import org.junit.Test;

public class BlockCacheFactoryTest {

  @Test
  public void testCreateLruBlockCacheFactory() throws Exception {
    ConfigurationCopy cc = new ConfigurationCopy();
    cc.set(Property.TSERV_CACHE_FACTORY_IMPL, LruBlockCacheFactory.class.getName());
    BlockCacheFactory.getInstance(cc);
  }

  @Test
  public void testCreateTinyLfuBlockCacheFactory() throws Exception {
    ConfigurationCopy cc = new ConfigurationCopy();
    cc.set(Property.TSERV_CACHE_FACTORY_IMPL, TinyLfuBlockCacheFactory.class.getName());
    BlockCacheFactory.getInstance(cc);
  }

  @Test
  public void testStart() throws Exception {
    ConfigurationCopy cc = new ConfigurationCopy();
    cc.set(Property.TSERV_CACHE_FACTORY_IMPL, LruBlockCacheFactory.class.getName());
    BlockCacheFactory<?,?> factory = BlockCacheFactory.getInstance(cc);
    String indexPrefix = CacheType.INDEX.getPropertyPrefix(factory.getCacheImplName());
    cc.setIfAbsent(indexPrefix + CacheType.ENABLED_SUFFIX, Boolean.TRUE.toString());
    cc.setIfAbsent(indexPrefix + BlockCacheConfiguration.BLOCK_SIZE_PROPERTY, Long.toString(2048));
    cc.setIfAbsent(indexPrefix + BlockCacheConfiguration.MAX_SIZE_PROPERTY, Long.toString(1048576));
    factory.start(cc);
    Assert.assertNotNull(factory.getBlockCache(CacheType.INDEX));
  }
}

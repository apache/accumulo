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
import org.junit.Assert;
import org.junit.Test;

public class CacheTypeTest {

  @Test
  public void testGetPropertyPrefix() throws Exception {
    Assert.assertEquals("general.custom.cache.block.lru.data.", CacheType.DATA.getPropertyPrefix("lru"));
  }

  @Test
  public void testCacheEnabled() {
    ConfigurationCopy cc = new ConfigurationCopy();
    String indexPrefix = CacheType.INDEX.getPropertyPrefix("lru");
    cc.setIfAbsent(indexPrefix + CacheType.ENABLED_SUFFIX, Boolean.TRUE.toString());
    cc.setIfAbsent(indexPrefix + BlockCacheConfiguration.BLOCK_SIZE_PROPERTY, Long.toString(100000));
    cc.setIfAbsent(indexPrefix + BlockCacheConfiguration.MAX_SIZE_PROPERTY, Long.toString(100000000));
    ConfigurationCopy cc2 = CacheType.INDEX.getCacheProperties(cc, "lru");
    Assert.assertNotNull(cc2);
    Assert.assertEquals(Boolean.TRUE.toString(), cc2.get(indexPrefix + CacheType.ENABLED_SUFFIX));
    Assert.assertEquals(Long.toString(100000), cc2.get(indexPrefix + BlockCacheConfiguration.BLOCK_SIZE_PROPERTY));
    Assert.assertEquals(Long.toString(100000000), cc2.get(indexPrefix + BlockCacheConfiguration.MAX_SIZE_PROPERTY));
  }

  @Test
  public void testCacheDisabled() {
    ConfigurationCopy cc = new ConfigurationCopy();
    String indexPrefix = CacheType.INDEX.getPropertyPrefix("lru");
    cc.setIfAbsent(indexPrefix + CacheType.ENABLED_SUFFIX, Boolean.FALSE.toString());
    ConfigurationCopy cc2 = CacheType.INDEX.getCacheProperties(cc, "lru");
    Assert.assertNull(cc2);
  }

}

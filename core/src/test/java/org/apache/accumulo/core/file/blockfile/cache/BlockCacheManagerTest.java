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

import org.junit.Assert;
import org.junit.Test;

public class BlockCacheManagerTest {

  @Test
  public void testGetPropertyPrefix() throws Exception {
    Assert.assertEquals("tserver.cache.config.lru.data.", BlockCacheManager.getFullyQualifiedPropertyPrefix("lru", CacheType.DATA));
    Assert.assertEquals("tserver.cache.config.lru.index.", BlockCacheManager.getFullyQualifiedPropertyPrefix("lru", CacheType.INDEX));
    Assert.assertEquals("tserver.cache.config.lru.summary.", BlockCacheManager.getFullyQualifiedPropertyPrefix("lru", CacheType.SUMMARY));
    Assert.assertEquals("tserver.cache.config.lru.default.", BlockCacheManager.getFullyQualifiedPropertyPrefix("lru"));
  }

}

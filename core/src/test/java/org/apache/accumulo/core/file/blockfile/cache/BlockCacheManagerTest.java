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
package org.apache.accumulo.core.file.blockfile.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.junit.jupiter.api.Test;

public class BlockCacheManagerTest {

  @Test
  @SuppressWarnings("deprecation")
  public void testGetPropertyPrefix() {
    assertEquals("tserver.cache.config.lru.data.",
        BlockCacheManager.getFullyQualifiedPropertyPrefix("lru", CacheType.DATA));
    assertEquals("tserver.cache.config.lru.index.",
        BlockCacheManager.getFullyQualifiedPropertyPrefix("lru", CacheType.INDEX));
    assertEquals("tserver.cache.config.lru.summary.",
        BlockCacheManager.getFullyQualifiedPropertyPrefix("lru", CacheType.SUMMARY));
    assertEquals("tserver.cache.config.lru.default.",
        BlockCacheManager.getFullyQualifiedPropertyPrefix("lru"));
  }

}

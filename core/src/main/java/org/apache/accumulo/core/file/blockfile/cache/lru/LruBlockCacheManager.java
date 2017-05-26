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

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LruBlockCacheManager extends BlockCacheManager {

  private static final Logger LOG = LoggerFactory.getLogger(LruBlockCacheManager.class);

  @Override
  protected BlockCache createCache(Configuration conf, CacheType type) {
    LruBlockCacheConfiguration cc = new LruBlockCacheConfiguration(conf, type);
    LOG.info("Creating {} cache with configuration {}", type, cc);
    return new LruBlockCache(cc);
  }

  @Override
  public void stop() {
    for (CacheType type : CacheType.values()) {
      LruBlockCache cache = ((LruBlockCache) this.getBlockCache(type));
      if (null != cache) {
        cache.shutdown();
      }
    }
    super.stop();
  }

}

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
package org.apache.accumulo.core.file.blockfile.impl;

import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;

public class OpportunisticBlockCache implements BlockCache {

  private BlockCache cache;

  public OpportunisticBlockCache(BlockCache cache) {
    this.cache = cache;
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return null;
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    return cache.getBlock(blockName);
  }

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    return cache.getBlock(blockName);
  }

  @Override
  public long getMaxHeapSize() {
    return cache.getMaxHeapSize();
  }

  @Override
  public long getMaxSize() {
    return cache.getMaxSize();
  }

  @Override
  public Stats getStats() {
    return cache.getStats();
  }

}

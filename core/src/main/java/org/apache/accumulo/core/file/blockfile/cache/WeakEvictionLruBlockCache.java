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

import java.util.Map;

import com.google.common.collect.MapMaker;

public class WeakEvictionLruBlockCache extends LruBlockCache {

  private final Map<String,CachedBlock> weakEvictionMap = new MapMaker().weakValues().concurrencyLevel(DEFAULT_CONCURRENCY_LEVEL).makeMap();

  public WeakEvictionLruBlockCache(long maxSize, long blockSize, boolean evictionThread) {
    super(maxSize, blockSize, evictionThread);
  }

  public WeakEvictionLruBlockCache(long maxSize, long blockSize) {
    super(maxSize, blockSize);
  }

  @Override
  public CachedBlock getBlock(String blockName) {
    CachedBlock cb = super.getBlock(blockName);
    if (cb != null)
      return cb;
    return weakEvictionMap.get(blockName);
  }

  @Override
  protected long evictBlock(CachedBlock block) {
    weakEvictionMap.put(block.getName(), block);
    return super.evictBlock(block);
  }
}

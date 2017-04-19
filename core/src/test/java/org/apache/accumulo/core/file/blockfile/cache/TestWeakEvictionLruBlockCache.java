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
import java.util.Map.Entry;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestWeakEvictionLruBlockCache {

  @Test
  public void testWeakEviction() {
    // create a cache
    WeakEvictionLruBlockCache cache = new WeakEvictionLruBlockCache(1000, 100, false);
    // add more things than will fit
    Map<String,byte[]> blocks = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      String key = "" + i;
      byte[] buf = new byte[200];
      cache.cacheBlock(key, buf);
      blocks.put(key, buf);
    }
    // force eviction
    cache.evict();

    assertTrue("expected at least something to get evicted", cache.getEvictedCount() > 0);
    // read
    for (Entry<String,byte[]> entry : blocks.entrySet()) {
      CachedBlock cachedBlock = cache.getBlock(entry.getKey());
      assertTrue("expected evicted blocks to still be in weak reference map, since we're holding onto a reference in this test", cachedBlock != null
          && cachedBlock.getBuffer() == entry.getValue());
    }
  }

}

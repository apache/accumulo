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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.accumulo.core.file.blockfile.cache.impl.CompressedBlockCache;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CompressedBlockCacheTest {
  private static class MapBlockCache implements BlockCache {
    private final ConcurrentHashMap<String,byte[]> map = new ConcurrentHashMap<>();

    @Override
    public CacheEntry cacheBlock(String blockName, byte[] buf) {
      map.put(blockName, buf.clone());
      return wrap(buf);
    }

    @Override
    public CacheEntry getBlock(String blockName) {
      byte[] data = map.get(blockName);
      return data == null ? null : wrap(data);
    }

    @Override
    public CacheEntry getBlock(String blockName, Loader loader) {
      byte[] data = map.computeIfAbsent(blockName, k -> {
        byte[] loaded = loader.load(Integer.MAX_VALUE, Collections.emptyMap());
        return loaded;
      });
      return data == null ? null : wrap(data);
    }

    private CacheEntry wrap(byte[] data) {
      return new CacheEntry() {
        @Override
        public byte[] getBuffer() {
          return data;
        }

        @Override
        public <T extends Weighable> T getIndex(Supplier<T> supplier) {
          return null;
        }

        @Override
        public void indexWeightChanged() {}
      };
    }

    @Override
    public long getMaxHeapSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public long getMaxSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public Stats getStats() {
      return new Stats() {
        @Override
        public long hitCount() {
          return 0;
        }

        @Override
        public long requestCount() {
          return 0;
        }

        @Override
        public long evictionCount() {
          return 0;
        }
      };
    }

    int size() {
      return map.size();
    }

    byte[] getRaw(String key) {
      return map.get(key);
    }
  }

  private MapBlockCache underlying;
  private CompressedBlockCache cache;

  @BeforeEach
  public void setUp() {
    underlying = new MapBlockCache();
    cache = new CompressedBlockCache(underlying);
  }

  @Test
  public void testCacheAndRetrieve() {
    byte[] data = new byte[1000];
    Arrays.fill(data, (byte) 'A');

    CacheEntry stored = cache.cacheBlock("block1", data);
    assertNotNull(stored);
    assertArrayEquals(data, stored.getBuffer());

    byte[] compressed = underlying.getRaw("block1");
    assertNotNull(compressed);
    assert compressed.length < data.length
        : "Expected compressed size < original for repetitive data";
  }

  @Test
  public void testRetrieveMiss() {
    CacheEntry ce = cache.getBlock("nonexistent");
    assertNull(ce);
  }

  @Test
  public void testRetrieveHit() {
    byte[] data = new byte[500];
    Arrays.fill(data, (byte) 'Z');

    cache.cacheBlock("blockZ", data);

    CacheEntry ce = cache.getBlock("blockZ");
    assertNotNull(ce);
    assertArrayEquals(data, ce.getBuffer());
  }

  @Test
  public void testGetBlockWithLoader() {
    byte[] data = new byte[800];
    Arrays.fill(data, (byte) 0x42);

    BlockCache.Loader loader = new BlockCache.Loader() {
      @Override
      public Map<String,BlockCache.Loader> getDependencies() {
        return Collections.emptyMap();
      }

      @Override
      public byte[] load(int maxSize, Map<String,byte[]> dependencies) {
        return data.clone();
      }
    };

    CacheEntry ce = cache.getBlock("blockLoader", loader);
    assertNotNull(ce);
    assertArrayEquals(data, ce.getBuffer());
    assert underlying.size() == 1 : "Expected one entry in underlying cache";

    CacheEntry ce2 = cache.getBlock("blockLoader", loader);
    assertNotNull(ce2);
    assertArrayEquals(data, ce2.getBuffer());
  }

  @Test
  public void testGetIndexReturnsNull() {
    byte[] data = new byte[100];
    cache.cacheBlock("blockIdx", data);

    CacheEntry ce = cache.getBlock("blockIdx");
    assertNotNull(ce);
    assertNull(ce.getIndex(() -> null));
  }

  @Test
  public void testLoaderNullReturnIsHandled() {
    BlockCache.Loader nullLoader = new BlockCache.Loader() {
      @Override
      public Map<String,BlockCache.Loader> getDependencies() {
        return Collections.emptyMap();
      }

      @Override
      public byte[] load(int maxSize, Map<String,byte[]> dependencies) {
        return null;
      }
    };

    CacheEntry ce = cache.getBlock("blockNull", nullLoader);
    assertNull(ce);
    assert underlying.size() == 0 : "No entries should be stored when loader returns null";
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    final int THREADS = 20;
    final int OPS_PER_THREAD = 50;
    byte[] data = new byte[256];
    Arrays.fill(data, (byte) 0xFF);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; i++) {
      final String key = "block-" + (i % 5);
      threads[i] = new Thread(() -> {
        for (int j = 0; j < OPS_PER_THREAD; j++) {
          cache.cacheBlock(key, data);
          CacheEntry ce = cache.getBlock(key);
          if (ce != null) {
            assertArrayEquals(data, ce.getBuffer());
          }
        }
      });
    }

    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    // If we reach here without exceptions, thread safety is satisfied
  }
}

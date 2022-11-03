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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheManagerFactory;
import org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize;
import org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock;
import org.apache.accumulo.core.file.blockfile.cache.lru.HeapSize;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCache;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheManager;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.junit.jupiter.api.Test;

/**
 * Tests the concurrent LruBlockCache.
 * <p>
 *
 * Tests will ensure it grows and shrinks in size properly, evictions run when they're supposed to
 * and do what they should, and that cached blocks are accessible when expected to be.
 */
public class TestLruBlockCache {

  private static final SecureRandom random = new SecureRandom();

  @Test
  public void testConfiguration() {
    ConfigurationCopy cc = new ConfigurationCopy();
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(1019));
    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(1000023));
    cc.set(Property.TSERV_DATACACHE_SIZE, Long.toString(1000027));
    cc.set(Property.TSERV_SUMMARYCACHE_SIZE, Long.toString(1000029));

    LruBlockCacheConfiguration.builder(Property.TSERV_PREFIX, CacheType.INDEX)
        .useEvictionThread(false).minFactor(0.93f).acceptableFactor(0.97f).singleFactor(0.20f)
        .multiFactor(0.30f).memoryFactor(0.50f).mapConcurrencyLevel(5).buildMap().forEach(cc::set);

    String defaultPrefix = BlockCacheConfiguration.getCachePropertyBase(Property.TSERV_PREFIX)
        + LruBlockCacheConfiguration.PROPERTY_PREFIX + ".default.";

    // this should be overridden by cache type specific setting
    cc.set(defaultPrefix + LruBlockCacheConfiguration.MEMORY_FACTOR_PROPERTY, "0.6");

    // this is not set for the cache type, so should fall back to default
    cc.set(defaultPrefix + LruBlockCacheConfiguration.MAP_LOAD_PROPERTY, "0.53");

    BlockCacheConfiguration bcc = BlockCacheConfiguration.forTabletServer(cc);
    LruBlockCacheConfiguration lbcc = new LruBlockCacheConfiguration(bcc, CacheType.INDEX);

    assertFalse(lbcc.isUseEvictionThread());
    assertEquals(0.93f, lbcc.getMinFactor(), 0.0000001);
    assertEquals(0.97f, lbcc.getAcceptableFactor(), 0.0000001);
    assertEquals(0.20f, lbcc.getSingleFactor(), 0.0000001);
    assertEquals(0.30f, lbcc.getMultiFactor(), 0.0000001);
    assertEquals(0.50f, lbcc.getMemoryFactor(), 0.0000001);
    assertEquals(0.53f, lbcc.getMapLoadFactor(), 0.0000001);
    assertEquals(5, lbcc.getMapConcurrencyLevel());
    assertEquals(1019, lbcc.getBlockSize());
    assertEquals(1000023, lbcc.getMaxSize());
  }

  @Test
  public void testBackgroundEvictionThread() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 9); // room for 9, will evict

    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(dc);
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    BlockCacheManager manager = BlockCacheManagerFactory.getInstance(cc);
    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(blockSize));
    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(maxSize));
    manager.start(BlockCacheConfiguration.forTabletServer(cc));
    LruBlockCache cache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);

    Block[] blocks = generateFixedBlocks(10, blockSize, "block");

    // Add all the blocks
    for (Block block : blocks) {
      cache.cacheBlock(block.blockName, block.buf);
    }

    // Let the eviction run
    int n = 0;
    while (cache.getEvictionCount() == 0) {
      Thread.sleep(1000);
      assertTrue(n++ < 1);
    }
    // A single eviction run should have occurred
    assertEquals(cache.getEvictionCount(), 1);

    manager.stop();
  }

  @Test
  public void testCacheSimple() throws Exception {

    long maxSize = 1000000;
    long blockSize = calculateBlockSizeDefault(maxSize, 101);

    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(dc);
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    BlockCacheManager manager = BlockCacheManagerFactory.getInstance(cc);
    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(blockSize));
    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(maxSize));
    manager.start(BlockCacheConfiguration.forTabletServer(cc));
    LruBlockCache cache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);

    Block[] blocks = generateRandomBlocks(100, blockSize);

    long expectedCacheSize = cache.heapSize();

    // Confirm empty
    for (Block block : blocks) {
      assertNull(cache.getBlock(block.blockName));
    }

    // Add blocks
    for (Block block : blocks) {
      cache.cacheBlock(block.blockName, block.buf);
      expectedCacheSize += block.heapSize();
    }

    // Verify correctly calculated cache heap size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Check if all blocks are properly cached and retrieved
    for (Block block : blocks) {
      CacheEntry ce = cache.getBlock(block.blockName);
      assertNotNull(ce);
      assertEquals(ce.getBuffer().length, block.buf.length);
    }

    // Verify correctly calculated cache heap size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Check if all blocks are properly cached and retrieved
    for (Block block : blocks) {
      CacheEntry ce = cache.getBlock(block.blockName);
      assertNotNull(ce);
      assertEquals(ce.getBuffer().length, block.buf.length);
    }

    // Expect no evictions
    assertEquals(0, cache.getEvictionCount());
    // Thread t = new LruBlockCache.StatisticsThread(cache);
    // t.start();
    // t.join();
    manager.stop();
  }

  @Test
  public void testCacheEvictionSimple() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 10);

    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(dc);
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    BlockCacheManager manager = BlockCacheManagerFactory.getInstance(cc);
    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(blockSize));
    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(maxSize));
    LruBlockCacheConfiguration.builder(Property.TSERV_PREFIX, CacheType.INDEX)
        .useEvictionThread(false).buildMap().forEach(cc::set);
    manager.start(BlockCacheConfiguration.forTabletServer(cc));

    LruBlockCache cache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);

    Block[] blocks = generateFixedBlocks(10, blockSize, "block");

    long expectedCacheSize = cache.heapSize();

    // Add all the blocks
    for (Block block : blocks) {
      cache.cacheBlock(block.blockName, block.buf);
      expectedCacheSize += block.heapSize();
    }

    // A single eviction run should have occurred
    assertEquals(1, cache.getEvictionCount());

    // Our expected size overruns acceptable limit
    assertTrue(
        expectedCacheSize > (maxSize * LruBlockCacheConfiguration.DEFAULT_ACCEPTABLE_FACTOR));

    // But the cache did not grow beyond max
    assertTrue(cache.heapSize() < maxSize);

    // And is still below the acceptable limit
    assertTrue(cache.heapSize() < (maxSize * LruBlockCacheConfiguration.DEFAULT_ACCEPTABLE_FACTOR));

    // All blocks except block 0 and 1 should be in the cache
    assertNull(cache.getBlock(blocks[0].blockName));
    assertNull(cache.getBlock(blocks[1].blockName));
    for (int i = 2; i < blocks.length; i++) {
      assertArrayEquals(cache.getBlock(blocks[i].blockName).getBuffer(), blocks[i].buf);
    }
    manager.stop();
  }

  @Test
  public void testCacheEvictionTwoPriorities() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 10);

    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(dc);
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    BlockCacheManager manager = BlockCacheManagerFactory.getInstance(cc);
    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(blockSize));
    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(maxSize));
    LruBlockCacheConfiguration.builder(Property.TSERV_PREFIX, CacheType.INDEX)
        .useEvictionThread(false).minFactor(0.98f).acceptableFactor(0.99f).singleFactor(0.25f)
        .multiFactor(0.50f).memoryFactor(0.25f).buildMap().forEach(cc::set);
    manager.start(BlockCacheConfiguration.forTabletServer(cc));
    LruBlockCache cache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);

    Block[] singleBlocks = generateFixedBlocks(5, 10000, "single");
    Block[] multiBlocks = generateFixedBlocks(5, 10000, "multi");

    long expectedCacheSize = cache.heapSize();

    // Add and get the multi blocks
    for (Block block : multiBlocks) {
      cache.cacheBlock(block.blockName, block.buf);
      expectedCacheSize += block.heapSize();
      assertArrayEquals(cache.getBlock(block.blockName).getBuffer(), block.buf);
    }

    // Add the single blocks (no get)
    for (Block block : singleBlocks) {
      cache.cacheBlock(block.blockName, block.buf);
      expectedCacheSize += block.heapSize();
    }

    // A single eviction run should have occurred
    assertEquals(cache.getEvictionCount(), 1);

    // We expect two entries evicted
    assertEquals(cache.getEvictedCount(), 2);

    // Our expected size overruns acceptable limit
    assertTrue(
        expectedCacheSize > (maxSize * LruBlockCacheConfiguration.DEFAULT_ACCEPTABLE_FACTOR));

    // But the cache did not grow beyond max
    assertTrue(cache.heapSize() <= maxSize);

    // And is now below the acceptable limit
    assertTrue(
        cache.heapSize() <= (maxSize * LruBlockCacheConfiguration.DEFAULT_ACCEPTABLE_FACTOR));

    // We expect fairness across the two priorities.
    // This test makes multi go barely over its limit, in-memory
    // empty, and the rest in single. Two single evictions and
    // one multi eviction expected.
    assertNull(cache.getBlock(singleBlocks[0].blockName));
    assertNull(cache.getBlock(multiBlocks[0].blockName));

    // And all others to be cached
    for (int i = 1; i < 4; i++) {
      assertArrayEquals(cache.getBlock(singleBlocks[i].blockName).getBuffer(), singleBlocks[i].buf);
      assertArrayEquals(cache.getBlock(multiBlocks[i].blockName).getBuffer(), multiBlocks[i].buf);
    }
    manager.stop();
  }

  @Test
  public void testCacheEvictionThreePriorities() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(dc);
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    BlockCacheManager manager = BlockCacheManagerFactory.getInstance(cc);
    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(blockSize));
    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(maxSize));
    LruBlockCacheConfiguration.builder(Property.TSERV_PREFIX, CacheType.INDEX)
        .useEvictionThread(false).minFactor(0.98f).acceptableFactor(0.99f).singleFactor(0.33f)
        .multiFactor(0.33f).memoryFactor(0.34f).buildMap().forEach(cc::set);
    manager.start(BlockCacheConfiguration.forTabletServer(cc));
    LruBlockCache cache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);

    Block[] singleBlocks = generateFixedBlocks(5, blockSize, "single");
    Block[] multiBlocks = generateFixedBlocks(5, blockSize, "multi");
    Block[] memoryBlocks = generateFixedBlocks(5, blockSize, "memory");

    long expectedCacheSize = cache.heapSize();

    // Add 3 blocks from each priority
    for (int i = 0; i < 3; i++) {

      // Just add single blocks
      cache.cacheBlock(singleBlocks[i].blockName, singleBlocks[i].buf);
      expectedCacheSize += singleBlocks[i].heapSize();

      // Add and get multi blocks
      cache.cacheBlock(multiBlocks[i].blockName, multiBlocks[i].buf);
      expectedCacheSize += multiBlocks[i].heapSize();
      cache.getBlock(multiBlocks[i].blockName);

      // Add memory blocks as such
      cache.cacheBlock(memoryBlocks[i].blockName, memoryBlocks[i].buf, true);
      expectedCacheSize += memoryBlocks[i].heapSize();

    }

    // Do not expect any evictions yet
    assertEquals(0, cache.getEvictionCount());

    // Verify cache size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Insert a single block, oldest single should be evicted
    cache.cacheBlock(singleBlocks[3].blockName, singleBlocks[3].buf);

    // Single eviction, one thing evicted
    assertEquals(1, cache.getEvictionCount());
    assertEquals(1, cache.getEvictedCount());

    // Verify oldest single block is the one evicted
    assertNull(cache.getBlock(singleBlocks[0].blockName));

    // Change the oldest remaining single block to a multi
    cache.getBlock(singleBlocks[1].blockName);

    // Insert another single block
    cache.cacheBlock(singleBlocks[4].blockName, singleBlocks[4].buf);

    // Two evictions, two evicted.
    assertEquals(2, cache.getEvictionCount());
    assertEquals(2, cache.getEvictedCount());

    // Oldest multi block should be evicted now
    assertNull(cache.getBlock(multiBlocks[0].blockName));

    // Insert another memory block
    cache.cacheBlock(memoryBlocks[3].blockName, memoryBlocks[3].buf, true);

    // Three evictions, three evicted.
    assertEquals(3, cache.getEvictionCount());
    assertEquals(3, cache.getEvictedCount());

    // Oldest memory block should be evicted now
    assertNull(cache.getBlock(memoryBlocks[0].blockName));

    // Add a block that is twice as big (should force two evictions)
    Block[] bigBlocks = generateFixedBlocks(3, blockSize * 3, "big");
    cache.cacheBlock(bigBlocks[0].blockName, bigBlocks[0].buf);

    // Four evictions, six evicted (inserted block 3X size, expect +3 evicted)
    assertEquals(4, cache.getEvictionCount());
    assertEquals(6, cache.getEvictedCount());

    // Expect three remaining singles to be evicted
    assertNull(cache.getBlock(singleBlocks[2].blockName));
    assertNull(cache.getBlock(singleBlocks[3].blockName));
    assertNull(cache.getBlock(singleBlocks[4].blockName));

    // Make the big block a multi block
    cache.getBlock(bigBlocks[0].blockName);

    // Cache another single big block
    cache.cacheBlock(bigBlocks[1].blockName, bigBlocks[1].buf);

    // Five evictions, nine evicted (3 new)
    assertEquals(5, cache.getEvictionCount());
    assertEquals(9, cache.getEvictedCount());

    // Expect three remaining multis to be evicted
    assertNull(cache.getBlock(singleBlocks[1].blockName));
    assertNull(cache.getBlock(multiBlocks[1].blockName));
    assertNull(cache.getBlock(multiBlocks[2].blockName));

    // Cache a big memory block
    cache.cacheBlock(bigBlocks[2].blockName, bigBlocks[2].buf, true);

    // Six evictions, twelve evicted (3 new)
    assertEquals(6, cache.getEvictionCount());
    assertEquals(12, cache.getEvictedCount());

    // Expect three remaining in-memory to be evicted
    assertNull(cache.getBlock(memoryBlocks[1].blockName));
    assertNull(cache.getBlock(memoryBlocks[2].blockName));
    assertNull(cache.getBlock(memoryBlocks[3].blockName));

    manager.stop();
  }

  // test scan resistance
  @Test
  public void testScanResistance() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    DefaultConfiguration dc = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(dc);
    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
    BlockCacheManager manager = BlockCacheManagerFactory.getInstance(cc);
    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(blockSize));
    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(maxSize));
    LruBlockCacheConfiguration.builder(Property.TSERV_PREFIX, CacheType.INDEX)
        .useEvictionThread(false).minFactor(0.66f).acceptableFactor(0.99f).singleFactor(0.33f)
        .multiFactor(0.33f).memoryFactor(0.34f).buildMap().forEach(cc::set);
    manager.start(BlockCacheConfiguration.forTabletServer(cc));
    LruBlockCache cache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);

    Block[] singleBlocks = generateFixedBlocks(20, blockSize, "single");
    Block[] multiBlocks = generateFixedBlocks(5, blockSize, "multi");

    // Add 5 multi blocks
    for (Block block : multiBlocks) {
      cache.cacheBlock(block.blockName, block.buf);
      cache.getBlock(block.blockName);
    }

    // Add 5 single blocks
    for (int i = 0; i < 5; i++) {
      cache.cacheBlock(singleBlocks[i].blockName, singleBlocks[i].buf);
    }

    // An eviction ran
    assertEquals(1, cache.getEvictionCount());

    // To drop down to 2/3 capacity, we'll need to evict 4 blocks
    assertEquals(4, cache.getEvictedCount());

    // Should have been taken off equally from single and multi
    assertNull(cache.getBlock(singleBlocks[0].blockName));
    assertNull(cache.getBlock(singleBlocks[1].blockName));
    assertNull(cache.getBlock(multiBlocks[0].blockName));
    assertNull(cache.getBlock(multiBlocks[1].blockName));

    // Let's keep "scanning" by adding single blocks. From here on we only
    // expect evictions from the single bucket.

    // Every time we reach 10 total blocks (every 4 inserts) we get 4 single
    // blocks evicted. Inserting 13 blocks should yield 3 more evictions and
    // 12 more evicted.

    for (int i = 5; i < 18; i++) {
      cache.cacheBlock(singleBlocks[i].blockName, singleBlocks[i].buf);
    }

    // 4 total evictions, 16 total evicted
    assertEquals(4, cache.getEvictionCount());
    assertEquals(16, cache.getEvictedCount());

    // Should now have 7 total blocks
    assertEquals(7, cache.size());

    manager.stop();
  }

  private Block[] generateFixedBlocks(int numBlocks, int size, String pfx) {
    Block[] blocks = new Block[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] = new Block(pfx + i, size);
    }
    return blocks;
  }

  private Block[] generateFixedBlocks(int numBlocks, long size, String pfx) {
    return generateFixedBlocks(numBlocks, (int) size, pfx);
  }

  private Block[] generateRandomBlocks(int numBlocks, long maxSize) {
    Block[] blocks = new Block[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] = new Block("block" + i, random.nextInt((int) maxSize) + 1);
    }
    return blocks;
  }

  private long calculateBlockSize(long maxSize, int numBlocks) {
    long roughBlockSize = maxSize / numBlocks;
    int numEntries = (int) Math.ceil((1.2) * maxSize / roughBlockSize);
    long totalOverhead = LruBlockCache.CACHE_FIXED_OVERHEAD + ClassSize.CONCURRENT_HASHMAP
        + ((long) numEntries * ClassSize.CONCURRENT_HASHMAP_ENTRY)
        + ((long) LruBlockCacheConfiguration.DEFAULT_CONCURRENCY_LEVEL
            * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
    long negateBlockSize = totalOverhead / numEntries;
    negateBlockSize += CachedBlock.PER_BLOCK_OVERHEAD;
    return ClassSize.align((long) Math.floor((roughBlockSize - negateBlockSize) * 0.99f));
  }

  private long calculateBlockSizeDefault(long maxSize, int numBlocks) {
    long roughBlockSize = maxSize / numBlocks;
    int numEntries = (int) Math.ceil((1.2) * maxSize / roughBlockSize);
    long totalOverhead = LruBlockCache.CACHE_FIXED_OVERHEAD + ClassSize.CONCURRENT_HASHMAP
        + ((long) numEntries * ClassSize.CONCURRENT_HASHMAP_ENTRY)
        + ((long) LruBlockCacheConfiguration.DEFAULT_CONCURRENCY_LEVEL
            * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
    long negateBlockSize = totalOverhead / numEntries;
    negateBlockSize += CachedBlock.PER_BLOCK_OVERHEAD;
    return ClassSize.align((long) Math.floor(
        (roughBlockSize - negateBlockSize) * LruBlockCacheConfiguration.DEFAULT_ACCEPTABLE_FACTOR));
  }

  private static class Block implements HeapSize {
    String blockName;
    byte[] buf;

    Block(String blockName, int size) {
      this.blockName = blockName;
      this.buf = new byte[size];
    }

    @Override
    public long heapSize() {
      return CachedBlock.PER_BLOCK_OVERHEAD + ClassSize.align(blockName.length())
          + ClassSize.align(buf.length);
    }
  }
}

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
package org.apache.accumulo.core.file.blockfile.cache.lru;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize.CONCURRENT_HASHMAP;
import static org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize.CONCURRENT_HASHMAP_ENTRY;
import static org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize.CONCURRENT_HASHMAP_SEGMENT;

import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize;
import org.apache.accumulo.core.file.blockfile.cache.impl.SizeConstants;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads.AccumuloDaemonThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A block cache implementation that is memory-aware using {@link HeapSize}, memory-bound using an
 * LRU eviction algorithm, and concurrent: backed by a {@link ConcurrentHashMap} and with a
 * non-blocking eviction thread giving constant-time {@link #cacheBlock} and {@link #getBlock}
 * operations.
 *
 * <p>
 * Contains three levels of block priority to allow for scan-resistance and in-memory families. A
 * block is added with an inMemory flag if necessary, otherwise a block becomes a single access
 * priority. Once a blocked is accessed again, it changes to multiple access. This is used to
 * prevent scans from thrashing the cache, adding a least-frequently-used element to the eviction
 * algorithm.
 *
 * <p>
 * Each priority is given its own chunk of the total cache to ensure fairness during eviction. Each
 * priority will retain close to its maximum size, however, if any priority is not using its entire
 * chunk the others are able to grow beyond their chunk size.
 *
 * <p>
 * Instantiated at a minimum with the total size and average block size. All sizes are in bytes. The
 * block size is not especially important as this cache is fully dynamic in its sizing of blocks. It
 * is only used for pre-allocating data structures and in initial heap estimation of the map.
 *
 * <p>
 * The detailed constructor defines the sizes for the three priorities (they should total to the
 * maximum size defined). It also sets the levels that trigger and control the eviction thread.
 *
 * <p>
 * The acceptable size is the cache size level which triggers the eviction process to start. It
 * evicts enough blocks to get the size below the minimum size specified.
 *
 * <p>
 * Eviction happens in a separate thread and involves a single full-scan of the map. It determines
 * how many bytes must be freed to reach the minimum size, and then while scanning determines the
 * fewest least-recently-used blocks necessary from each of the three priorities (would be 3 times
 * bytes to free). It then uses the priority chunk sizes to evict fairly according to the relative
 * sizes and usage.
 */
public class LruBlockCache extends SynchronousLoadingBlockCache implements BlockCache, HeapSize {

  private static final Logger log = LoggerFactory.getLogger(LruBlockCache.class);

  /** Statistics thread */
  static final int statThreadPeriod = 60;

  /** Concurrent map (the cache) */
  private final ConcurrentHashMap<String,CachedBlock> map;

  /** Eviction lock (locked when eviction in process) */
  private final ReentrantLock evictionLock = new ReentrantLock(true);

  /** Volatile boolean to track if we are in an eviction process or not */
  private volatile boolean evictionInProgress = false;

  /** Eviction thread */
  private final EvictionThread evictionThread;

  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private final ScheduledExecutorService scheduleThreadPool =
      ThreadPools.getServerThreadPools().createScheduledExecutorService(1, "LRUBlockCacheStats");

  /** Current size of cache */
  private final AtomicLong size;

  /** Current number of cached elements */
  private final AtomicLong elements;

  /** Cache access count (sequential ID) */
  private final AtomicLong count;

  /** Cache statistics */
  private final CacheStats stats;

  /** Overhead of the structure itself */
  private final long overhead;

  private final LruBlockCacheConfiguration conf;

  /**
   * Default constructor. Specify maximum size and expected average block size (approximation is
   * fine).
   *
   * <p>
   * All other factors will be calculated based on defaults specified in this class.
   *
   * @param conf block cache configuration
   */
  @SuppressFBWarnings(value = "SC_START_IN_CTOR",
      justification = "bad practice to start threads in constructor; probably needs rewrite")
  public LruBlockCache(final LruBlockCacheConfiguration conf) {
    this.conf = conf;

    int mapInitialSize = (int) Math.ceil(1.2 * conf.getMaxSize() / conf.getBlockSize());

    map = new ConcurrentHashMap<>(mapInitialSize, conf.getMapLoadFactor(),
        conf.getMapConcurrencyLevel());
    this.stats = new CacheStats();
    this.count = new AtomicLong(0);
    this.elements = new AtomicLong(0);
    this.overhead =
        calculateOverhead(conf.getMaxSize(), conf.getBlockSize(), conf.getMapConcurrencyLevel());
    this.size = new AtomicLong(this.overhead);

    if (conf.isUseEvictionThread()) {
      this.evictionThread = new EvictionThread(this);
      this.evictionThread.start();
      while (!this.evictionThread.running()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    } else {
      this.evictionThread = null;
    }
    ScheduledFuture<?> future = this.scheduleThreadPool.scheduleAtFixedRate(
        new StatisticsThread(this), statThreadPeriod, statThreadPeriod, SECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  public long getOverhead() {
    return overhead;
  }

  /*
   * This class exists so that every cache entry does not have a reference to the cache.
   */
  private class LruCacheEntry implements CacheEntry {
    private final CachedBlock block;

    LruCacheEntry(CachedBlock block) {
      this.block = block;
    }

    @Override
    public byte[] getBuffer() {
      return block.getBuffer();
    }

    @Override
    public <T extends Weighable> T getIndex(Supplier<T> supplier) {
      return block.getIndex(supplier);
    }

    @Override
    public void indexWeightChanged() {
      long newSize = block.tryRecordSize(size);
      if (newSize >= 0 && newSize > acceptableSize() && !evictionInProgress) {
        runEviction();
      }
    }
  }

  private CacheEntry wrap(CachedBlock cb) {
    if (cb == null) {
      return null;
    }

    return new LruCacheEntry(cb);
  }

  // BlockCache implementation

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * It is assumed this will NEVER be called on an already cached block. If that is done, it is
   * assumed that you are reinserting the same exact block due to a race condition and will update
   * the buffer but not modify the size of the cache.
   *
   * @param blockName block name
   * @param buf block buffer
   * @param inMemory if block is in-memory
   */
  public CacheEntry cacheBlock(String blockName, byte[] buf, boolean inMemory) {
    CachedBlock cb = map.get(blockName);
    if (cb != null) {
      stats.duplicateReads();
      cb.access(count.incrementAndGet());
    } else {
      cb = new CachedBlock(blockName, buf, count.incrementAndGet(), inMemory);
      CachedBlock currCb = map.putIfAbsent(blockName, cb);
      if (currCb != null) {
        stats.duplicateReads();
        cb = currCb;
        cb.access(count.incrementAndGet());
      } else {
        // Actually added block to cache
        long newSize = cb.recordSize(size);
        elements.incrementAndGet();
        if (newSize > acceptableSize() && !evictionInProgress) {
          runEviction();
        }
      }
    }

    return wrap(cb);
  }

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * It is assumed this will NEVER be called on an already cached block. If that is done, it is
   * assumed that you are reinserting the same exact block due to a race condition and will update
   * the buffer but not modify the size of the cache.
   *
   * @param blockName block name
   * @param buf block buffer
   */
  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return cacheBlock(blockName, buf, false);
  }

  /**
   * Get the buffer of the block with the specified name.
   *
   * @param blockName block name
   * @return buffer of specified block name, or null if not in cache
   */
  @Override
  public CacheEntry getBlock(String blockName) {
    CachedBlock cb = map.get(blockName);
    if (cb == null) {
      stats.miss();
      return null;
    }
    stats.hit();
    cb.access(count.incrementAndGet());
    return wrap(cb);
  }

  @Override
  protected CacheEntry getBlockNoStats(String blockName) {
    CachedBlock cb = map.get(blockName);
    if (cb != null) {
      cb.access(count.incrementAndGet());
    }
    return wrap(cb);
  }

  protected long evictBlock(CachedBlock block) {
    if (map.remove(block.getName()) != null) {
      elements.decrementAndGet();
      stats.evicted();
      return block.evicted(size);
    }
    return 0;
  }

  /**
   * Multi-threaded call to run the eviction process.
   */
  private void runEviction() {
    if (evictionThread == null) {
      evict();
    } else {
      evictionThread.evict();
    }
  }

  /**
   * Eviction method.
   */
  void evict() {

    // Ensure only one eviction at a time
    if (!evictionLock.tryLock()) {
      return;
    }

    try {
      evictionInProgress = true;

      long bytesToFree = size.get() - minSize();

      log.trace("Block cache LRU eviction started.  Attempting to free {} bytes", bytesToFree);

      if (bytesToFree <= 0) {
        return;
      }

      // Instantiate priority buckets
      BlockBucket bucketSingle = new BlockBucket(bytesToFree, conf.getBlockSize(), singleSize());
      BlockBucket bucketMulti = new BlockBucket(bytesToFree, conf.getBlockSize(), multiSize());
      BlockBucket bucketMemory = new BlockBucket(bytesToFree, conf.getBlockSize(), memorySize());

      // Scan entire map putting into appropriate buckets
      for (CachedBlock cachedBlock : map.values()) {
        switch (cachedBlock.getPriority()) {
          case SINGLE:
            bucketSingle.add(cachedBlock);
            break;
          case MULTI:
            bucketMulti.add(cachedBlock);
            break;
          case MEMORY:
            bucketMemory.add(cachedBlock);
            break;
        }
      }

      PriorityQueue<BlockBucket> bucketQueue = new PriorityQueue<>(3);

      bucketQueue.add(bucketSingle);
      bucketQueue.add(bucketMulti);
      bucketQueue.add(bucketMemory);

      int remainingBuckets = 3;
      long bytesFreed = 0;

      BlockBucket bucket;
      while ((bucket = bucketQueue.poll()) != null) {
        long overflow = bucket.overflow();
        if (overflow > 0) {
          long bucketBytesToFree = Math.min(overflow,
              (long) Math.ceil((bytesToFree - bytesFreed) / (double) remainingBuckets));
          bytesFreed += bucket.free(bucketBytesToFree);
        }
        remainingBuckets--;
      }

      float singleMB = ((float) bucketSingle.totalSize()) / ((float) (1024 * 1024));
      float multiMB = ((float) bucketMulti.totalSize()) / ((float) (1024 * 1024));
      float memoryMB = ((float) bucketMemory.totalSize()) / ((float) (1024 * 1024));

      log.trace(
          "Block cache LRU eviction completed. Freed {} bytes. Priority Sizes:"
              + " Single={}MB ({}), Multi={}MB ({}), Memory={}MB ({})",
          bytesFreed, singleMB, bucketSingle.totalSize(), multiMB, bucketMulti.totalSize(),
          memoryMB, bucketMemory.totalSize());

    } finally {
      stats.evict();
      evictionInProgress = false;
      evictionLock.unlock();
    }
  }

  /**
   * Used to group blocks into priority buckets. There will be a BlockBucket for each priority
   * (single, multi, memory). Once bucketed, the eviction algorithm takes the appropriate number of
   * elements out of each according to configuration parameters and their relatives sizes.
   */
  private class BlockBucket implements Comparable<BlockBucket> {

    private final CachedBlockQueue queue;
    private long totalSize;
    private final long bucketSize;

    public BlockBucket(long bytesToFree, long blockSize, long bucketSize) {
      this.bucketSize = bucketSize;
      queue = new CachedBlockQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    public void add(CachedBlock block) {
      totalSize += block.heapSize();
      queue.add(block);
    }

    public long free(long toFree) {
      CachedBlock[] blocks = queue.get();
      long freedBytes = 0;
      for (CachedBlock block : blocks) {
        freedBytes += evictBlock(block);
        if (freedBytes >= toFree) {
          return freedBytes;
        }
      }
      return freedBytes;
    }

    public long overflow() {
      return totalSize - bucketSize;
    }

    public long totalSize() {
      return totalSize;
    }

    @Override
    public int compareTo(BlockBucket that) {
      if (this.overflow() == that.overflow()) {
        return 0;
      }
      return this.overflow() > that.overflow() ? 1 : -1;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(overflow());
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof BlockBucket) {
        return compareTo((BlockBucket) that) == 0;
      }
      return false;
    }
  }

  @Override
  public long getMaxHeapSize() {
    return getMaxSize();
  }

  @Override
  public long getMaxSize() {
    return this.conf.getMaxSize();
  }

  @Override
  public int getMaxEntrySize() {
    return (int) Math.min(Integer.MAX_VALUE, getMaxSize());
  }

  /**
   * Get the current size of this cache.
   *
   * @return current size in bytes
   */
  public long getCurrentSize() {
    return this.size.get();
  }

  /**
   * Get the current size of this cache.
   *
   * @return current size in bytes
   */
  public long getFreeSize() {
    return getMaxSize() - getCurrentSize();
  }

  /**
   * Get the size of this cache (number of cached blocks)
   *
   * @return number of cached blocks
   */
  public long size() {
    return this.elements.get();
  }

  /**
   * Get the number of eviction runs that have occurred
   */
  public long getEvictionCount() {
    return this.stats.getEvictionCount();
  }

  /**
   * Get the number of blocks that have been evicted during the lifetime of this cache.
   */
  public long getEvictedCount() {
    return this.stats.getEvictedCount();
  }

  /**
   * Eviction thread. Sits in waiting state until an eviction is triggered when the cache size grows
   * above the acceptable level.
   *
   * <p>
   * Thread is triggered into action by {@link LruBlockCache#runEviction()}
   */
  private static class EvictionThread extends AccumuloDaemonThread {
    private final WeakReference<LruBlockCache> cache;
    private boolean running = false;

    public EvictionThread(LruBlockCache cache) {
      super("LruBlockCache.EvictionThread");
      this.cache = new WeakReference<>(cache);
    }

    public synchronized boolean running() {
      return running;
    }

    @SuppressFBWarnings(value = "UW_UNCOND_WAIT", justification = "eviction is resumed by caller")
    @Override
    public void run() {
      while (true) {
        synchronized (this) {
          running = true;
          try {
            this.wait();
          } catch (InterruptedException e) {
            // empty
          }
        }
        LruBlockCache cache = this.cache.get();
        if (cache == null) {
          break;
        }
        cache.evict();
      }
    }

    @SuppressFBWarnings(value = "NN_NAKED_NOTIFY", justification = "eviction is resumed by caller")
    public void evict() {
      synchronized (this) {
        this.notify();
      }
    }
  }

  /*
   * Statistics thread. Periodically prints the cache statistics to the log.
   */
  private static class StatisticsThread extends AccumuloDaemonThread {
    LruBlockCache lru;

    public StatisticsThread(LruBlockCache lru) {
      super("LruBlockCache.StatisticsThread");
      this.lru = lru;
    }

    @Override
    public void run() {
      lru.logStats();
    }
  }

  public void logStats() {
    // Log size
    long totalSize = heapSize();
    long freeSize = this.conf.getMaxSize() - totalSize;
    float sizeMB = ((float) totalSize) / ((float) (1024 * 1024));
    float freeMB = ((float) freeSize) / ((float) (1024 * 1024));
    float maxMB = ((float) this.conf.getMaxSize()) / ((float) (1024 * 1024));
    log.debug(
        "Cache Stats: Sizes: Total={}MB ({}), Free={}MB ({}), Max={}MB"
            + " ({}), Counts: Blocks={}, Access={}, Hit={}, Miss={}, Evictions={},"
            + " Evicted={},Ratios: Hit Ratio={}%, Miss Ratio={}%, Evicted/Run={},"
            + " Duplicate Reads={}",
        sizeMB, totalSize, freeMB, freeSize, maxMB, this.conf.getMaxSize(), size(),
        stats.requestCount(), stats.hitCount(), stats.getMissCount(), stats.getEvictionCount(),
        stats.getEvictedCount(), stats.getHitRatio() * 100, stats.getMissRatio() * 100,
        stats.evictedPerEviction(), stats.getDuplicateReads());
  }

  /**
   * Get counter statistics for this cache.
   *
   * <p>
   * Includes: total accesses, hits, misses, evicted blocks, and runs of the eviction processes.
   */
  @Override
  public CacheStats getStats() {
    return this.stats;
  }

  public static class CacheStats implements BlockCache.Stats {
    private final AtomicLong accessCount = new AtomicLong(0);
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);
    private final AtomicLong evictedCount = new AtomicLong(0);
    private final AtomicLong duplicateReads = new AtomicLong(0);

    public void miss() {
      missCount.incrementAndGet();
      accessCount.incrementAndGet();
    }

    public void hit() {
      hitCount.incrementAndGet();
      accessCount.incrementAndGet();
    }

    public void evict() {
      evictionCount.incrementAndGet();
    }

    public void duplicateReads() {
      duplicateReads.incrementAndGet();
    }

    public void evicted() {
      evictedCount.incrementAndGet();
    }

    @Override
    public long requestCount() {
      return accessCount.get();
    }

    public long getMissCount() {
      return missCount.get();
    }

    @Override
    public long hitCount() {
      return hitCount.get();
    }

    public long getEvictionCount() {
      return evictionCount.get();
    }

    public long getDuplicateReads() {
      return duplicateReads.get();
    }

    public long getEvictedCount() {
      return evictedCount.get();
    }

    public double getHitRatio() {
      return ((float) hitCount() / (float) requestCount());
    }

    public double getMissRatio() {
      return ((float) getMissCount() / (float) requestCount());
    }

    public double evictedPerEviction() {
      return (float) getEvictedCount() / (float) getEvictionCount();
    }
  }

  public static final long CACHE_FIXED_OVERHEAD =
      ClassSize.align((3 * SizeConstants.SIZEOF_LONG) + (8 * ClassSize.REFERENCE)
          + (5 * SizeConstants.SIZEOF_FLOAT) + SizeConstants.SIZEOF_BOOLEAN + ClassSize.OBJECT);

  // HeapSize implementation
  @Override
  public long heapSize() {
    return getCurrentSize();
  }

  public static long calculateOverhead(long maxSize, long blockSize, int concurrency) {
    long entryPart = Math.round(maxSize * 1.2 / blockSize) * CONCURRENT_HASHMAP_ENTRY;
    long segmentPart = (long) concurrency * CONCURRENT_HASHMAP_SEGMENT;
    return CACHE_FIXED_OVERHEAD + CONCURRENT_HASHMAP + entryPart + segmentPart;
  }

  // Simple calculators of sizes given factors and maxSize

  private long acceptableSize() {
    return (long) Math.floor(this.conf.getMaxSize() * this.conf.getAcceptableFactor());
  }

  private long minSize() {
    return (long) Math.floor(this.conf.getMaxSize() * this.conf.getMinFactor());
  }

  private long singleSize() {
    return (long) Math
        .floor(this.conf.getMaxSize() * this.conf.getSingleFactor() * this.conf.getMinFactor());
  }

  private long multiSize() {
    return (long) Math
        .floor(this.conf.getMaxSize() * this.conf.getMultiFactor() * this.conf.getMinFactor());
  }

  private long memorySize() {
    return (long) Math
        .floor(this.conf.getMaxSize() * this.conf.getMemoryFactor() * this.conf.getMinFactor());
  }

  public void shutdown() {
    this.scheduleThreadPool.shutdown();
  }
}

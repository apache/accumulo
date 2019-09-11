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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.PriorityQueue;

import com.google.common.base.Preconditions;

/**
 * A memory-bound queue that will grow until an element brings total size &gt;=
 * maxSize. From then on, only entries that are sorted larger than the smallest
 * current entry will be inserted and the smallest entry is removed.
 *
 * <p>
 * Use this when you want to find the largest elements (according to their
 * natural ordering, not their heap size) that consume as close to the specified
 * maxSize as possible. Default behavior is to grow just above rather than just
 * below specified max.
 * </p>
 *
 * <p>
 * Object used in this queue must implement {@link HeapSize} as well as
 * {@link Comparable}.
 * </p>
 */
public class CachedBlockQueue implements HeapSize {

  private final PriorityQueue<CachedBlock> queue;

  private final long maxSize;
  private long heapSize;

  /**
   * Construct a CachedBlockQueue.
   *
   * @param maxSize the target size of elements in the queue
   * @param blockSize expected average size of blocks
   */
  public CachedBlockQueue(final long maxSize, final long blockSize) {
    Preconditions.checkArgument(maxSize > 0L);
    Preconditions.checkArgument(blockSize > 0L);

    final long initialSize = (maxSize + blockSize - 1L) / blockSize;

    this.queue = new PriorityQueue<>(Math.toIntExact(initialSize));
    this.heapSize = 0L;
    this.maxSize = maxSize;
  }

  /**
   * Attempt to add the specified cached block to this queue.
   *
   * If the block fits within the max heap size, the element will be added to
   * the queue. Otherwise, if the block being added is sorted larger than the
   * smallest element, the block is inserted and the smallest element is removed
   * from the queue.
   *
   * @param cb block to try to add to the queue
   * @return true if the CachedBlock was added to the queue
   */
  public boolean add(final CachedBlock cb) {
    if (available() > 0) {
      heapSize += cb.heapSize();
      return queue.add(cb);
    }
    final CachedBlock mruPeek = queue.peek();
    if (mruPeek.compareTo(cb) > 0) {
      return false;
    }
    heapSize += cb.heapSize();
    heapSize -= mruPeek.heapSize();
    queue.poll();
    return queue.add(cb);
  }

  /**
   * Get a copy of all elements in this queue, in LRU ascending order.
   *
   * @return Collection of cached elements in queue order
   */
  public Collection<CachedBlock> getBlocks() {
    CachedBlock[] blocks = queue.toArray(new CachedBlock[0]);
    Arrays.sort(blocks, Collections.reverseOrder());
    return Arrays.asList(blocks);
  }

  /**
   * Total size of all elements in this queue.
   *
   * @return size of all elements currently in queue, in bytes
   */
  @Override
  public long heapSize() {
    return this.heapSize;
  }

  public int size() {
    return this.queue.size();
  }

  /**
   * The number of available bytes relative to the maxSize. May be negative.
   *
   * @return The number of bytes available before the max size is reached
   */
  public long available() {
    return this.maxSize - this.heapSize;
  }
}

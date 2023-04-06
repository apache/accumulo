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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlockQueue;
import org.junit.jupiter.api.Test;

public class TestCachedBlockQueue {
  @Test
  public void testLargeBlock() {
    CachedBlockQueue queue = new CachedBlockQueue(10000L, 1000L);
    CachedBlock cb1 = new CachedBlock(10001L, "cb1", 1L);
    queue.add(cb1);

    List<org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock> blocks = getList(queue);
    assertEquals("cb1", Objects.requireNonNull(blocks.get(0)).getName());
  }

  @Test
  public void testAddNewerBlock() {
    CachedBlockQueue queue = new CachedBlockQueue(10000L, 1000L);

    AtomicLong sum = new AtomicLong();

    CachedBlock cb1 = new CachedBlock(5000L, "cb1", 1L);
    cb1.recordSize(sum);
    CachedBlock cb2 = new CachedBlock(5000, "cb2", 2L);
    cb2.recordSize(sum);
    CachedBlock cb3 = new CachedBlock(5000, "cb3", 3L);
    cb3.recordSize(sum);

    queue.add(cb1);
    queue.add(cb2);
    queue.add(cb3);

    List<org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock> blocks = getList(queue);

    assertEquals(2, blocks.size());

    long expectedSize = cb1.heapSize() + cb2.heapSize();
    assertEquals(expectedSize, queue.heapSize());
    assertEquals(expectedSize, sum.get() - cb3.heapSize());

    assertEquals(List.of("cb1", "cb2"),
        blocks.stream().map(cb -> cb.getName()).collect(Collectors.toList()));
  }

  @Test
  public void testQueue() {
    AtomicLong sum = new AtomicLong();
    CachedBlock cb1 = new CachedBlock(1000, "cb1", 1);
    cb1.recordSize(sum);
    CachedBlock cb2 = new CachedBlock(1500, "cb2", 2);
    cb2.recordSize(sum);
    CachedBlock cb3 = new CachedBlock(1000, "cb3", 3);
    cb3.recordSize(sum);
    CachedBlock cb4 = new CachedBlock(1500, "cb4", 4);
    cb4.recordSize(sum);
    CachedBlock cb5 = new CachedBlock(1000, "cb5", 5);
    cb5.recordSize(sum);
    CachedBlock cb6 = new CachedBlock(1750, "cb6", 6);
    cb6.recordSize(sum);
    CachedBlock cb7 = new CachedBlock(1000, "cb7", 7);
    cb7.recordSize(sum);
    CachedBlock cb8 = new CachedBlock(1500, "cb8", 8);
    cb8.recordSize(sum);
    CachedBlock cb9 = new CachedBlock(1000, "cb9", 9);
    cb9.recordSize(sum);
    CachedBlock cb10 = new CachedBlock(1500, "cb10", 10);
    cb10.recordSize(sum);

    CachedBlockQueue queue = new CachedBlockQueue(10000, 1000);

    queue.add(cb1);
    queue.add(cb2);
    queue.add(cb3);
    queue.add(cb4);
    queue.add(cb5);
    queue.add(cb6);
    queue.add(cb7);
    queue.add(cb8);
    queue.add(cb9);
    queue.add(cb10);

    // We expect cb1 through cb8 to be in the queue
    long expectedSize = cb1.heapSize() + cb2.heapSize() + cb3.heapSize() + cb4.heapSize()
        + cb5.heapSize() + cb6.heapSize() + cb7.heapSize() + cb8.heapSize();

    assertEquals(expectedSize, queue.heapSize());
    assertEquals(expectedSize, sum.get() - cb9.heapSize() - cb10.heapSize());

    List<org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock> blocks = getList(queue);

    assertEquals(List.of("cb1", "cb2", "cb3", "cb4", "cb5", "cb6", "cb7", "cb8"),
        blocks.stream().map(cb -> cb.getName()).collect(Collectors.toList()));
  }

  @Test
  public void testQueueSmallBlockEdgeCase() {

    AtomicLong sum = new AtomicLong();
    CachedBlock cb1 = new CachedBlock(1000, "cb1", 1);
    cb1.recordSize(sum);
    CachedBlock cb2 = new CachedBlock(1500, "cb2", 2);
    cb2.recordSize(sum);
    CachedBlock cb3 = new CachedBlock(1000, "cb3", 3);
    cb3.recordSize(sum);
    CachedBlock cb4 = new CachedBlock(1500, "cb4", 4);
    cb4.recordSize(sum);
    CachedBlock cb5 = new CachedBlock(1000, "cb5", 5);
    cb5.recordSize(sum);
    CachedBlock cb6 = new CachedBlock(1750, "cb6", 6);
    cb6.recordSize(sum);
    CachedBlock cb7 = new CachedBlock(1000, "cb7", 7);
    cb7.recordSize(sum);
    CachedBlock cb8 = new CachedBlock(1500, "cb8", 8);
    cb8.recordSize(sum);
    CachedBlock cb9 = new CachedBlock(1000, "cb9", 9);
    cb9.recordSize(sum);
    CachedBlock cb10 = new CachedBlock(1500, "cb10", 10);
    cb10.recordSize(sum);

    // validate that sum was not improperly added to heapSize in recordSize method.
    assertEquals(cb3.heapSize(), cb7.heapSize());

    CachedBlockQueue queue = new CachedBlockQueue(10000, 1000);

    queue.add(cb1);
    queue.add(cb2);
    queue.add(cb3);
    queue.add(cb4);
    queue.add(cb5);
    queue.add(cb6);
    queue.add(cb7);
    queue.add(cb8);
    queue.add(cb9);
    queue.add(cb10);

    CachedBlock cb0 = new CachedBlock(10 + CachedBlock.PER_BLOCK_OVERHEAD, "cb0", 0);
    queue.add(cb0);

    // This is older, so we must include it, but it will not end up kicking
    // anything out because (heapSize - cb8.heapSize + cb0.heapSize < maxSize)
    // and we must always maintain heapSize >= maxSize once we achieve it.

    // We expect cb0 through cb8 to be in the queue
    long expectedSize = cb1.heapSize() + cb2.heapSize() + cb3.heapSize() + cb4.heapSize()
        + cb5.heapSize() + cb6.heapSize() + cb7.heapSize() + cb8.heapSize() + cb0.heapSize();

    assertEquals(expectedSize, queue.heapSize());
    assertEquals(expectedSize, sum.get() - cb9.heapSize() - cb10.heapSize());

    List<org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock> blocks = getList(queue);

    assertEquals(List.of("cb0", "cb1", "cb2", "cb3", "cb4", "cb5", "cb6", "cb7", "cb8"),
        blocks.stream().map(cb -> cb.getName()).collect(Collectors.toList()));
  }

  private static class CachedBlock
      extends org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock {
    public CachedBlock(long heapSize, String name, long accessTime) {
      super(name, new byte[(int) (heapSize - CachedBlock.PER_BLOCK_OVERHEAD)], accessTime, false);
    }
  }

  /**
   * Get a sorted List of all elements in this queue, in descending order.
   *
   * @return list of cached elements in descending order
   */
  private List<org.apache.accumulo.core.file.blockfile.cache.lru.CachedBlock>
      getList(final CachedBlockQueue queue) {
    return List.of(queue.get());
  }

}

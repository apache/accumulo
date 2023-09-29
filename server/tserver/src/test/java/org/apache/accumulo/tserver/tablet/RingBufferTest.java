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
package org.apache.accumulo.tserver.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

public class RingBufferTest {

  @Test
  public void goPathTest() {
    TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(4);
    assertEquals(4, ring.capacity());
    assertEquals(0, ring.size());
    assertTrue(ring.isEmpty());
    assertNull(ring.add("1"));
    assertEquals(1, ring.size());
    assertFalse(ring.isEmpty());
    assertNull(ring.add("2"));
    assertEquals(2, ring.size());
    assertFalse(ring.isEmpty());
    assertNull(ring.add("3"));
    assertEquals(3, ring.size());
    assertFalse(ring.isEmpty());
    assertNull(ring.add("4"));
    assertEquals(4, ring.size());
    assertFalse(ring.isEmpty());
    assertEquals(4, ring.capacity());
    assertEquals(List.of("1", "2", "3", "4"), ring.toList());
  }

  @Test
  public void overflowTest() {
    TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(4);
    assertNull(ring.add("1"));
    assertNull(ring.add("2"));
    assertNull(ring.add("3"));
    assertNull(ring.add("4"));
    assertEquals("1", ring.add("5"));

    assertFalse(ring.isEmpty());
    assertEquals(4, ring.size());
    assertEquals(4, ring.capacity());
    assertEquals(List.of("2", "3", "4", "5"), ring.toList());
  }

  @Test
  public void oddCapacityTest() {
    TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(3);
    assertNull(ring.add("1"));
    assertNull(ring.add("2"));
    assertNull(ring.add("3"));
    assertEquals("1", ring.add("4"));
    assertEquals("2", ring.add("5"));

    assertFalse(ring.isEmpty());
    assertEquals(3, ring.capacity());
    assertEquals(3, ring.size());
    assertEquals(List.of("3", "4", "5"), ring.toList());
  }

  @Test
  public void zeroLengthTest() {
    TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(0);
    assertEquals("1", ring.add("1"));
    assertTrue(ring.isEmpty());
    assertEquals(0, ring.size());
    assertEquals(0, ring.capacity());
    assertEquals(List.of(), ring.toList());
  }

  @Test
  public void threadOrderingTest() throws Exception {
    var executor = Executors.newFixedThreadPool(2);

    final TabletTransactionLog.Ring<Integer> ring = new TabletTransactionLog.Ring<>(11);

    final int max = 100_000_000;

    var readerFuture = executor.submit(() -> {
      int lastSize = 0;

      while (true) {
        boolean done = false;
        List<Integer> ints = Collections.emptyList();
        while (!done) {
          try {
            ints = ring.toList();
            done = true;
          } catch (ConcurrentModificationException c) {
            // try again
          }
        }

        // the list size should increase up to the capacity
        // it could shrink by 1 every once in a while if the list is gathered during
        // the addition of a transaction that is removing an item as well.
        assertTrue(ints.size() >= (lastSize - 1) && ints.size() <= ring.capacity());

        lastSize = ints.size();

        // always expect list to contain monotonically increasing integers
        for (int i = 1; i < ints.size(); i++) {
          assertEquals(ints.get(i - 1), ints.get(i) - 1);
        }

        if (!ints.isEmpty() && ints.get(ints.size() - 1) == max - 1) {
          return;
        }
      }
    });

    var writerFuture = executor.submit(() -> {
      for (int i = 0; i < max; i++) {
        ring.add(i);
      }
    });

    // ensure task completed w/o exception
    readerFuture.get();
    writerFuture.get();

    executor.shutdownNow();
  }

  @Test
  public void maxAllowedSizeFails() {
    try {
      TabletTransactionLog.Ring<Integer> ring =
          new TabletTransactionLog.Ring<>(TabletTransactionLog.Ring.MAX_ALLOWED_SIZE + 1);
      fail("Expected failure creating ring larger than "
          + TabletTransactionLog.Ring.MAX_ALLOWED_SIZE);
    } catch (IllegalArgumentException e) {
      // expected;
    }
  }

  @Test
  public void testOverrun() {
    int capacity = 13;
    TabletTransactionLog.Ring<Integer> ring = new TabletTransactionLog.Ring<>(capacity);
    int count = 0;
    for (int i = 0; i < TabletTransactionLog.Ring.OVERRUN_THRESHOLD; i++) {
      ring.add(count++);
    }

    assertEquals(IntStream.range(count - capacity, count).boxed().collect(Collectors.toList()),
        ring.toList());
    assertEquals(capacity, ring.size());
    assertEquals(TabletTransactionLog.Ring.OVERRUN_THRESHOLD - 1, ring.last());
    assertEquals(TabletTransactionLog.Ring.OVERRUN_THRESHOLD - capacity, ring.first());

    ring.add(count++);

    assertEquals(IntStream.range(count - capacity, count).boxed().collect(Collectors.toList()),
        ring.toList());
    assertEquals(capacity, ring.size());
    assertEquals(TabletTransactionLog.Ring.OVERRUN_THRESHOLD, ring.last());
    assertEquals(TabletTransactionLog.Ring.OVERRUN_THRESHOLD - capacity + 1, ring.first());

    ring.add(count++);

    assertEquals(IntStream.range(count - capacity, count).boxed().collect(Collectors.toList()),
        ring.toList());
    assertEquals(capacity, ring.size());
    // calculate the first index that leaves us in the same position
    int first = TabletTransactionLog.Ring.OVERRUN_THRESHOLD - capacity + 2;
    int delta = first % capacity;
    assertEquals(capacity - 1 + delta, ring.last());
    assertEquals(delta, ring.first());

    ring.add(count++);

    assertEquals(IntStream.range(count - capacity, count).boxed().collect(Collectors.toList()),
        ring.toList());
    assertEquals(capacity, ring.size());
    assertEquals(capacity + delta, ring.last());
    assertEquals(delta + 1, ring.first());
  }
}

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
import static org.junit.jupiter.api.Assertions.fail;

import java.security.SecureRandom;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

public class RingBufferTest {

  @Test
  public void goPathTest() {
    TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(4);
    ring.add("1");
    ring.add("2");
    ring.add("3");
    ring.add("4");

    assertFalse(ring.isEmpty());
    assertEquals(4, ring.capacity());
    assertEquals(List.of("1", "2", "3", "4"), ring.toList());
  }

  @Test
  public void overflowTest() {
    TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(4);
    ring.add("1");
    ring.add("2");
    ring.add("3");
    ring.add("4");
    assertEquals("1", ring.add("5"));

    assertFalse(ring.isEmpty());
    assertEquals(4, ring.capacity());
    assertEquals(List.of("2", "3", "4", "5"), ring.toList());
  }

  @Test
  public void oddCapacityTest() {
    TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(3);
    ring.add("1");
    ring.add("2");
    ring.add("3");
    assertEquals("1", ring.add("4"));
    assertEquals("2", ring.add("5"));

    assertFalse(ring.isEmpty());
    assertEquals(3, ring.capacity());
    assertEquals(List.of("3", "4", "5"), ring.toList());
  }

  @Test
  public void zeroLengthTest() {
    TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(0);
    assertEquals("1", ring.add("1"));
  }

  private static class ExceptionHandler implements Thread.UncaughtExceptionHandler {
    public Throwable thrown = null;

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      synchronized (this) {
        thrown = e;
      }
    }
  }
  @Test
  public void threadOrderingTest() throws Exception {
    var executor = Executors.newFixedThreadPool(2);

    // TODO could try diff ring sizes
    final TabletTransactionLog.Ring<Integer> ring = new TabletTransactionLog.Ring<>(11);

    final int max = 100_000_000;

    var readerFuture = executor.submit(() -> {
      int lastSize = 0;

      while (true) {
        List<Integer> ints = ring.toListNoFail();

        // the list size should increase up to the capacity, do not expect it to ever shrink
        assertTrue(ints.size() >= lastSize && ints.size() <= ring.capacity());

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
  public void testThreadSafety() throws InterruptedException {
    final TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(5);
    final ExceptionHandler handler = new ExceptionHandler();
    final Object startLock = new Object();
    final AtomicInteger ready = new AtomicInteger(0);
    final SecureRandom random = new SecureRandom();

    // create a writer threads
    Thread writerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        synchronized (startLock) {
          try {
            ready.incrementAndGet();
            startLock.wait(3000);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 2000) {
          if (random.nextBoolean()) {
            ring.add(UUID.randomUUID().toString());
          } else {
            ring.clear();
          }
        }
      }
    });
    writerThread.setUncaughtExceptionHandler(handler);

    // create a writer threads
    Thread readerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        synchronized (startLock) {
          try {
            ready.incrementAndGet();
            startLock.wait(3000);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 2000) {
          ring.toListNoFail();
        }
      }
    });
    readerThread.setUncaughtExceptionHandler(handler);

    // start the threads
    writerThread.start();
    readerThread.start();

    // wait until they are both waiting on the start lock
    while (ready.get() < 2) {
      Thread.sleep(100);
    }

    // unlock the threads
    synchronized (startLock) {
      ready.incrementAndGet();
      startLock.notifyAll();
    }

    // wait for the threads to complete
    while (writerThread.isAlive() || readerThread.isAlive()) {
      Thread.sleep(100);
    }

    // ensure no exceptions were thrown
    assertNull(handler.thrown, "Exception was thrown: " + handler.thrown);
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

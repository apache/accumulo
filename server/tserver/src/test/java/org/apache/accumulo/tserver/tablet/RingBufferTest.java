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

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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
  public void testThreadSafety() {
    final TabletTransactionLog.Ring<String> ring = new TabletTransactionLog.Ring<>(5);
    final ExceptionHandler handler = new ExceptionHandler();
    final Object startLock = new Object();
    final AtomicInteger ready = new AtomicInteger(0);

    // create a writer threads
    Thread writerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        synchronized (startLock) {
          try {
            ready.incrementAndGet();
            startLock.wait();
          } catch (Exception e) {}
        }
        long start = System.currentTimeMillis();
        Random random = new Random();
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
            startLock.wait();
          } catch (Exception e) {}
        }
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 2000) {
          ring.toList();
        }
      }
    });
    readerThread.setUncaughtExceptionHandler(handler);

    // start the threads
    writerThread.start();
    readerThread.start();

    // wait until they are both waiting on the start lock
    while (ready.get() < 2) {
      try {
        Thread.sleep(100);
      } catch (Exception e) {}
    }

    // unlock the threads
    synchronized (startLock) {
      startLock.notifyAll();
    }

    // wait for the threads to complete
    while (writerThread.isAlive() || readerThread.isAlive()) {
      try {
        Thread.sleep(100);
      } catch (Exception e) {}
    }

    // ensure no exceptions were thrown
    assertNull(handler.thrown, "Exception was thrown: " + handler.thrown);
  }
}

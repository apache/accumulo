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
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

public class LockMapTest {

  /**
   * Verify locks are created as needed and disposed of when no longer in use.
   */
  @Test
  public void testReferences() {
    var lm = new LockMap<String>();

    var perKeyLock1 = lm.lock("abc");
    var perKeyLock2 = lm.lock("abc");
    var perKeyLock3 = lm.lock("def");

    assertSame(perKeyLock1, perKeyLock2);
    assertNotSame(perKeyLock1, perKeyLock3);

    // This should not dispose of the lock as there is still one use of it
    perKeyLock1.close();

    // verify the lock for abc was not disposed
    var perKeyLock4 = lm.lock("abc");
    assertSame(perKeyLock2, perKeyLock4);

    // these locks should be disposed of as nothing will be using them after the close calls
    perKeyLock2.close();
    perKeyLock3.close();
    perKeyLock4.close();

    // should create new locks for the two keys
    var perKeyLock5 = lm.lock("abc");
    var perKeyLock6 = lm.lock("def");
    assertNotSame(perKeyLock4, perKeyLock5);
    assertNotSame(perKeyLock3, perKeyLock6);
    assertNotSame(perKeyLock5, perKeyLock6);

    perKeyLock5.close();
    perKeyLock6.close();
  }

  @Test
  public void testMultiClose() {
    var lm = new LockMap<String>();

    var perKeyLock1 = lm.lock("abc");

    perKeyLock1.close();
    assertThrows(IllegalMonitorStateException.class, perKeyLock1::close);
  }

  /**
   * This test attempts to verify two things for a LockMap instance. First that only one thread will
   * ever be able to lock a given key. Second that different threads can lock different keys at the
   * same time.
   */
  @Test
  public void testConcurrency() throws Exception {
    final int numThreads = 32;
    var executor = Executors.newFixedThreadPool(numThreads);
    final int numTasks = numThreads * 4;

    try {
      var lockMap = new LockMap<Integer>();
      var random = LazySingletons.RANDOM.get();

      var booleans = new AtomicBoolean[] {new AtomicBoolean(), new AtomicBoolean(),
          new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean()};

      var futures = new ArrayList<Future<Boolean>>(numTasks);
      // start a portion of threads at the same time
      CountDownLatch startLatch = new CountDownLatch(numThreads);
      assertTrue(numTasks >= startLatch.getCount(),
          "Not enough tasks to satisfy latch count - deadlock risk");
      assertTrue(numThreads >= startLatch.getCount(),
          "Not enough threads to satisfy latch count - deadlock risk");

      var maxLocked = new AtomicLong(0);

      for (int i = 0; i < numTasks; i++) {
        int key = random.nextInt(booleans.length);
        var future = executor.submit(() -> {
          startLatch.countDown();
          startLatch.await();
          try (var unused = lockMap.lock(key)) {
            var set1 = booleans[key].compareAndSet(false, true);
            // maxLocked is used to check that at some point we see another thread w/ a lock for a
            // different key.
            do {
              Thread.sleep(0, 100);
            } while (maxLocked.accumulateAndGet(
                Stream.of(booleans).filter(AtomicBoolean::get).count(), Long::max) < 2);
            var set2 = booleans[key].compareAndSet(true, false);
            // If only one thread executes per key, then set1 and set2 should always be true
            return set1 && set2;
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          }
        });
        futures.add(future);
      }
      assertEquals(numTasks, futures.size());

      for (var future : futures) {
        assertTrue(future.get());
      }

      assertTrue(maxLocked.get() >= 2 && maxLocked.get() <= 5);
    } finally {
      executor.shutdownNow();
    }
  }

}

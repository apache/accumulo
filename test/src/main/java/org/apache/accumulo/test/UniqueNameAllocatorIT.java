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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class UniqueNameAllocatorIT extends SharedMiniClusterBase {
  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
      justification = "predictable random is ok for testing")
  @Test
  void testConcurrentNameGeneration() throws Exception {
    var srvCtx = getCluster().getServerContext();

    // create multiple namers, each will be interact with zookeeper independently and should see
    // updates from others.
    var allocators = new UniqueNameAllocator[10];
    for (int i = 0; i < 10; i++) {
      allocators[i] = new UniqueNameAllocator(srvCtx);
    }
    Random random = new Random();
    Set<String> namesSeen = ConcurrentHashMap.newKeySet();

    var executorService = Executors.newCachedThreadPool();
    List<Future<Integer>> futures = new ArrayList<>();
    // start a portion of threads at the same time
    CountDownLatch startLatch = new CountDownLatch(32);
    assertTrue(64 >= startLatch.getCount(),
        "Not enough tasks to satisfy latch count - deadlock risk");

    // create threads that are allocating large random chunks
    for (int i = 0; i < 64; i++) {
      var future = executorService.submit(() -> {
        startLatch.countDown();
        startLatch.await();
        int added = 0;
        while (namesSeen.size() < 1_000_000) {
          var allocator = allocators[random.nextInt(allocators.length)];
          int needed = Math.max(1, random.nextInt(999));
          allocate(allocator, needed, namesSeen);
          added += needed;
        }
        return added;
      });
      futures.add(future);
    }

    // create threads that are always allocating a small amount
    for (int i = 1; i <= 10; i++) {
      int needed = i;
      var future = executorService.submit(() -> {
        startLatch.countDown();
        startLatch.await();
        int added = 0;
        while (namesSeen.size() < 1_000_000) {
          var allocator = allocators[random.nextInt(allocators.length)];
          allocate(allocator, needed, namesSeen);
          added += needed;
        }
        return added;
      });
      futures.add(future);
    }

    for (var future : futures) {
      // expect all threads to add some names
      assertTrue(future.get() > 0);
    }

    assertThrows(IllegalArgumentException.class, () -> allocators[0].getNextNames(-1));
    assertFalse(allocators[0].getNextNames(0).hasNext());

    assertTrue(namesSeen.size() >= 1_000_000);
    assertTrue(namesSeen.stream().allMatch(name -> name.matches("[0-9a-z]{7}")));
  }

  private static void allocate(UniqueNameAllocator allocator, int needed, Set<String> namesSeen) {
    Iterator<String> names = allocator.getNextNames(needed);
    for (int n = 0; n < needed; n++) {
      assertTrue(names.hasNext());
      String nextName = names.next();
      // set should never contain a name already
      assertTrue(namesSeen.add(nextName));
    }
    assertFalse(names.hasNext());
    // the iterator should be exhausted
    assertThrows(NoSuchElementException.class, names::next);
  }
}

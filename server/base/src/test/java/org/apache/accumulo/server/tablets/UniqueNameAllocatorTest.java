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
package org.apache.accumulo.server.tablets;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.server.tablets.UniqueNameAllocator.NameIterator;
import org.junit.jupiter.api.Test;

public class UniqueNameAllocatorTest {
  @Test
  public void testErrors() {
    assertThrows(IllegalArgumentException.class, () -> new NameIterator(0, 0));
    assertThrows(IllegalArgumentException.class, () -> new NameIterator(1, 0));
  }

  @Test
  public void testNameIterator() {
    testRange(0, 1);
    testRange(0, 2);
    testRange(0, 10);
    testRange(10, 11);
    testRange(10, 12);
    testRange(10, 20);
  }

  private void testRange(long start, long end) {
    NameIterator iter = new NameIterator(start, end);
    for (long i = start; i < end; i++) {
      assertTrue(iter.hasNext());
      assertEquals(i, Long.parseLong(iter.next(), 36));
    }
    assertFalse(iter.hasNext());
    assertThrows(NoSuchElementException.class, iter::next);
  }

  @Test
  public void testConcurrent() throws Exception {
    NameIterator iter = new NameIterator(0, 10_000);
    var executor = Executors.newCachedThreadPool();

    // start 100 threads each consuming 100 names
    List<Future<String[]>> futures = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      var future = executor.submit(() -> {
        String[] names = new String[100];
        for (int j = 0; j < 100; j++) {
          assertTrue(iter.hasNext());
          names[j] = iter.next();
        }
        return names;
      });
      futures.add(future);
    }

    // verify 10_000 unique names were generated
    TreeSet<String> allNames = new TreeSet<>();
    for (var future : futures) {
      String[] names = future.get();
      for (String name : names) {
        assertTrue(allNames.add(name));
      }
    }

    // everything should be consumed from the iterator
    assertFalse(iter.hasNext());

    // verify order of names
    long expected = 0;
    for (String name : allNames) {
      assertEquals(expected++, Long.parseLong(name, 36));
    }
  }
}

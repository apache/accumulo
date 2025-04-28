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
package org.apache.accumulo.manager.compaction.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

public class SizeTrackingTreeMapTest {
  @Test
  public void testSizeTracking() {
    List<String> computeSizeCalls = new ArrayList<>();
    var stmap = new SizeTrackingTreeMap<Integer,String>(val -> {
      computeSizeCalls.add(val);
      return val.length();
    });

    TreeMap<Integer,String> expected = new TreeMap<>();

    check(expected, stmap);
    assertEquals(List.of(), computeSizeCalls);

    stmap.put(3, "1234567890");
    expected.put(3, "1234567890");
    check(expected, stmap);
    assertEquals(List.of("1234567890"), computeSizeCalls);

    stmap.put(4, "12345");
    expected.put(4, "12345");
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345"), computeSizeCalls);

    // remove a key that does not exist
    stmap.remove(2);
    expected.remove(2);
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345"), computeSizeCalls);

    // remove a key that does exist
    stmap.remove(3);
    expected.remove(3);
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345"), computeSizeCalls);

    // update an existing key, should decrement the old size and increment the new size
    stmap.put(4, "123");
    expected.put(4, "123");
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345", "123"), computeSizeCalls);

    stmap.put(7, "123456789012345");
    expected.put(7, "123456789012345");
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345", "123", "123456789012345"), computeSizeCalls);

    stmap.put(11, "1234567");
    expected.put(11, "1234567");
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345", "123", "123456789012345", "1234567"),
        computeSizeCalls);

    assertEquals(expected.pollFirstEntry(), stmap.pollFirstEntry());
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345", "123", "123456789012345", "1234567"),
        computeSizeCalls);

    assertEquals(expected.pollLastEntry(), stmap.pollLastEntry());
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345", "123", "123456789012345", "1234567"),
        computeSizeCalls);

    expected.clear();
    stmap.clear();
    check(expected, stmap);
    assertEquals(List.of("1234567890", "12345", "123", "123456789012345", "1234567"),
        computeSizeCalls);
  }

  private void check(TreeMap<Integer,String> expected, SizeTrackingTreeMap<Integer,String> stmap) {
    long expectedDataSize = expected.values().stream().mapToLong(String::length).sum();
    assertEquals(expectedDataSize, stmap.dataSize());
    assertEquals(expected.size(), stmap.entrySize());
    assertEquals(expected.isEmpty(), stmap.isEmpty());
    assertEquals(expected.firstEntry(), stmap.firstEntry());
    if (expected.isEmpty()) {
      assertThrows(NoSuchElementException.class, stmap::lastKey);
    } else {
      assertEquals(expected.lastKey(), stmap.lastKey());
    }
  }
}

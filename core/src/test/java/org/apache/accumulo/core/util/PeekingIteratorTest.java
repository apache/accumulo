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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

public class PeekingIteratorTest {

  @Test
  public void testPeek() {
    Iterator<Integer> ints = IntStream.range(1, 11).iterator();
    PeekingIterator<Integer> peek = new PeekingIterator<>(ints);

    assertEquals(1, peek.peek());

    for (int i = 1; i < 11; i++) {
      assertTrue(peek.hasNext());
      assertEquals(i, peek.next());
      if (i == 10) {
        assertNull(peek.peek());
      } else {
        assertEquals(i + 1, peek.peek());
      }
    }

    assertFalse(peek.hasNext());
    assertNull(peek.next());
  }

  @Test
  public void testAdvance() {

    Iterator<Integer> ints = IntStream.range(1, 11).iterator();
    PeekingIterator<Integer> peek = new PeekingIterator<>(ints);

    assertEquals(1, peek.peek());
    assertTrue(peek.advanceTo((x) -> x == 4, 5));
    assertTrue(peek.hasNext());
    assertEquals(4, peek.next());
    assertEquals(5, peek.peek());

    // Advance the iterator 2 times looking for 7.
    // This will return false, but will advance
    // twice leaving the iterator at 6.
    assertFalse(peek.advanceTo((x) -> x == 7, 2));

    assertTrue(peek.hasNext());
    assertEquals(6, peek.next());

    assertTrue(peek.advanceTo((x) -> x == 8, 2));
    assertTrue(peek.hasNext());
    assertEquals(8, peek.next());

  }

}

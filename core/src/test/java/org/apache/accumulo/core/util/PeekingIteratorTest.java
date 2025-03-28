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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

public class PeekingIteratorTest {

  @Test
  public void testEmpty() {
    var iter = new PeekingIterator<>(Collections.emptyIterator());
    assertFalse(iter.hasNext());
    assertNull(iter.peek());
    assertThrows(NoSuchElementException.class, iter::next);
  }

  @Test
  public void testNotInitialized() {
    var iter = new PeekingIterator<>();
    assertThrows(IllegalStateException.class, iter::hasNext);
    assertThrows(IllegalStateException.class, iter::next);
    assertThrows(IllegalStateException.class, iter::peek);
  }

  @Test
  public void testNull() {
    List<String> list = new ArrayList<>();

    list.add("a");
    list.add(null);
    list.add("b");

    var iter = new PeekingIterator<>(list.iterator());

    assertTrue(iter.hasNext());
    assertEquals("a", iter.peek());
    assertEquals("a", iter.next());
    assertThrows(UnsupportedOperationException.class, iter::remove);
    assertTrue(iter.hasNext());
    assertNull(iter.peek());
    assertNull(iter.next());
    assertTrue(iter.hasNext());
    assertEquals("b", iter.peek());
    assertEquals("b", iter.next());
    assertFalse(iter.hasNext());
    assertThrows(NoSuchElementException.class, iter::next);

    iter = new PeekingIterator<>(list.iterator());
    iter.findWithin(Objects::isNull, 3);
    assertTrue(iter.hasNext());
    assertNull(iter.peek());
    assertNull(iter.next());
    assertTrue(iter.hasNext());
    assertEquals("b", iter.peek());
    assertEquals("b", iter.next());
    assertFalse(iter.hasNext());
    assertThrows(NoSuchElementException.class, iter::next);

    iter = new PeekingIterator<>(list.iterator());
    iter.findWithin(e -> Objects.equals(e, "b"), 3);
    assertTrue(iter.hasNext());
    assertEquals("b", iter.peek());
    assertEquals("b", iter.next());
    assertFalse(iter.hasNext());
    assertThrows(NoSuchElementException.class, iter::next);

    assertEquals(3, list.size());
  }

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
    assertThrows(NoSuchElementException.class, peek::next);
  }

  @Test
  public void testFind() {

    Iterator<Integer> ints = IntStream.range(1, 11).iterator();
    PeekingIterator<Integer> peek = new PeekingIterator<>(ints);

    assertThrows(IllegalArgumentException.class, () -> peek.findWithin(e -> false, -1));
    assertEquals(1, peek.peek());
    assertTrue(peek.findWithin((x) -> x != null && x == 4, 5));
    assertTrue(peek.hasNext());
    assertEquals(4, peek.next());
    assertEquals(5, peek.peek());

    // Advance the iterator 2 times looking for 7.
    // This will return false, but will advance
    // twice leaving the iterator at 6.
    assertFalse(peek.findWithin((x) -> x != null && x == 7, 2));

    assertTrue(peek.hasNext());
    assertEquals(6, peek.peek());
    assertEquals(6, peek.next());

    assertTrue(peek.findWithin((x) -> x != null && x == 8, 2));
    assertTrue(peek.hasNext());
    assertEquals(8, peek.next());

    // Try to advance past the end
    assertFalse(peek.findWithin((x) -> x != null && x == 7, 3));
    assertFalse(peek.hasNext());
    assertNull(peek.peek());
    assertThrows(NoSuchElementException.class, peek::next);

    // test finding that exhaust iterator w/o finding anything
    var peek2 = new PeekingIterator<>(ints);
    peek2.findWithin(x -> Objects.equals(x, 100), 50);
    assertFalse(peek2.hasNext());
    assertNull(peek2.peek());
    assertThrows(NoSuchElementException.class, peek2::next);

  }

}

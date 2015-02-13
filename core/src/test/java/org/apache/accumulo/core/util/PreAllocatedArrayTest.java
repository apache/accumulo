/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PreAllocatedArrayTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Test method for {@link org.apache.accumulo.core.util.PreAllocatedArray#PreAllocatedArray(int)}.
   */
  @Test
  public void testPreAllocatedArray() {
    PreAllocatedArray<String> strings = new PreAllocatedArray<>(5);
    assertEquals(5, strings.length);

    strings = new PreAllocatedArray<>(3);
    assertEquals(3, strings.length);

    strings = new PreAllocatedArray<>(0);
    assertEquals(0, strings.length);
  }

  @Test
  public void testPreAllocatedArray_Fail() {
    exception.expect(IllegalArgumentException.class);
    new PreAllocatedArray<String>(-5);
  }

  /**
   * Test method for {@link org.apache.accumulo.core.util.PreAllocatedArray#set(int, java.lang.Object)}.<br>
   * Test method for {@link org.apache.accumulo.core.util.PreAllocatedArray#get(int)}.<br>
   * Test method for {@link org.apache.accumulo.core.util.PreAllocatedArray#iterator()}.
   */
  @Test
  public void testSet() {
    int capacity = 5;
    PreAllocatedArray<String> strings = new PreAllocatedArray<>(capacity);
    assertEquals(capacity, strings.length);

    // everything else should be null
    strings.set(1, "a");
    strings.set(4, "b");
    assertEquals(capacity, strings.length);

    // overwrite
    String b = strings.set(4, "c");
    assertEquals("b", b);
    assertEquals(capacity, strings.length);

    Iterator<String> iter = strings.iterator();
    assertNull(iter.next()); // index 0
    assertEquals("a", iter.next()); // index 1
    assertNull(iter.next()); // index 2
    assertNull(iter.next()); // index 3
    assertEquals("c", iter.next()); // index 4
    assertFalse(iter.hasNext()); // index 5
  }

  @Test
  public void testSetIndexHigh() {
    PreAllocatedArray<String> strings = new PreAllocatedArray<>(3);
    strings.set(2, "in bounds");
    exception.expect(IndexOutOfBoundsException.class);
    strings.set(3, "out of bounds");
  }

  @Test
  public void testSetIndexNegative() {
    PreAllocatedArray<String> strings = new PreAllocatedArray<>(3);
    strings.set(0, "in bounds");
    exception.expect(IndexOutOfBoundsException.class);
    strings.set(-3, "out of bounds");
  }

  @Test
  public void testGetIndexHigh() {
    PreAllocatedArray<String> strings = new PreAllocatedArray<>(3);
    strings.get(2);
    exception.expect(IndexOutOfBoundsException.class);
    strings.get(3);
  }

  @Test
  public void testGetIndexNegative() {
    PreAllocatedArray<String> strings = new PreAllocatedArray<>(3);
    strings.get(0);
    exception.expect(IndexOutOfBoundsException.class);
    strings.get(-3);
  }
}

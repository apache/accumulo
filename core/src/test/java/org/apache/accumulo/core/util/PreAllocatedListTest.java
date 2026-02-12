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

import static org.apache.accumulo.core.util.LocalityGroupUtil.newPreallocatedList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
    justification = "lambda assertThrows testing exception thrown")
public class PreAllocatedListTest {

  /**
   * Test method for
   * {@link org.apache.accumulo.core.util.LocalityGroupUtil#newPreallocatedList(int)} (int)}.
   */
  @Test
  public void testPreAllocatedArray() {
    List<String> strings = newPreallocatedList(5);
    assertEquals(5, strings.size());

    strings = newPreallocatedList(3);
    assertEquals(3, strings.size());

    strings = newPreallocatedList(0);
    assertEquals(0, strings.size());
  }

  @Test
  public void testPreAllocatedArray_Fail() {
    assertThrows(NegativeArraySizeException.class, () -> newPreallocatedList(-5));
  }

  /**
   * Test method for {@link List#set(int, java.lang.Object)}.<br>
   * Test method for {@link List#get(int)}.<br>
   * Test method for {@link List#iterator()}.
   */
  @Test
  public void testSet() {
    int capacity = 5;
    List<String> strings = newPreallocatedList(capacity);
    assertEquals(capacity, strings.size());

    // everything else should be null
    strings.set(1, "a");
    strings.set(4, "b");
    assertEquals(capacity, strings.size());

    // overwrite
    String b = strings.set(4, "c");
    assertEquals("b", b);
    assertEquals(capacity, strings.size());

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
    List<String> strings = newPreallocatedList(3);
    strings.set(2, "in bounds");
    assertThrows(IndexOutOfBoundsException.class, () -> strings.set(3, "out of bounds"));
  }

  @Test
  public void testSetIndexNegative() {
    List<String> strings = newPreallocatedList(3);
    strings.set(0, "in bounds");
    assertThrows(IndexOutOfBoundsException.class, () -> strings.set(-3, "out of bounds"));
  }

  @Test
  public void testGetIndexHigh() {
    List<String> strings = newPreallocatedList(3);
    assertNull(strings.get(2));
    // spotbugs error suppressed at class level for lambda
    assertThrows(IndexOutOfBoundsException.class, () -> strings.get(3));
  }

  @Test
  public void testGetIndexNegative() {
    List<String> strings = newPreallocatedList(3);
    assertNull(strings.get(0));
    // spotbugs error suppressed at class level for lambda
    assertThrows(IndexOutOfBoundsException.class, () -> strings.get(-3));
  }

  @Test
  public void testIteratorRemove() {
    List<String> strings = newPreallocatedList(3);
    strings.set(1, "data");
    var iter = strings.iterator();
    for (int i = 0; i < 3; i++) {
      assertTrue(iter.hasNext());
      iter.next();
      assertThrows(UnsupportedOperationException.class, () -> iter.remove());
    }
    assertFalse(iter.hasNext());
    assertNull(strings.get(0));
    assertEquals("data", strings.get(1));
    assertNull(strings.get(2));
  }
}

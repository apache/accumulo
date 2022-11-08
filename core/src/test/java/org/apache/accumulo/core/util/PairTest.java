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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;

public class PairTest {

  /**
   * Test method for {@link org.apache.accumulo.core.util.Pair#hashCode()}.
   */
  @Test
  public void testHashMethod() {
    Pair<Integer,String> pair1 = new Pair<>(25, "twenty-five");
    Pair<Integer,String> pair2 = new Pair<>(25, "twenty-five");
    Pair<Integer,String> pair3 = new Pair<>(null, null);
    Pair<Integer,String> pair4 = new Pair<>(25, "twentyfive");
    Pair<Integer,String> pair5 = new Pair<>(225, "twenty-five");
    assertNotSame(pair1, pair2);
    assertEquals(pair1.hashCode(), pair2.hashCode());
    assertNotSame(pair2, pair3);
    assertNotEquals(pair1.hashCode(), pair4.hashCode());
    assertNotEquals(pair1.hashCode(), pair5.hashCode());
  }

  /**
   * Test method for {@link org.apache.accumulo.core.util.Pair#equals(java.lang.Object)}.
   */
  @Test
  public void testEqualsObject() {
    Pair<Integer,String> pair1 = new Pair<>(25, "twenty-five");
    Pair<Integer,String> pair2 = new Pair<>(25, "twenty-five");
    Pair<Integer,String> pair3 = new Pair<>(25, "twentyfive");
    Pair<Integer,String> null1 = null;

    assertEquals(pair1, pair1);
    assertEquals(pair2, pair1);
    assertNotEquals(pair1, pair3);

    // verify direct calls
    assertEquals(pair1, pair2);
    assertEquals(pair2, pair1);
    assertNotEquals(pair1, pair3);

    // check null
    assertEquals(null1, null1);
    assertNull(null1);
    assertNotEquals(pair1, null1);
  }

  /**
   * Test method for {@link org.apache.accumulo.core.util.Pair#getFirst()}.
   */
  @Test
  public void testGetFirst() {
    Pair<Integer,String> pair = new Pair<>(25, "twenty-five");
    assertEquals((Integer) 25, pair.getFirst());
  }

  /**
   * Test method for {@link org.apache.accumulo.core.util.Pair#getSecond()}.
   */
  @Test
  public void testGetSecond() {
    Pair<Integer,String> pair = new Pair<>(25, "twenty-five");
    assertEquals("twenty-five", pair.getSecond());
  }

  /**
   * Test method for {@link org.apache.accumulo.core.util.Pair#toString()}.
   */
  @Test
  public void testToString() {
    Pair<Integer,String> pair = new Pair<>(25, "twenty-five");
    assertEquals("(25,twenty-five)", pair.toString());
  }

  /**
   * Test method for
   * {@link org.apache.accumulo.core.util.Pair#toString(java.lang.String, java.lang.String, java.lang.String)}.
   */
  @Test
  public void testToStringStringStringString() {
    Pair<Integer,String> pair = new Pair<>(25, "twenty-five");
    assertEquals("---25~~~twenty-five+++", pair.toString("---", "~~~", "+++"));
  }

  /**
   * Test method for {@link org.apache.accumulo.core.util.Pair#toMapEntry()}.
   */
  @Test
  public void testToMapEntry() {
    Pair<Integer,String> pair = new Pair<>(10, "IO");

    Entry<Integer,String> entry = pair.toMapEntry();
    assertEquals(pair.getFirst(), entry.getKey());
    assertEquals(pair.getSecond(), entry.getValue());
  }

  /**
   * Test method for {@link org.apache.accumulo.core.util.Pair#swap()}.
   */
  @Test
  public void testSwap() {
    Pair<Integer,String> pair = new Pair<>(25, "twenty-five");
    assertEquals(pair, pair.swap().swap());
    Pair<String,Integer> pair2 = new Pair<>("twenty-five", 25);
    assertEquals(pair, pair2.swap());
    assertEquals(pair2, pair.swap());
  }

  /**
   * Test method for {@link org.apache.accumulo.core.util.Pair#fromEntry(java.util.Map.Entry)}.
   */
  @Test
  public void testFromEntry() {
    Entry<Integer,String> entry = new SimpleImmutableEntry<>(10, "IO");

    Pair<Integer,String> pair0 = Pair.fromEntry(entry);
    assertEquals(entry.getKey(), pair0.getFirst());
    assertEquals(entry.getValue(), pair0.getSecond());

    Pair<Object,Object> pair = Pair.fromEntry(entry);
    assertEquals(entry.getKey(), pair.getFirst());
    assertEquals(entry.getValue(), pair.getSecond());

    Pair<Number,CharSequence> pair2 = Pair.fromEntry(entry);
    assertEquals(entry.getKey(), pair2.getFirst());
    assertEquals(entry.getValue(), pair2.getSecond());
  }

}

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
package org.apache.accumulo.core.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArrayByteSequenceTest {

  ArrayByteSequence abs;
  byte[] data;

  @BeforeEach
  public void setUp() {
    data = new byte[] {'s', 'm', 'i', 'l', 'e', 's'};
    abs = new ArrayByteSequence(data);
  }

  @Test
  public void testInvalidByteBufferBounds0() {
    assertThrows(IllegalArgumentException.class, () -> abs = new ArrayByteSequence(data, -1, 0));
  }

  @Test
  public void testInvalidByteBufferBounds1() {
    assertThrows(IllegalArgumentException.class,
        () -> abs = new ArrayByteSequence(data, data.length + 1, 0));
  }

  @Test
  public void testInvalidByteBufferBounds2() {
    assertThrows(IllegalArgumentException.class, () -> abs = new ArrayByteSequence(data, 0, -1));
  }

  @Test
  public void testInvalidByteBufferBounds3() {
    assertThrows(IllegalArgumentException.class, () -> abs = new ArrayByteSequence(data, 6, 2));
  }

  @Test
  public void testInvalidByteAt0() {
    assertThrows(IllegalArgumentException.class, () -> abs.byteAt(-1));
  }

  @Test
  public void testInvalidByteAt1() {
    assertThrows(IllegalArgumentException.class, () -> abs.byteAt(data.length));
  }

  @Test
  public void testSubSequence() {
    assertEquals(0, abs.subSequence(0, 0).length());
    assertEquals("mile", abs.subSequence(1, 5).toString());
  }

  @Test
  public void testInvalidSubsequence0() {
    assertThrows(IllegalArgumentException.class, () -> abs.subSequence(5, 1));
  }

  @Test
  public void testInvalidSubsequence1() {
    assertThrows(IllegalArgumentException.class, () -> abs.subSequence(-1, 1));
  }

  @Test
  public void testInvalidSubsequence3() {
    assertThrows(IllegalArgumentException.class, () -> abs.subSequence(0, 10));
  }

  @Test
  public void testFromByteBuffer() {
    ByteBuffer bb = ByteBuffer.wrap(data, 1, 4);
    abs = new ArrayByteSequence(bb);

    assertEquals("mile", abs.toString());

    bb = bb.asReadOnlyBuffer();
    abs = new ArrayByteSequence(bb);

    assertEquals("mile", abs.toString());
  }

  @Test
  public void testToString() {
    assertEquals("", new ArrayByteSequence("").toString(),
        "String conversion should round trip correctly");
  }

}

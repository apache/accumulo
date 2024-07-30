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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;

public class ByteSequenceTest {

  @Test
  public void testCompareBytes() {
    ByteSequence e1 = new ArrayByteSequence("");
    ByteSequence a1 = new ArrayByteSequence("a");
    ByteSequence b1 = new ArrayByteSequence("b");
    ByteSequence abc1 = new ArrayByteSequence("abc");

    ByteSequence e2 = ByteSequence.of("");
    ByteSequence a2 = ByteSequence.of("a");
    ByteSequence b2 = ByteSequence.of("b");
    ByteSequence abc2 = ByteSequence.of("abc");

    assertLessThan(e1, a1);
    assertLessThan(e2, a1);
    assertLessThan(e1, a2);
    assertLessThan(e2, a2);

    assertLessThan(a1, b1);
    assertLessThan(a1, b2);
    assertLessThan(a2, b1);
    assertLessThan(a2, b2);

    assertLessThan(a1, abc1);
    assertLessThan(a2, abc1);
    assertLessThan(a1, abc2);
    assertLessThan(a2, abc2);

    assertLessThan(abc1, b1);
    assertLessThan(abc1, b2);
    assertLessThan(abc2, b1);
    assertLessThan(abc2, b2);
  }

  private void assertLessThan(ByteSequence lhs, ByteSequence rhs) {
    int result = ByteSequence.compareBytes(lhs, rhs);
    assertTrue(result < 0);
    result = ByteSequence.compareBytes(rhs, lhs);
    assertTrue(result > 0);
    result = lhs.compareTo(rhs);
    assertTrue(result < 0);
    result = rhs.compareTo(lhs);
    assertTrue(result > 0);
  }

  private static void checkEquals(ByteSequence a, ByteSequence b) {
    assertEquals(a, b);
    assertEquals(b, a);
    int hc1 = a.hashCode();
    int hc2 = b.hashCode();
    assertEquals(hc1, hc2);
    // calling hashCode a 2nd time should give the same results
    assertEquals(hc1, a.hashCode());
    assertEquals(hc1, b.hashCode());
    assertEquals(a.toString(), b.toString());
    assertEquals(a.length(), b.length());
    assertArrayEquals(a.toArray(), b.toArray());

    if (a.length() == 0 && a instanceof ImmutableByteSequence) {
      assertSame(ByteSequence.of(), a);
    }

    if (b.length() == 0 && b instanceof ImmutableByteSequence) {
      assertSame(ByteSequence.of(), b);
    }

    // Check compareTo when equals
    assertEquals(0, a.compareTo(b));
    assertEquals(0, b.compareTo(a));
  }

  private static void checkNotEquals(ByteSequence a, ByteSequence b) {
    assertNotEquals(a, b);
    assertNotEquals(b, a);
    assertNotEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a.toString(), b.toString());
    assertNotEquals(a.length(), b.length());
  }

  @Test
  public void testEqualsAndHashCode() {
    String[] testData = new String[] {"", "ab", "abc", "abcd"};
    for (String data : testData) {
      ByteSequence bs1 = new ArrayByteSequence(data);
      ByteSequence bs2 = ByteSequence.of(data);

      checkEquals(bs1, bs2);

      for (String data2 : testData) {
        if (data.equals(data2)) {
          continue;
        }
        ByteSequence other1 = new ArrayByteSequence(data2);
        ByteSequence other2 = ByteSequence.of(data2);

        checkNotEquals(bs1, other1);
        checkNotEquals(bs1, other2);
        checkNotEquals(bs2, other1);
        checkNotEquals(bs2, other2);

      }

      assertNotEquals(bs1, data);
      assertNotEquals(bs2, data);

      ByteSequence bs3 = new ArrayByteSequence(data.getBytes(UTF_8));
      ByteSequence bs4 = ByteSequence.of(data.getBytes(UTF_8));

      checkEquals(bs1, bs3);
      checkEquals(bs1, bs4);
      checkEquals(bs2, bs3);
      checkEquals(bs2, bs4);

      ByteSequence bs5 = new ArrayByteSequence(data.getBytes(UTF_8), 0, data.length());
      ByteSequence bs6 = ByteSequence.of(data.getBytes(UTF_8), 0, data.length());

      checkEquals(bs1, bs5);
      checkEquals(bs1, bs6);
      checkEquals(bs2, bs5);
      checkEquals(bs2, bs6);

      ByteSequence bs7 = new ArrayByteSequence(ByteBuffer.wrap(data.getBytes(UTF_8)));
      ByteSequence bs8 = ByteSequence.of(ByteBuffer.wrap(data.getBytes(UTF_8)));

      checkEquals(bs1, bs7);
      checkEquals(bs1, bs8);
      checkEquals(bs2, bs7);
      checkEquals(bs2, bs8);

      ByteSequence bs9 =
          new ArrayByteSequence(ByteBuffer.wrap(data.getBytes(UTF_8)).asReadOnlyBuffer());
      ByteSequence bs10 = ByteSequence.of(ByteBuffer.wrap(data.getBytes(UTF_8)).asReadOnlyBuffer());

      checkEquals(bs1, bs9);
      checkEquals(bs1, bs10);
      checkEquals(bs2, bs9);
      checkEquals(bs2, bs10);

      checkEquals(bs1, new CustomByteSequence(data));
      checkEquals(bs2, new CustomByteSequence(data));

      checkEquals(bs1, ByteSequence.of(new CustomByteSequence(data)));
      checkEquals(bs2, new ArrayByteSequence(new CustomByteSequence(data)));
    }
  }

  private void checkImmutable(ByteSequence byteSequence) {
    assertFalse(byteSequence.isBackedByArray());
    assertNotSame(byteSequence.toArray(), byteSequence.toArray());
    assertThrows(UnsupportedOperationException.class, byteSequence::getBackingArray);
    assertThrows(UnsupportedOperationException.class, byteSequence::offset);
  }

  @Test
  public void testImmutable() {
    String[] testData = new String[] {"ab", "abc", "abcd"};
    for (String data : testData) {
      checkImmutable(ByteSequence.of(data.getBytes(UTF_8)));
      checkImmutable(ByteSequence.of(data));
      checkImmutable(ByteSequence.of(data.getBytes(UTF_8), 0, data.length()));
      checkImmutable(ByteSequence.of(new ArrayByteSequence(data)));
      checkImmutable(ByteSequence.of(ByteSequence.of(data)));
      checkImmutable(ByteSequence.of(ByteBuffer.wrap(data.getBytes(UTF_8))));
    }
  }

  @Test
  public void testByteAt() {
    for (var byteSequence : List.of(ByteSequence.of("abc"), new ArrayByteSequence("abc"))) {
      assertEquals('a', byteSequence.byteAt(0));
      assertEquals('b', byteSequence.byteAt(1));
      assertEquals('c', byteSequence.byteAt(2));
      assertThrows(IndexOutOfBoundsException.class, () -> byteSequence.byteAt(-1));
      assertThrows(IndexOutOfBoundsException.class, () -> byteSequence.byteAt(3));
    }

    for (int index : List.of(-1, 0, 1, 2)) {
      assertThrows(IndexOutOfBoundsException.class, () -> ByteSequence.of().byteAt(index));
      assertThrows(IndexOutOfBoundsException.class, () -> new ArrayByteSequence("").byteAt(index));
    }
  }

  @Test
  public void testToArray() {
    for (var byteSequence : List.of(ByteSequence.of("abc"), new ArrayByteSequence("abc"))) {
      assertArrayEquals("abc".getBytes(UTF_8), byteSequence.toArray());
    }
  }

  @Test
  public void testBounds() {
    assertThrows(IndexOutOfBoundsException.class, () -> ByteSequence.of(new byte[0], 0, 5));
    assertThrows(IndexOutOfBoundsException.class, () -> ByteSequence.of(new byte[7], 5, 5));
    assertThrows(IndexOutOfBoundsException.class, () -> ByteSequence.of(new byte[7], 5, 3));
  }

  @Test
  public void testSubsequence() {
    for (var bs : List.of(ByteSequence.of("abc"), new ArrayByteSequence("abc"))) {
      checkEquals(ByteSequence.of("bc"), bs.subSequence(1, 3));
      checkEquals(ByteSequence.of("ab"), bs.subSequence(0, 2));
      checkEquals(ByteSequence.of("abc"), bs.subSequence(0, 3));

      assertThrows(IndexOutOfBoundsException.class, () -> bs.subSequence(2, 1));
      assertThrows(IndexOutOfBoundsException.class, () -> bs.subSequence(0, 5));
      assertThrows(IndexOutOfBoundsException.class, () -> bs.subSequence(3, 4));
      assertThrows(IndexOutOfBoundsException.class, () -> bs.subSequence(-4, 2));
      assertThrows(IndexOutOfBoundsException.class, () -> bs.subSequence(-2, -1));
    }

    assertSame(ByteSequence.of(), ByteSequence.of().subSequence(0, 0));
    assertThrows(IndexOutOfBoundsException.class, () -> ByteSequence.of().subSequence(0, 1));
  }

  // test passing an array backed ByteSequence with a non-zero offset to different methods that take
  // ByteSequence
  @Test
  public void testNonZeroOffset() {
    var data = "qwerty".getBytes(UTF_8);
    var abs = new ArrayByteSequence(new byte[0]);
    abs.reset(data, 2, 3);
    assertEquals(2, abs.offset());
    var copy = ByteSequence.of(abs);
    assertEquals(ByteSequence.of("ert"), copy);
    var copy2 = new ArrayByteSequence(abs);
    assertEquals(copy, copy2);
  }

  static class CustomByteSequence extends ByteSequence {

    private static final long serialVersionUID = 1L;

    private final String data;

    CustomByteSequence(String data) {
      this.data = data;
    }

    @Override
    public byte byteAt(int i) {
      char c = data.charAt(i);
      Preconditions.checkState(c < 128);
      return (byte) c;
    }

    @Override
    public int length() {
      return data.length();
    }

    @Override
    public ByteSequence subSequence(int start, int end) {
      return new CustomByteSequence(data.substring(start, end));
    }

    @Override
    public byte[] toArray() {
      return data.getBytes(UTF_8);
    }

    @Override
    public boolean isBackedByArray() {
      return false;
    }

    @Override
    public byte[] getBackingArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int offset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return data;
    }
  }
}

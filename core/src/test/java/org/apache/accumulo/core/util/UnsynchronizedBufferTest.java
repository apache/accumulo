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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.Test;

public class UnsynchronizedBufferTest {

  @Test
  public void testByteBufferConstructor() {
    byte[] test = "0123456789".getBytes(UTF_8);

    ByteBuffer bb1 = ByteBuffer.wrap(test);
    UnsynchronizedBuffer.Reader ub = new UnsynchronizedBuffer.Reader(bb1);
    byte[] buf = new byte[10];
    ub.readBytes(buf);
    assertEquals("0123456789", new String(buf, UTF_8));

    ByteBuffer bb2 = ByteBuffer.wrap(test, 3, 5);

    ub = new UnsynchronizedBuffer.Reader(bb2);
    buf = new byte[5];
    // should read data from offset 3 where the byte buffer starts
    ub.readBytes(buf);
    assertEquals("34567", new String(buf, UTF_8));

    buf = new byte[6];
    // the byte buffer has the extra byte, but should not be able to read it...
    final UnsynchronizedBuffer.Reader finalUb = ub;
    final byte[] finalBuf = buf;
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> finalUb.readBytes(finalBuf));
  }

  @Test
  public void testWriteVMethods() throws Exception {
    // writeV methods use an extra byte for length, unless value is only one byte
    // Integer.MAX_VALUE = 0x7fffffff
    testInteger(0x7fffffff, 4 + 1);
    testInteger(0x7fffff, 3 + 1);
    testInteger(0x7fff, 2 + 1);
    testInteger(0x7f, 1);

    // Long.MAX_VALUE = 0x7fffffffffffffffL
    testLong(0x7fffffffffffffffL, 8 + 1);
    testLong(0x7fffffffffffffL, 7 + 1);
    testLong(0x7fffffffffffL, 6 + 1);
    testLong(0x7fffffffffL, 5 + 1);
    testLong(0x7fffffffL, 4 + 1);
    testLong(0x7fffffL, 3 + 1);
    testLong(0x7fffL, 2 + 1);
    testLong(0x7fL, 1);
  }

  private void testInteger(int value, int length) throws Exception {
    byte[] integerBuffer = new byte[5];
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      UnsynchronizedBuffer.writeVInt(dos, integerBuffer, value);
      dos.flush();
      assertEquals(length, baos.toByteArray().length);
    }
  }

  private void testLong(long value, int length) throws Exception {
    byte[] longBuffer = new byte[9];
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      UnsynchronizedBuffer.writeVLong(dos, longBuffer, value);
      dos.flush();
      assertEquals(length, baos.toByteArray().length);
    }
  }

  @Test
  public void compareWithWritableUtils() throws Exception {
    byte[] hadoopBytes;
    byte[] accumuloBytes;
    int oneByteInt = 0x7f;
    int threeByteInt = 0x7fff;
    long sixByteLong = 0x7fffffffffL;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      WritableUtils.writeVInt(dos, oneByteInt);
      WritableUtils.writeVInt(dos, threeByteInt);
      WritableUtils.writeVLong(dos, sixByteLong);
      dos.flush();
      hadoopBytes = baos.toByteArray();
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      UnsynchronizedBuffer.writeVInt(dos, new byte[5], oneByteInt);
      UnsynchronizedBuffer.writeVInt(dos, new byte[5], threeByteInt);
      UnsynchronizedBuffer.writeVLong(dos, new byte[9], sixByteLong);
      dos.flush();
      accumuloBytes = baos.toByteArray();
    }
    assertArrayEquals(hadoopBytes, accumuloBytes,
        "The byte array written to by UnsynchronizedBuffer is not equal to WritableUtils");
  }

  @Test
  public void testNextArraySizeNegative() {
    assertThrows(IllegalArgumentException.class, () -> UnsynchronizedBuffer.nextArraySize(-1));
  }

  @Test
  public void testNextArraySize() {
    // 0 <= size <= 2^0
    assertEquals(1, UnsynchronizedBuffer.nextArraySize(0));
    assertEquals(1, UnsynchronizedBuffer.nextArraySize(1));

    // 2^0 < size <= 2^1
    assertEquals(2, UnsynchronizedBuffer.nextArraySize(2));

    // 2^1 < size <= 2^30
    for (int exp = 1; exp < 30; ++exp) {
      // 2^exp < size <= 2^(exp+1) (for all exp: [1,29])
      int nextExp = exp + 1;
      assertEquals(1 << nextExp, UnsynchronizedBuffer.nextArraySize((1 << exp) + 1));
      assertEquals(1 << nextExp, UnsynchronizedBuffer.nextArraySize(1 << nextExp));
    }
    // 2^30 < size < Integer.MAX_VALUE
    assertEquals(Integer.MAX_VALUE - 8, UnsynchronizedBuffer.nextArraySize((1 << 30) + 1));
    assertEquals(Integer.MAX_VALUE - 8, UnsynchronizedBuffer.nextArraySize(Integer.MAX_VALUE - 9));
    assertEquals(Integer.MAX_VALUE - 8, UnsynchronizedBuffer.nextArraySize(Integer.MAX_VALUE - 8));
    assertEquals(Integer.MAX_VALUE - 8, UnsynchronizedBuffer.nextArraySize(Integer.MAX_VALUE));
  }

}

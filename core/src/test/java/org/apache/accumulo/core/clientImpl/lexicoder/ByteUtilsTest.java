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
package org.apache.accumulo.core.clientImpl.lexicoder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ByteUtilsTest {

  private final byte[] empty = new byte[0];
  private final byte[] noSplits = "nosplits".getBytes();
  private final byte[] splitAt5 = ("1234" + (char) 0x00 + "56789").getBytes();

  @Test
  public void testSplit() {
    byte[][] result;

    // always returns the original array itself
    result = ByteUtils.split(empty);
    assertEquals(1, result.length);
    assertArrayEquals(empty, result[0]);

    result = ByteUtils.split(noSplits);
    assertEquals(1, result.length);
    assertArrayEquals(noSplits, result[0]);

    result = ByteUtils.split(splitAt5);
    assertEquals(2, result.length);
    assertArrayEquals("1234".getBytes(), result[0]);
    assertArrayEquals("56789".getBytes(), result[1]);
  }

  @Test
  public void testSplitWithOffset() {
    int offset;
    byte[][] result;

    // still see both splits
    offset = 4;
    result = ByteUtils.split(splitAt5, offset, splitAt5.length - offset);
    assertEquals(2, result.length);
    assertArrayEquals(empty, result[0]);
    assertArrayEquals("56789".getBytes(), result[1]);

    // should only see 1 split at this offset
    offset = 5;
    result = ByteUtils.split(splitAt5, offset, splitAt5.length - offset);
    assertEquals(1, result.length);
    assertArrayEquals("56789".getBytes(), result[0]);

    // still one split, but smaller ending
    offset = 5;
    int len = splitAt5.length - offset - 1;
    result = ByteUtils.split(splitAt5, offset, len);
    assertEquals(1, result.length);
    assertArrayEquals("5678".getBytes(), result[0]);
  }

  @Test
  public void testEscape() {
    byte[] bytes = {0x00, 0x01};
    byte[] escaped = ByteUtils.escape(bytes);
    assertArrayEquals(bytes, ByteUtils.unescape(escaped));

    // no escaped bytes found so returns the input
    byte[] notEscaped = {0x02, 0x02, 0x02};
    assertArrayEquals(notEscaped, ByteUtils.unescape(notEscaped));
  }

  @Test
  public void testIllegalArgument() {
    // incomplete bytes would cause an ArrayIndexOutOfBounds in the past
    byte[] errorBytes = {0x01};
    assertThrows(IllegalArgumentException.class, () -> ByteUtils.unescape(errorBytes));
  }
}

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
package org.apache.accumulo.core.client.lexicoder.impl;

import org.junit.Assert;
import org.junit.Test;

public class ByteUtilsTest {

  private final byte[] empty = new byte[0];
  private final byte[] noSplits = "nosplits".getBytes();
  private final byte[] splitAt5 = ("1234" + (char) 0x00 + "56789").getBytes();

  @Test
  public void testSplit() throws Exception {
    byte[][] result;

    // always returns the original array itself
    result = ByteUtils.split(empty);
    Assert.assertEquals(1, result.length);
    Assert.assertArrayEquals(empty, result[0]);

    result = ByteUtils.split(noSplits);
    Assert.assertEquals(1, result.length);
    Assert.assertArrayEquals(noSplits, result[0]);

    result = ByteUtils.split(splitAt5);
    Assert.assertEquals(2, result.length);
    Assert.assertArrayEquals("1234".getBytes(), result[0]);
    Assert.assertArrayEquals("56789".getBytes(), result[1]);
  }

  public void testSplitWithOffset() {
    int offset;
    byte[][] result;

    // still see both splits
    offset = 4;
    result = ByteUtils.split(splitAt5, offset, splitAt5.length - offset);
    Assert.assertEquals(2, result.length);
    Assert.assertArrayEquals(empty, result[0]);
    Assert.assertArrayEquals("56789".getBytes(), result[1]);

    // should only see 1 split at this offset
    offset = 5;
    result = ByteUtils.split(splitAt5, offset, splitAt5.length - offset);
    Assert.assertEquals(1, result.length);
    Assert.assertArrayEquals("56789".getBytes(), result[0]);

    // still one split, but smaller ending
    offset = 5;
    int len = splitAt5.length - offset - 1;
    result = ByteUtils.split(splitAt5, offset, len);
    Assert.assertEquals(1, result.length);
    Assert.assertArrayEquals("5678".getBytes(), result[0]);
  }
}

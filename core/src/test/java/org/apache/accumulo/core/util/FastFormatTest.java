/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

public class FastFormatTest {

  @Test
  public void testArrayOffset() {

    byte[] str = new byte[8];

    Arrays.fill(str, (byte) '-');
    int len = FastFormat.toZeroPaddedString(str, 4, 64L, 1, 16, new byte[] {});
    assertEquals(2, len);
    assertEquals("----40--", new String(str, UTF_8));

    Arrays.fill(str, (byte) '-');
    len = FastFormat.toZeroPaddedString(str, 4, 64L, 2, 16, new byte[] {});
    assertEquals(2, len);
    assertEquals("----40--", new String(str, UTF_8));

    Arrays.fill(str, (byte) '-');
    len = FastFormat.toZeroPaddedString(str, 4, 64L, 3, 16, new byte[] {});
    assertEquals(3, len);
    assertEquals("----040-", new String(str, UTF_8));

    Arrays.fill(str, (byte) '-');
    len = FastFormat.toZeroPaddedString(str, 4, 64L, 1, 16, new byte[] {'P'});
    assertEquals(3, len);
    assertEquals("----P40-", new String(str, UTF_8));

    Arrays.fill(str, (byte) '-');
    len = FastFormat.toZeroPaddedString(str, 4, 64L, 2, 16, new byte[] {'P'});
    assertEquals(3, len);
    assertEquals("----P40-", new String(str, UTF_8));

    Arrays.fill(str, (byte) '-');
    len = FastFormat.toZeroPaddedString(str, 4, 64L, 3, 16, new byte[] {'P'});
    assertEquals(4, len);
    assertEquals("----P040", new String(str, UTF_8));

    Arrays.fill(str, (byte) '-');
    len = FastFormat.toZeroPaddedString(str, 2, 64L, 4, 16, new byte[] {'P'});
    assertEquals(5, len);
    assertEquals("--P0040-", new String(str, UTF_8));
  }

  @Test
  public void testFormat() {
    assertEquals("100",
        new String(FastFormat.toZeroPaddedString(1296, 1, 36, new byte[] {}), UTF_8));
    assertEquals("100",
        new String(FastFormat.toZeroPaddedString(1296, 2, 36, new byte[] {}), UTF_8));
    assertEquals("100",
        new String(FastFormat.toZeroPaddedString(1296, 3, 36, new byte[] {}), UTF_8));
    assertEquals("0100",
        new String(FastFormat.toZeroPaddedString(1296, 4, 36, new byte[] {}), UTF_8));
    assertEquals("00100",
        new String(FastFormat.toZeroPaddedString(1296, 5, 36, new byte[] {}), UTF_8));

    assertEquals("PA100",
        new String(FastFormat.toZeroPaddedString(1296, 1, 36, new byte[] {'P', 'A'}), UTF_8));
    assertEquals("PA100",
        new String(FastFormat.toZeroPaddedString(1296, 2, 36, new byte[] {'P', 'A'}), UTF_8));
    assertEquals("PA100",
        new String(FastFormat.toZeroPaddedString(1296, 3, 36, new byte[] {'P', 'A'}), UTF_8));
    assertEquals("PA0100",
        new String(FastFormat.toZeroPaddedString(1296, 4, 36, new byte[] {'P', 'A'}), UTF_8));
    assertEquals("PA00100",
        new String(FastFormat.toZeroPaddedString(1296, 5, 36, new byte[] {'P', 'A'}), UTF_8));
    assertEquals("PA000100",
        new String(FastFormat.toZeroPaddedString(1296, 6, 36, new byte[] {'P', 'A'}), UTF_8));
    assertEquals("PA0000100",
        new String(FastFormat.toZeroPaddedString(1296, 7, 36, new byte[] {'P', 'A'}), UTF_8));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegative1() {
    FastFormat.toZeroPaddedString(-5, 1, 36, new byte[] {});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegative2() {
    byte[] str = new byte[8];
    FastFormat.toZeroPaddedString(str, 0, -5, 1, 36, new byte[] {});
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testArrayOutOfBounds() {
    byte[] str = new byte[8];
    FastFormat.toZeroPaddedString(str, 4, 64L, 4, 16, new byte[] {'P'});
  }
}

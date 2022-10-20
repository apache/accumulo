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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.apache.accumulo.core.fate.FateTxId;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testNegative1() {
    assertThrows(IllegalArgumentException.class,
        () -> FastFormat.toZeroPaddedString(-5, 1, 36, new byte[] {}));
  }

  @Test
  public void testNegative2() {
    byte[] str = new byte[8];
    assertThrows(IllegalArgumentException.class,
        () -> FastFormat.toZeroPaddedString(str, 0, -5, 1, 36, new byte[] {}));
  }

  @Test
  public void testArrayOutOfBounds() {
    byte[] str = new byte[8];
    assertThrows(ArrayIndexOutOfBoundsException.class,
        () -> FastFormat.toZeroPaddedString(str, 4, 64L, 4, 16, new byte[] {'P'}));
  }

  @Test
  public void testHexString() {
    final String PREFIX = "FATE[";
    final String SUFFIX = "]";
    String formattedTxId = FateTxId.formatTid(64L);
    String hexStr = FastFormat.toHexString(PREFIX, 64L, SUFFIX);
    assertEquals(formattedTxId, hexStr);
    long txid = FateTxId.fromString("FATE[2e429160071c63d8]");
    assertEquals("FATE[2e429160071c63d8]", FastFormat.toHexString(PREFIX, txid, SUFFIX));
    assertEquals(String.format("%016x", 64L), FastFormat.toHexString(64L));
    assertEquals(String.format("%016x", 0X2e429160071c63d8L),
        FastFormat.toHexString(0X2e429160071c63d8L));

    assertEquals("-0000000000000040-", FastFormat.toHexString("-", 64L, "-"));
    assertEquals("-00000000075bcd15", FastFormat.toHexString("-", 123456789L, ""));
    assertEquals("000000000000000a", FastFormat.toHexString(0XaL));
    assertEquals("000000000000000a", FastFormat.toHexString(10L));
    assertEquals("0000000000000009", FastFormat.toHexString(9L));
    assertEquals("0000000000000000", FastFormat.toHexString(0L));
  }

  @Test
  public void testZeroPaddedHex() {
    byte[] str = new byte[8];
    Arrays.fill(str, (byte) '-');
    str = FastFormat.toZeroPaddedHex(123456789L);
    assertEquals(16, str.length);
    assertEquals("00000000075bcd15", new String(str, UTF_8));

    Arrays.fill(str, (byte) '-');
    str = FastFormat.toZeroPaddedHex(0X2e429160071c63d8L);
    assertEquals(16, str.length);
    assertEquals("2e429160071c63d8", new String(str, UTF_8));
  }
}

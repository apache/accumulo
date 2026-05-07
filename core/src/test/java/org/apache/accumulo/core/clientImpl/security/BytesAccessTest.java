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
package org.apache.accumulo.core.clientImpl.security;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.clientImpl.access.BytesAccess;
import org.junit.jupiter.api.Test;

public class BytesAccessTest {
  @Test
  public void testIso8859() {
    // BytesAccess heavily depends on a 1:1 conversion of data from byte[] to string when using
    // ISO_8859_1. This test validates those assumptions.

    byte[] data = new byte[256];
    for (int i = 0; i < 256; i++) {
      data[i] = (byte) i;
    }

    var str = new String(data, ISO_8859_1);

    assertEquals(256, str.length());
    var chars = str.toCharArray();
    assertEquals(256, chars.length);
    for (int i = 0; i < 256; i++) {
      assertEquals(i, str.charAt(i));
      assertEquals(i, chars[i]);
    }

    byte[] data2 = str.getBytes(ISO_8859_1);
    assertArrayEquals(data, data2);
  }

  @Test
  public void testQuote() {
    byte[] data = new byte[256];
    for (int i = 0; i < 256; i++) {
      data[i] = (byte) i;
    }

    var quoted = BytesAccess.quote(data);
    assertEquals(260, quoted.length);
    assertEquals('"', quoted[0]);
    assertEquals('"', quoted[259]);
    int expected = 0;
    for (int i = 1; i < 259; i++) {
      if (quoted[i] == '\\') {
        i++;
        assertTrue('"' == quoted[i] || '\\' == quoted[i]);
      }
      assertEquals(expected, 0xff & quoted[i]);
      expected++;
    }
  }

  @Test
  public void testFindAuths() {
    byte[] exp = new byte[] {'"', 0, 1, '"', '&', 'A', 'B', '&', '"', 3, 4, 5, '"'};

    List<byte[]> seenAuths = new ArrayList<>();
    BytesAccess.findAuthorizations(exp, seenAuths::add);

    assertEquals(3, seenAuths.size());
    assertArrayEquals(new byte[] {0, 1}, seenAuths.get(0));
    assertArrayEquals(new byte[] {'A', 'B'}, seenAuths.get(1));
    assertArrayEquals(new byte[] {3, 4, 5}, seenAuths.get(2));
  }
}

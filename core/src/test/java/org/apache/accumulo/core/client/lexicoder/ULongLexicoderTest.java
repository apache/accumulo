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
package org.apache.accumulo.core.client.lexicoder;

import java.util.Arrays;

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoderTest;

public class ULongLexicoderTest extends AbstractLexicoderTest {

  public void testEncoding() {
    ULongLexicoder ull = new ULongLexicoder();

    assertEqualsB(ull.encode(0l), new byte[] {0x00});
    assertEqualsB(ull.encode(0x01l), new byte[] {0x01, 0x01});
    assertEqualsB(ull.encode(0x1234l), new byte[] {0x02, 0x12, 0x34});
    assertEqualsB(ull.encode(0x123456l), new byte[] {0x03, 0x12, 0x34, 0x56});
    assertEqualsB(ull.encode(0x12345678l), new byte[] {0x04, 0x12, 0x34, 0x56, 0x78});
    assertEqualsB(ull.encode(0x1234567890l), new byte[] {0x05, 0x12, 0x34, 0x56, 0x78, (byte) 0x90});
    assertEqualsB(ull.encode(0x1234567890abl), new byte[] {0x06, 0x12, 0x34, 0x56, 0x78, (byte) 0x90, (byte) 0xab});
    assertEqualsB(ull.encode(0x1234567890abcdl), new byte[] {0x07, 0x12, 0x34, 0x56, 0x78, (byte) 0x90, (byte) 0xab, (byte) 0xcd});
    assertEqualsB(ull.encode(0x1234567890abcdefl), new byte[] {0x08, 0x12, 0x34, 0x56, 0x78, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef});

    assertEqualsB(ull.encode(0xff34567890abcdefl), new byte[] {0x09, 0x34, 0x56, 0x78, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef});
    assertEqualsB(ull.encode(0xffff567890abcdefl), new byte[] {0x0a, 0x56, 0x78, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef});
    assertEqualsB(ull.encode(0xffffff7890abcdefl), new byte[] {0x0b, 0x78, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef});
    assertEqualsB(ull.encode(0xffffffff90abcdefl), new byte[] {0x0c, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef});
    assertEqualsB(ull.encode(0xffffffffffabcdefl), new byte[] {0x0d, (byte) 0xab, (byte) 0xcd, (byte) 0xef});
    assertEqualsB(ull.encode(0xffffffffffffcdefl), new byte[] {0x0e, (byte) 0xcd, (byte) 0xef});
    assertEqualsB(ull.encode(0xffffffffffffffefl), new byte[] {0x0f, (byte) 0xef});

    assertEqualsB(ull.encode(-1l), new byte[] {16});
  }

  public void testSortOrder() {
    // only testing non negative
    assertSortOrder(new ULongLexicoder(),
        Arrays.asList(0l, 0x01l, 0x1234l, 0x123456l, 0x12345678l, 0x1234567890l, 0x1234567890abl, 0x1234567890abcdl, 0x1234567890abcdefl, Long.MAX_VALUE));
  }

  public void testDecodes() {
    assertDecodes(new ULongLexicoder(), Long.MIN_VALUE);
    assertDecodes(new ULongLexicoder(), -1l);
    assertDecodes(new ULongLexicoder(), 0l);
    assertDecodes(new ULongLexicoder(), 1l);
    assertDecodes(new ULongLexicoder(), 2l);
    assertDecodes(new ULongLexicoder(), Long.MAX_VALUE);
  }

}

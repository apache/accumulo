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

import org.apache.accumulo.core.client.lexicoder.LexicoderTest;
import org.apache.commons.lang.ArrayUtils;

/**
 * Assists in Testing classes that extend {@link org.apache.accumulo.core.client.lexicoder.AbstractEncoder}. It references methods not formally defined in the
 * {@link org.apache.accumulo.core.client.lexicoder.Lexicoder} interface.
 *
 * @since 1.7.0
 */
public abstract class AbstractLexicoderTest extends LexicoderTest {

  public static <T> void assertDecodes(AbstractLexicoder<T> lexicoder, T expected) {
    LexicoderTest.assertDecodes(lexicoder, expected);

    byte[] encoded = lexicoder.encode(expected);

    assertOutOfBoundsFails(lexicoder, encoded);

    // munge bytes at start and end, then use offset and length to decode
    final byte[] combined = ArrayUtils.addAll(ArrayUtils.addAll(START_PAD, encoded), END_PAD);

    int offset = START_PAD.length;
    int len = encoded.length;
    T result = lexicoder.decode(combined, offset, len);
    assertEquals(expected, result);
  }

  public void assertDecodesB(AbstractLexicoder<byte[]> lexicoder, byte[] expected) {
    super.assertDecodesB(lexicoder, expected);

    byte[] encoded = lexicoder.encode(expected);

    assertOutOfBoundsFails(lexicoder, encoded);

    // munge bytes at start and end, then use offset and length to decode
    final byte[] combined = ArrayUtils.addAll(ArrayUtils.addAll(START_PAD, encoded), END_PAD);

    int offset = START_PAD.length;
    int len = encoded.length;
    byte[] result = lexicoder.decode(combined, offset, len);
    assertEqualsB(expected, result);
  }

  protected static <T> void assertOutOfBoundsFails(AbstractLexicoder<T> lexicoder, byte[] encoded) {
    // decode null; should fail
    try {
      lexicoder.decode(null, 0, encoded.length);
      fail("Should throw on null bytes.");
    } catch (NullPointerException e) {}

    // decode out of bounds, expect an exception
    try {
      lexicoder.decode(encoded, 0, encoded.length + 1);
      fail("Should throw on exceeding length.");
    } catch (IllegalArgumentException e) {}

    try {
      lexicoder.decode(encoded, -1, encoded.length);
      fail("Should throw on negative offset.");
    } catch (IllegalArgumentException e) {}

    try {
      lexicoder.decode(encoded, 0, -1);
      fail("Should throw on negative length.");
    } catch (IllegalArgumentException e) {}

    try {
      lexicoder.decode(encoded, 1, -1);
      fail("Should throw on negative length, even if (offset+len) is within bounds.");
    } catch (IllegalArgumentException e) {}
  }
}

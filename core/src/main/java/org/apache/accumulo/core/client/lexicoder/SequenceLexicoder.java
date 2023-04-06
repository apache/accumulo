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
package org.apache.accumulo.core.client.lexicoder;

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.concat;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.escape;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.split;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.unescape;

import java.util.ArrayList;
import java.util.List;

/**
 * A Lexicoder to encode/decode a Java List to/from a byte array where the concatenation of each
 * encoded element sorts lexicographically.
 *
 * Note: Unlike {@link ListLexicoder}, this implementation supports empty lists.
 *
 * The lists are encoded with the elements separated by null (0x0) bytes, which null bytes appearing
 * in the elements escaped as two 0x1 bytes, and 0x1 bytes appearing in the elements escaped as 0x1
 * and 0x2 bytes. The list is terminated with a final delimiter after the last element, with no
 * bytes following it. An empty list is represented as an empty byte array, with no delimiter,
 * whereas a list with a single empty element is represented as a single terminating delimiter.
 *
 * @since 2.0.0
 * @param <E> list element type.
 */
public class SequenceLexicoder<E> extends AbstractLexicoder<List<E>> {

  private static final byte[] EMPTY_ELEMENT = new byte[0];
  private final Lexicoder<E> elementLexicoder;

  /**
   * Primary constructor.
   *
   * @param elementLexicoder Lexicoder to apply to elements.
   */
  public SequenceLexicoder(final Lexicoder<E> elementLexicoder) {
    this.elementLexicoder = requireNonNull(elementLexicoder, "elementLexicoder");
  }

  /**
   * {@inheritDoc}
   *
   * @return a byte array containing the concatenation of each element in the list encoded.
   */
  @Override
  public byte[] encode(final List<E> v) {
    final byte[][] encElements = new byte[v.size() + 1][];
    int index = 0;
    for (final E element : v) {
      encElements[index++] = escape(elementLexicoder.encode(element));
    }
    encElements[v.size()] = EMPTY_ELEMENT;
    return concat(encElements);
  }

  @Override
  protected List<E> decodeUnchecked(final byte[] b, final int offset, final int len) {
    final byte[][] escapedElements = split(b, offset, len);
    assert escapedElements.length > 0
        : "ByteUtils.split always returns a minimum of 1 element, even for empty input";
    // There should be no bytes after the final delimiter. Lack of delimiter indicates empty list.
    final byte[] lastElement = escapedElements[escapedElements.length - 1];
    if (lastElement.length > 0) {
      throw new IllegalArgumentException(
          lastElement.length + " trailing bytes found at end of list");
    }
    final ArrayList<E> decodedElements = new ArrayList<>(escapedElements.length - 1);
    for (int i = 0; i < escapedElements.length - 1; i++) {
      final byte[] escapedElement = escapedElements[i];
      final byte[] unescapedElement = unescape(escapedElement);
      decodedElements.add(elementLexicoder.decode(unescapedElement));
    }
    return decodedElements;
  }
}

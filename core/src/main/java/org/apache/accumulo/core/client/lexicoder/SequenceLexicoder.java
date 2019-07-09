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

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.concat;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.escape;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.split;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.unescape;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoder;

import javax.annotation.Nonnull;

/**
 * A lexicoder to encode/decode a Java List to/from a byte array where the concatenation of each
 * encoded element sorts lexicographically.
 *
 * Note: Unlike {@link ListLexicoder}, this implementation supports empty lists.
 *
 * @since 2.0.0
 * @param <LT> list element type.
 */
public class SequenceLexicoder<LT> extends AbstractLexicoder<List<LT>> {

  private Lexicoder<LT> lexicoder;

  /**
   * Primary constructor.
   *
   * @param lexicoder Lexicoder to apply to elements.
   */
  public SequenceLexicoder(@Nonnull final Lexicoder<LT> lexicoder) {
    this.lexicoder = requireNonNull(lexicoder, "lexicoder");
  }

  /**
   * {@inheritDoc}
   *
   * @return a byte array containing the concatenation of each element in the list encoded.
   */
  @Nonnull
  @Override
  public byte[] encode(@Nonnull final List<LT> v) {
    final byte[][] encElements = new byte[v.size() + 1][];
    int index = 0;
    for (final LT element : v) {
      encElements[index++] = escape(lexicoder.encode(element));
    }
    encElements[v.size()] = new byte[0];
    return concat(encElements);
  }

  @Nonnull
  @Override
  protected List<LT> decodeUnchecked(@Nonnull final byte[] b, final int offset, final int len) {
    final byte[][] escapedElements = split(b, offset, len);
    assert escapedElements.length > 0 : "ByteUtils.split always returns a minimum of 1 element, even for empty input";
    // There should be no bytes after the final delimiter. Lack of delimiter indicates empty list.
    final byte[] lastElement = escapedElements[escapedElements.length - 1];
    if (lastElement.length > 0) {
      throw new IllegalArgumentException(lastElement.length + " trailing bytes found at end of list");
    }
    final ArrayList<LT> ret = new ArrayList<>(escapedElements.length);
    for (int i = 0; i < escapedElements.length - 1; i++) {
      final byte[] escapedElement = escapedElements[i];
      ret.add(lexicoder.decode(unescape(escapedElement)));
    }
    return ret;
  }
}

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

import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.concat;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.escape;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.split;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.unescape;

import java.util.ArrayList;
import java.util.List;

/**
 * A lexicoder to encode/decode a Java List to/from a byte array where the concatenation of each
 * encoded element sorts lexicographically.
 *
 * Note: Empty lists are not supported. See {@link SequenceLexicoder} for an encoding that supports
 * empty lists.
 *
 * @since 1.6.0
 */
public class ListLexicoder<LT> extends AbstractLexicoder<List<LT>> {

  private Lexicoder<LT> lexicoder;

  public ListLexicoder(Lexicoder<LT> lexicoder) {
    this.lexicoder = lexicoder;
  }

  /**
   * {@inheritDoc}
   *
   * @return a byte array containing the concatenation of each element in the list encoded.
   */
  @Override
  public byte[] encode(List<LT> v) {
    if (v.isEmpty()) {
      throw new IllegalArgumentException("ListLexicoder does not support empty lists");
    }
    byte[][] encElements = new byte[v.size()][];

    int index = 0;
    for (LT element : v) {
      encElements[index++] = escape(lexicoder.encode(element));
    }

    return concat(encElements);
  }

  @Override
  public List<LT> decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility, since the corresponding
    // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
    return super.decode(b);
  }

  @Override
  protected List<LT> decodeUnchecked(byte[] b, int offset, int len) {

    byte[][] escapedElements = split(b, offset, len);
    ArrayList<LT> ret = new ArrayList<>(escapedElements.length);

    for (byte[] escapedElement : escapedElements) {
      ret.add(lexicoder.decode(unescape(escapedElement)));
    }

    return ret;
  }
}

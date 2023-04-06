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

import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.escape;
import static org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils.unescape;

/**
 * A lexicoder that flips the sort order from another lexicoder. If this is applied to
 * {@link DateLexicoder}, the most recent date will be sorted first and the oldest date will be
 * sorted last. If it's applied to {@link LongLexicoder}, the Long.MAX_VALUE will be sorted first
 * and Long.MIN_VALUE will be sorted last, etc...
 *
 * @since 1.6.0
 */
public class ReverseLexicoder<T> extends AbstractLexicoder<T> {

  private Lexicoder<T> lexicoder;

  /**
   * @param lexicoder The lexicoder who's sort order will be flipped.
   */
  public ReverseLexicoder(Lexicoder<T> lexicoder) {
    this.lexicoder = lexicoder;
  }

  @Override
  public byte[] encode(T data) {
    byte[] bytes = escape(lexicoder.encode(data));
    byte[] ret = new byte[bytes.length + 1];

    for (int i = 0; i < bytes.length; i++) {
      ret[i] = (byte) (0xff - (0xff & bytes[i]));
    }

    ret[bytes.length] = (byte) 0xff;

    return ret;
  }

  @Override
  protected T decodeUnchecked(byte[] data, int offset, int len) {
    byte[] ret = new byte[len - 1];

    int dataIndex;
    for (int i = 0; i < ret.length; i++) {
      dataIndex = offset + i;
      ret[i] = (byte) (0xff - (0xff & data[dataIndex]));
    }

    return lexicoder.decode(unescape(ret));
  }
}

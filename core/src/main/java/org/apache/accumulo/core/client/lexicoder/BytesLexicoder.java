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

/**
 * For each of the methods, this lexicoder just passes the input through untouched. It is meant to
 * be combined with other lexicoders like the {@link ReverseLexicoder}.
 *
 * @since 1.6.0
 */
public class BytesLexicoder extends AbstractLexicoder<byte[]> {

  @Override
  public byte[] encode(byte[] data) {
    return data;
  }

  @Override
  public byte[] decode(byte[] data) {
    // overrides AbstractLexicoder since this simply returns the array; this is more flexible than
    // the superclass behavior, since it can return null
    return data;
  }

  /**
   * If offset == 0 and len == data.length, returns data. Otherwise, returns a new byte array with
   * contents starting at data[offset] with length len.
   *
   * {@inheritDoc}
   */
  @Override
  protected byte[] decodeUnchecked(byte[] data, int offset, int len) {
    if (offset == 0 && len == data.length) {
      return data;
    }

    byte[] copy = new byte[len];
    System.arraycopy(data, offset, copy, 0, len);
    return copy;
  }

}

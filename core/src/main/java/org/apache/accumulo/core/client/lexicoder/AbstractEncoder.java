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

import org.apache.accumulo.core.iterators.ValueFormatException;

import com.google.common.base.Preconditions;

/**
 * AbstractEncoder is an {@link org.apache.accumulo.core.client.lexicoder.Encoder} that implements all of Encoder's methods validating the input, but has those
 * methods defer logic to to a new method, {@link #decodeUnchecked(byte[], int, int)}.
 *
 * @since 1.7.0
 */
public abstract class AbstractEncoder<T> implements Encoder<T> {

  /**
   * Decodes a byte array without checking if the offset and len exceed the bounds of the actual array.
   */
  protected abstract T decodeUnchecked(byte[] b, int offset, int len) throws ValueFormatException;

  @Override
  public T decode(byte[] b) {
    Preconditions.checkNotNull(b, "cannot decode null byte array");
    return decodeUnchecked(b, 0, b.length);
  }

  /**
   * Checks if the byte array is null, or if parameters exceed the bounds of the byte array, then calls {@link #decodeUnchecked(byte[], int, int)}.
   *
   * @throws java.lang.NullPointerException
   *           if {@code b} is null
   * @throws java.lang.IllegalArgumentException
   *           if {@code offset + len} exceeds the length of {@code b}
   */
  public T decode(byte[] b, int offset, int len) {
    Preconditions.checkNotNull(b, "cannot decode null byte array");
    Preconditions.checkArgument(offset >= 0, "offset %s cannot be negative", offset);
    Preconditions.checkArgument(len >= 0, "length %s cannot be negative", len);
    Preconditions.checkArgument(offset + len <= b.length, "offset + length %s exceeds byte array length %s", (offset + len), b.length);

    return decodeUnchecked(b, offset, len);
  }
}

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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * AbstractEncoder is an {@link Encoder} that implements all of Encoder's methods validating the
 * input, but has those methods defer logic to a new method,
 * {@link #decodeUnchecked(byte[], int, int)}.
 *
 * @since 1.7.0
 */
public abstract class AbstractEncoder<T> implements Encoder<T> {

  /**
   * Decodes a byte array without checking if the offset and len exceed the bounds of the actual
   * array.
   */
  protected abstract T decodeUnchecked(byte[] b, int offset, int len)
      throws IllegalArgumentException;

  @Override
  public T decode(byte[] b) {
    requireNonNull(b, "cannot decode null byte array");
    return decodeUnchecked(b, 0, b.length);
  }

  /**
   * Checks if the byte array is null, or if parameters exceed the bounds of the byte array, then
   * calls {@link #decodeUnchecked(byte[], int, int)}.
   *
   * @throws java.lang.NullPointerException if {@code b} is null
   * @throws java.lang.IllegalArgumentException if {@code offset + len} exceeds the length of
   *         {@code b}
   */
  public T decode(byte[] b, int offset, int len) {
    requireNonNull(b, "cannot decode null byte array");
    checkArgument(offset >= 0, "offset %s cannot be negative", offset);
    checkArgument(len >= 0, "length %s cannot be negative", len);
    checkArgument(offset + len <= b.length, "offset + length %s exceeds byte array length %s",
        (offset + len), b.length);

    return decodeUnchecked(b, offset, len);
  }
}

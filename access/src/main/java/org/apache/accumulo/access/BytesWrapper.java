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
package org.apache.accumulo.access;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

class BytesWrapper implements Comparable<BytesWrapper> {

  protected byte[] data;
  protected int offset;
  protected int length;

  /**
   * Creates a new sequence. The given byte array is used directly as the backing array, so later
   * changes made to the array reflect into the new sequence.
   *
   * @param data byte data
   */
  public BytesWrapper(byte[] data) {
    this.data = data;
    this.offset = 0;
    this.length = data.length;
  }

  /**
   * Creates a new sequence from a subsequence of the given byte array. The given byte array is used
   * directly as the backing array, so later changes made to the (relevant portion of the) array
   * reflect into the new sequence.
   *
   * @param data byte data
   * @param offset starting offset in byte array (inclusive)
   * @param length number of bytes to include in sequence
   * @throws IllegalArgumentException if the offset or length are out of bounds for the given byte
   *         array
   */
  public BytesWrapper(byte[] data, int offset, int length) {

    if (offset < 0 || offset > data.length || length < 0 || (offset + length) > data.length) {
      throw new IllegalArgumentException(" Bad offset and/or length data.length = " + data.length
          + " offset = " + offset + " length = " + length);
    }

    this.data = data;
    this.offset = offset;
    this.length = length;

  }

  public byte byteAt(int i) {

    if (i < 0) {
      throw new IllegalArgumentException("i < 0, " + i);
    }

    if (i >= length) {
      throw new IllegalArgumentException("i >= length, " + i + " >= " + length);
    }

    return data[offset + i];
  }

  public int length() {
    return length;
  }

  public byte[] toArray() {
    if (offset == 0 && length == data.length) {
      return data;
    }

    byte[] copy = new byte[length];
    System.arraycopy(data, offset, copy, 0, length);
    return copy;
  }

  @Override
  public int compareTo(BytesWrapper obs) {
    return Arrays.compare(data, offset, offset + length(), obs.data, obs.offset,
        obs.offset + obs.length());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BytesWrapper) {
      BytesWrapper obs = (BytesWrapper) o;

      if (this == o) {
        return true;
      }

      if (length() != obs.length()) {
        return false;
      }

      return compareTo(obs) == 0;
    }

    return false;

  }

  @Override
  public int hashCode() {
    int hash = 1;

    int end = offset + length();
    for (int i = offset; i < end; i++) {
      hash = (31 * hash) + data[i];
    }

    return hash;
  }

  @Override
  public String toString() {
    return new String(data, offset, length, UTF_8);
  }
}

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
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.apache.accumulo.core.util.ByteBufferUtil;

/**
 * An immutable implementation of {@link ByteSequence} that copies data in its constructor and never
 * lets its internal byte array escape. This class is final to prevent it from being made mutable in
 * a subclass.
 *
 * <p>
 * This class is package private because direct construction is not expected, instead use of methods
 * like {@link ByteSequence#of(byte[])} is how instances of this should be created.
 * </p>
 */
final class ImmutableByteSequence extends ByteSequence {

  private static final long serialVersionUID = 1L;

  // A reference to this must never escape this class.
  private final byte[] data;

  private int hash = 1;
  private boolean hashIsOne = false;

  ImmutableByteSequence(ByteSequence data) {
    if (data.isBackedByArray()) {
      this.data = Arrays.copyOfRange(data.toArray(), data.offset(), data.offset() + data.offset());
    } else {
      var dataArray = data.toArray();
      this.data = Arrays.copyOf(dataArray, dataArray.length);
    }
  }

  /**
   * Creates a new sequence. The array is copied into an new internal array.
   *
   * @param data byte data
   */
  ImmutableByteSequence(byte[] data) {
    this.data = Arrays.copyOf(data, data.length);
  }

  /**
   * Creates a new sequence from a subsequence of the given byte array. The contents of the array
   * are copied into an internal array.
   *
   * @param data byte data
   * @param offset starting offset in byte array (inclusive)
   * @param length number of bytes to include in sequence
   * @throws IllegalArgumentException if the offset or length are out of bounds for the given byte
   *         array
   */
  ImmutableByteSequence(byte[] data, int offset, int length) {
    Objects.checkFromIndexSize(offset, length, data.length);
    this.data = new byte[length];
    System.arraycopy(data, offset, this.data, 0, length);
  }

  /**
   * Creates a new sequence from the given string. The bytes are determined from the string using
   * UTF-8 to encode.
   *
   * @param s string to represent as bytes
   */
  ImmutableByteSequence(String s) {
    // String.getBytes will allocate a new array that is never shared anywhere else, so no need to
    // copy it.
    this.data = s.getBytes(UTF_8);
  }

  /**
   * Creates a new sequence based on a byte buffer by copying the byte buffers contents into an
   * internal array.
   *
   * @param buffer byte buffer
   */
  ImmutableByteSequence(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      var array = buffer.array();
      var length = buffer.remaining();
      var offset = buffer.position() + buffer.arrayOffset();
      this.data = new byte[length];
      System.arraycopy(array, offset, this.data, 0, length);
    } else {
      this.data = ByteBufferUtil.toBytes(buffer);
    }
  }

  @Override
  public byte byteAt(int i) {
    return data[i];
  }

  @Override
  public int length() {
    return data.length;
  }

  @Override
  public ByteSequence subSequence(int start, int end) {
    return new ImmutableByteSequence(data, start, end - start);
  }

  @Override
  public byte[] toArray() {
    return Arrays.copyOf(data, data.length);
  }

  @Override
  public boolean isBackedByArray() {
    return false;
  }

  @Override
  public byte[] getBackingArray() {
    throw new UnsupportedOperationException(
        "This method is not supported when isBackedByArray() returns false.");
  }

  @Override
  public int offset() {
    throw new UnsupportedOperationException(
        "This method is not supported when isBackedByArray() returns false.");
  }

  /**
   * An optimized version of the hashcode algorithm that directly uses the internal byte array.
   */
  @Override
  public int hashCode() {
    // This is the same algorithm, with a few modifications so it's not a copy, that java String
    // uses to deal with race conditions.
    int h = hash;
    if (h == 1 && !hashIsOne) {
      h = ByteSequence.hashCode(data, 0, data.length);
      if (h == 1) {
        hashIsOne = true;
      } else {
        hash = h;
      }
    }
    return h;
  }

  @Override
  public String toString() {
    return new String(data, UTF_8);
  }
}

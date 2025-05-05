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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.apache.accumulo.core.util.ByteBufferUtil;

/**
 * An implementation of {@link ByteSequence} that uses a backing byte array.
 */
public class ArrayByteSequence extends ByteSequence implements Serializable {

  private static final long serialVersionUID = 1L;

  protected byte[] data;
  protected int offset;
  protected int length;

  /**
   * Creates a new sequence. The given byte array is used directly as the backing array, so later
   * changes made to the array reflect into the new sequence.
   *
   * @param data byte data
   */
  public ArrayByteSequence(byte[] data) {
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
  public ArrayByteSequence(byte[] data, int offset, int length) {

    Objects.checkFromIndexSize(offset, length, data.length);

    this.data = data;
    this.offset = offset;
    this.length = length;

  }

  /**
   * Creates a new sequence from the given string. The bytes are determined from the string using
   * the default platform encoding.
   *
   * @param s string to represent as bytes
   */
  public ArrayByteSequence(String s) {
    this(s.getBytes(UTF_8));
  }

  /**
   * Creates a new sequence based on a byte buffer. If the byte buffer has an array, that array (and
   * the buffer's offset and limit) are used; otherwise, a new backing array is created and a
   * relative bulk get is performed to transfer the buffer's contents (starting at its current
   * position and not beyond its limit).
   *
   * @param buffer byte buffer
   */
  public ArrayByteSequence(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      this.data = buffer.array();
      this.offset = buffer.position() + buffer.arrayOffset();
      this.length = buffer.remaining();
    } else {
      this.offset = 0;
      this.data = ByteBufferUtil.toBytes(buffer);
      this.length = data.length;
    }
  }

  private static byte[] copy(ByteSequence bs) {
    if (bs.isBackedByArray()) {
      return Arrays.copyOfRange(bs.getBackingArray(), bs.offset(), bs.offset() + bs.length());
    } else {
      return bs.toArray();
    }
  }

  /**
   * Copy constructor. Copies contents of byteSequence.
   *
   * @since 2.0.0
   */
  public ArrayByteSequence(ByteSequence byteSequence) {
    this(copy(byteSequence));
  }

  @Override
  public byte byteAt(int i) {

    Objects.checkIndex(i, length);

    return data[offset + i];
  }

  @Override
  public byte[] getBackingArray() {
    return data;
  }

  @Override
  public boolean isBackedByArray() {
    return true;
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public int offset() {
    return offset;
  }

  /**
   * Reset the backing array for this byte sequence object. This is useful for object re-use.
   *
   * @since 3.1.0
   */
  public void reset(byte[] data, int offset, int length) {
    Objects.checkFromIndexSize(offset, length, data.length);
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public ByteSequence subSequence(int start, int end) {

    Objects.checkFromToIndex(start, end, length);

    return new ArrayByteSequence(data, offset + start, end - start);
  }

  @Override
  public byte[] toArray() {
    if (offset == 0 && length == data.length) {
      return data;
    }

    byte[] copy = new byte[length];
    System.arraycopy(data, offset, copy, 0, length);
    return copy;
  }

  @Override
  public String toString() {
    return new String(data, offset, length, UTF_8);
  }
}

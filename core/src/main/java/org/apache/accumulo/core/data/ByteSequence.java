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

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.WritableComparator;

/**
 * A sequence of bytes.
 */
public abstract class ByteSequence implements Comparable<ByteSequence>, Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Gets a byte within this sequence.
   *
   * @param i index into sequence
   * @return byte
   * @throws IllegalArgumentException if i is out of range
   */
  public abstract byte byteAt(int i);

  /**
   * Gets the length of this sequence.
   *
   * @return sequence length
   */
  public abstract int length();

  /**
   * Returns a portion of this sequence.
   *
   * @param start index of subsequence start (inclusive)
   * @param end index of subsequence end (exclusive)
   */
  public abstract ByteSequence subSequence(int start, int end);

  /**
   * Returns a byte array containing the bytes in this sequence. This method may copy the sequence
   * data or may return a backing byte array directly.
   *
   * @return byte array
   */
  public abstract byte[] toArray();

  /**
   * Determines whether this sequence is backed by a byte array.
   *
   * @return true if sequence is backed by a byte array
   */
  public abstract boolean isBackedByArray();

  /**
   * Gets the backing byte array for this sequence.
   *
   * @return byte array
   */
  public abstract byte[] getBackingArray();

  /**
   * Gets the offset for this sequence. This value represents the starting point for the sequence in
   * the backing array, if there is one.
   *
   * @return offset (inclusive)
   */
  public abstract int offset();

  /**
   * Compares the two given byte sequences, byte by byte, returning a negative, zero, or positive
   * result if the first sequence is less than, equal to, or greater than the second. The comparison
   * is performed starting with the first byte of each sequence, and proceeds until a pair of bytes
   * differs, or one sequence runs out of byte (is shorter). A shorter sequence is considered less
   * than a longer one.
   *
   * @param bs1 first byte sequence to compare
   * @param bs2 second byte sequence to compare
   * @return comparison result
   */
  public static int compareBytes(ByteSequence bs1, ByteSequence bs2) {

    int len1 = bs1.length();
    int len2 = bs2.length();

    int minLen = Math.min(len1, len2);

    for (int i = 0; i < minLen; i++) {
      int a = (bs1.byteAt(i) & 0xff);
      int b = (bs2.byteAt(i) & 0xff);

      if (a != b) {
        return a - b;
      }
    }

    return len1 - len2;
  }

  @Override
  public int compareTo(ByteSequence obs) {
    if (isBackedByArray() && obs.isBackedByArray()) {
      return WritableComparator.compareBytes(getBackingArray(), offset(), length(),
          obs.getBackingArray(), obs.offset(), obs.length());
    }

    return compareBytes(this, obs);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ByteSequence) {
      ByteSequence obs = (ByteSequence) o;

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

  /**
   * A method that subclasses can use to consistently implement the expected hashcode
   *
   * @since 3.1.
   */
  protected static int hashCode(byte[] data, int offset, int len) {
    int hash = 1;
    int end = offset + len;
    for (int i = offset; i < end; i++) {
      hash = (31 * hash) + data[i];
    }
    return hash;
  }

  /**
   * All implementations of this method must use the following algorithm. This ensures different
   * implementations compute the same hash for the same data.
   *
   * <pre>
   *     {@code
   * int hash = 1;
   * for (int i = 0; i < length(); i++) {
   *   hash = (31 * hash) + byteAt(i);
   * }
   * return hash;
   * }
   * </pre>
   *
   */
  @Override
  public int hashCode() {
    int hash = 1;
    if (isBackedByArray()) {
      return hashCode(getBackingArray(), offset(), length());
    } else {
      for (int i = 0; i < length(); i++) {
        hash = (31 * hash) + byteAt(i);
      }
    }
    return hash;
  }

  /**
   * This method returns an empty immutable byte sequence. The same object is always returned.
   *
   * @since 3.1.0
   */
  public static ByteSequence of() {
    return EmptyByteSequence.INSTANCE;
  }

  /**
   * This method creates a new immutable ByteSequence. If a mutable ByteSequence is needed or copies
   * are not desired then use {@link ArrayByteSequence}.
   *
   * @since 3.1.0
   */
  public static ByteSequence of(byte[] data) {
    if (data.length == 0) {
      return EmptyByteSequence.INSTANCE;
    } else {
      return new ImmutableByteSequence(data);
    }
  }

  /**
   * This method creates a new immutable ByteSequence. If a mutable ByteSequence is needed or copies
   * are not desired then use {@link ArrayByteSequence}.
   *
   * @since 3.1.0
   */
  public static ByteSequence of(byte[] data, int offset, int length) {
    if (length == 0) {
      return EmptyByteSequence.INSTANCE;
    } else {
      return new ImmutableByteSequence(data, offset, length);
    }
  }

  /**
   * This method creates a new immutable ByteSequence. If a mutable ByteSequence is needed or copies
   * are not desired then use {@link ArrayByteSequence}. The method use UTF8 to encode the String
   * into bytes.
   *
   * @since 3.1.0
   */
  public static ByteSequence of(String data) {
    if (data.isEmpty()) {
      return EmptyByteSequence.INSTANCE;
    } else {
      return new ImmutableByteSequence(data);
    }
  }

  /**
   * This method creates a new immutable ByteSequence. If a mutable ByteSequence is needed or copies
   * are not desired then use {@link ArrayByteSequence}.
   *
   * @since 3.1.0
   */
  public static ByteSequence of(ByteBuffer data) {
    if (data.remaining() == 0) {
      return EmptyByteSequence.INSTANCE;
    } else {
      return new ImmutableByteSequence(data);
    }
  }

  /**
   * This method creates a new immutable ByteSequence. If a mutable ByteSequence is needed or copies
   * are not desired then use {@link ArrayByteSequence}.
   *
   * @since 3.1.0
   */
  public static ByteSequence of(ByteSequence data) {
    if (data instanceof ImmutableByteSequence) {
      return data;
    } else if (data.length() == 0) {
      return EmptyByteSequence.INSTANCE;
    } else {
      return new ImmutableByteSequence(data);
    }
  }
}

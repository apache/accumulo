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
package org.apache.accumulo.core.data;

import org.apache.hadoop.io.WritableComparator;

/**
 * A sequence of bytes.
 */
public abstract class ByteSequence implements Comparable<ByteSequence> {

  /**
   * Gets a byte within this sequence.
   *
   * @param i
   *          index into sequence
   * @return byte
   * @throws IllegalArgumentException
   *           if i is out of range
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
   * @param start
   *          index of subsequence start (inclusive)
   * @param end
   *          index of subsequence end (exclusive)
   */
  public abstract ByteSequence subSequence(int start, int end);

  /**
   * Returns a byte array containing the bytes in this sequence. This method may copy the sequence data or may return a backing byte array directly.
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
   * Gets the offset for this sequence. This value represents the starting point for the sequence in the backing array, if there is one.
   *
   * @return offset (inclusive)
   */
  public abstract int offset();

  /**
   * Compares the two given byte sequences, byte by byte, returning a negative, zero, or positive result if the first sequence is less than, equal to, or
   * greater than the second. The comparison is performed starting with the first byte of each sequence, and proceeds until a pair of bytes differs, or one
   * sequence runs out of byte (is shorter). A shorter sequence is considered less than a longer one.
   *
   * @param bs1
   *          first byte sequence to compare
   * @param bs2
   *          second byte sequence to compare
   * @return comparison result
   */
  public static int compareBytes(ByteSequence bs1, ByteSequence bs2) {

    int minLen = Math.min(bs1.length(), bs2.length());

    for (int i = 0; i < minLen; i++) {
      int a = (bs1.byteAt(i) & 0xff);
      int b = (bs2.byteAt(i) & 0xff);

      if (a != b) {
        return a - b;
      }
    }

    return bs1.length() - bs2.length();
  }

  @Override
  public int compareTo(ByteSequence obs) {
    if (isBackedByArray() && obs.isBackedByArray()) {
      return WritableComparator.compareBytes(getBackingArray(), offset(), length(), obs.getBackingArray(), obs.offset(), obs.length());
    }

    return compareBytes(this, obs);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ByteSequence) {
      ByteSequence obs = (ByteSequence) o;

      if (this == o)
        return true;

      if (length() != obs.length())
        return false;

      return compareTo(obs) == 0;
    }

    return false;

  }

  @Override
  public int hashCode() {
    int hash = 1;
    if (isBackedByArray()) {
      byte[] data = getBackingArray();
      int end = offset() + length();
      for (int i = offset(); i < end; i++)
        hash = (31 * hash) + data[i];
    } else {
      for (int i = 0; i < length(); i++)
        hash = (31 * hash) + byteAt(i);
    }
    return hash;
  }

}

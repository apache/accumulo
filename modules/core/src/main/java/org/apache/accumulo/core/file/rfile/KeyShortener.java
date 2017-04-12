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

package org.apache.accumulo.core.file.rfile;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;

/*
 * Code to shorten keys that will be placed into RFile indexes. This code attempts to find a key thats between two keys that shorter.
 */
public class KeyShortener {

  private static final byte[] EMPTY = new byte[0];
  private static final byte[] B00 = new byte[] {(byte) 0x00};
  private static final byte[] BFF = new byte[] {(byte) 0xff};

  private static final Logger log = LoggerFactory.getLogger(KeyShortener.class);

  private KeyShortener() {}

  private static int findNonFF(ByteSequence bs, int start) {
    for (int i = start; i < bs.length(); i++) {
      if (bs.byteAt(i) != (byte) 0xff) {
        return i;
      }
    }

    return bs.length();
  }

  /*
   * return S such that prev < S < current or null if no such sequence
   */
  public static ByteSequence shorten(ByteSequence prev, ByteSequence current) {

    int minLen = Math.min(prev.length(), current.length());

    for (int i = 0; i < minLen; i++) {
      int pb = 0xff & prev.byteAt(i);
      int cb = 0xff & current.byteAt(i);

      int diff = cb - pb;

      if (diff == 1) {
        int newLen = findNonFF(prev, i + 1);
        byte[] successor;
        if (newLen < prev.length()) {
          successor = Bytes.concat(prev.subSequence(0, newLen).toArray(), BFF);
        } else {
          successor = Bytes.concat(prev.subSequence(0, newLen).toArray(), B00);
        }
        return new ArrayByteSequence(successor);
      } else if (diff > 1) {
        byte[] copy = new byte[i + 1];
        System.arraycopy(prev.subSequence(0, i + 1).toArray(), 0, copy, 0, i + 1);
        copy[i] = (byte) ((0xff & copy[i]) + 1);
        return new ArrayByteSequence(copy);
      }
    }

    ArrayByteSequence successor = new ArrayByteSequence(Bytes.concat(prev.toArray(), B00));
    if (successor.equals(current)) {
      return null;
    }

    return successor;
  }

  /*
   * This entire class supports an optional optimization. This code does a sanity check to ensure the optimization code did what was intended, doing a noop if
   * there is a bug.
   */
  @VisibleForTesting
  static Key sanityCheck(Key prev, Key current, Key shortened) {
    if (prev.compareTo(shortened) >= 0) {
      log.warn("Bug in key shortening code, please open an issue " + prev + " >= " + shortened);
      return prev;
    }

    if (current.compareTo(shortened) <= 0) {
      log.warn("Bug in key shortening code, please open an issue " + current + " <= " + shortened);
      return prev;
    }

    return shortened;
  }

  /*
   * Find a key K where prev < K < current AND K is shorter. If can not find a K that meets criteria, then returns prev.
   */
  public static Key shorten(Key prev, Key current) {
    Preconditions.checkArgument(prev.compareTo(current) <= 0, "Expected key less than or equal. " + prev + " > " + current);

    if (prev.getRowData().compareTo(current.getRowData()) < 0) {
      ByteSequence shortenedRow = shorten(prev.getRowData(), current.getRowData());
      if (shortenedRow == null) {
        return prev;
      }
      return sanityCheck(prev, current, new Key(shortenedRow.toArray(), EMPTY, EMPTY, EMPTY, 0));
    } else if (prev.getColumnFamilyData().compareTo(current.getColumnFamilyData()) < 0) {
      ByteSequence shortenedFam = shorten(prev.getColumnFamilyData(), current.getColumnFamilyData());
      if (shortenedFam == null) {
        return prev;
      }
      return sanityCheck(prev, current, new Key(prev.getRowData().toArray(), shortenedFam.toArray(), EMPTY, EMPTY, 0));
    } else if (prev.getColumnQualifierData().compareTo(current.getColumnQualifierData()) < 0) {
      ByteSequence shortenedQual = shorten(prev.getColumnQualifierData(), current.getColumnQualifierData());
      if (shortenedQual == null) {
        return prev;
      }
      return sanityCheck(prev, current, new Key(prev.getRowData().toArray(), prev.getColumnFamilyData().toArray(), shortenedQual.toArray(), EMPTY, 0));
    } else {
      return prev;
    }
  }
}

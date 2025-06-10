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
package org.apache.accumulo.core.dataImpl;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;

public class RangeImpl {
  public static Range bound(Range range, Column min, Column max,
      boolean returnEmptyRangeWhenDisjoint) {

    if (min.compareTo(max) > 0) {
      throw new IllegalArgumentException("min column > max column " + min + " " + max);
    }

    Key sk = range.getStartKey();
    boolean ski = range.isStartKeyInclusive();

    if (sk != null) {

      ByteSequence cf = sk.getColumnFamilyData();
      ByteSequence cq = sk.getColumnQualifierData();

      ByteSequence mincf = new ArrayByteSequence(min.columnFamily);
      ByteSequence mincq;

      if (min.columnQualifier != null) {
        mincq = new ArrayByteSequence(min.columnQualifier);
      } else {
        mincq = new ArrayByteSequence(new byte[0]);
      }

      int cmp = cf.compareTo(mincf);

      if (cmp < 0 || (cmp == 0 && cq.compareTo(mincq) < 0)) {
        ski = true;
        sk = new Key(sk.getRowData().toArray(), mincf.toArray(), mincq.toArray(), new byte[0],
            Long.MAX_VALUE, true);
      }
    }

    Key ek = range.getEndKey();
    boolean eki = range.isEndKeyInclusive();

    if (ek != null) {
      ByteSequence row = ek.getRowData();
      ByteSequence cf = ek.getColumnFamilyData();
      ByteSequence cq = ek.getColumnQualifierData();
      ByteSequence cv = ek.getColumnVisibilityData();

      ByteSequence maxcf = new ArrayByteSequence(max.columnFamily);
      ByteSequence maxcq = null;
      if (max.columnQualifier != null) {
        maxcq = new ArrayByteSequence(max.columnQualifier);
      }

      boolean set = false;

      int comp = cf.compareTo(maxcf);

      if (comp > 0) {
        set = true;
      } else if (comp == 0 && maxcq != null && cq.compareTo(maxcq) > 0) {
        set = true;
      } else if (!eki && row.length() > 0 && row.byteAt(row.length() - 1) == 0 && cf.length() == 0
          && cq.length() == 0 && cv.length() == 0 && ek.getTimestamp() == Long.MAX_VALUE) {
        row = row.subSequence(0, row.length() - 1);
        set = true;
      }

      if (set) {
        eki = false;
        if (maxcq == null) {
          ek = new Key(row.toArray(), maxcf.toArray(), new byte[0], new byte[0], 0, false)
              .followingKey(PartialKey.ROW_COLFAM);
        } else {
          ek = new Key(row.toArray(), maxcf.toArray(), maxcq.toArray(), new byte[0], 0, false)
              .followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        }
      }
    }

    if (returnEmptyRangeWhenDisjoint && sk != null && ek != null && sk.compareTo(ek) > 0) {
      return new Range(sk, true, sk, false);
    }
    return new Range(sk, ski, ek, eki);
  }
}

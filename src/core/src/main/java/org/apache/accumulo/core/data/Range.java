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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Range implements WritableComparable<Range> {
  
  private Key start;
  private Key stop;
  private boolean startKeyInclusive;
  private boolean stopKeyInclusive;
  private boolean infiniteStartKey;
  private boolean infiniteStopKey;
  
  /**
   * Creates a range that goes from negative to positive infinity
   */
  
  public Range() {
    this((Key) null, true, (Key) null, true);
  }
  
  /**
   * Creates a range from startKey inclusive to endKey inclusive
   * 
   * @param startKey
   *          set this to null when negative infinity is needed
   * @param endKey
   *          set this to null when positive infinity is needed
   */
  public Range(Key startKey, Key endKey) {
    this(startKey, true, endKey, true);
  }
  
  /**
   * Creates a range that covers an entire row
   * 
   * @param row
   *          set this to null to cover all rows
   */
  public Range(CharSequence row) {
    this(row, true, row, true);
  }
  
  /**
   * Creates a range that covers an entire row
   * 
   * @param row
   *          set this to null to cover all rows
   */
  public Range(Text row) {
    this(row, true, row, true);
  }
  
  /**
   * Creates a range from startRow inclusive to endRow inclusive
   * 
   * @param startRow
   *          set this to null when negative infinity is needed
   * @param endRow
   *          set this to null when positive infinity is needed
   */
  public Range(Text startRow, Text endRow) {
    this(startRow, true, endRow, true);
  }
  
  /**
   * Creates a range from startRow inclusive to endRow inclusive
   * 
   * @param startRow
   *          set this to null when negative infinity is needed
   * @param endRow
   *          set this to null when positive infinity is needed
   */
  public Range(CharSequence startRow, CharSequence endRow) {
    this(startRow, true, endRow, true);
  }
  
  /**
   * Creates a range from startRow inclusive to endRow inclusive
   * 
   * @param startRow
   *          set this to null when negative infinity is needed
   * @param startRowInclusive
   *          determines if the start row is skipped
   * @param endRow
   *          set this to null when positive infinity is needed
   * @param endRowInclusive
   *          determines if the endRow is included
   */
  
  public Range(Text startRow, boolean startRowInclusive, Text endRow, boolean endRowInclusive) {
    this((startRow == null ? null : (startRowInclusive ? new Key(startRow) : new Key(startRow).followingKey(PartialKey.ROW))), true, (endRow == null ? null
        : (endRowInclusive ? new Key(endRow).followingKey(PartialKey.ROW) : new Key(endRow))), false);
  }
  
  /**
   * Creates a range from startRow inclusive to endRow inclusive
   * 
   * @param startRow
   *          set this to null when negative infinity is needed
   * @param startRowInclusive
   *          determines if the start row is skipped
   * @param endRow
   *          set this to null when positive infinity is needed
   * @param endRowInclusive
   *          determines if the endRow is included
   */
  
  public Range(CharSequence startRow, boolean startRowInclusive, CharSequence endRow, boolean endRowInclusive) {
    this(startRow == null ? null : new Text(startRow.toString()), startRowInclusive, endRow == null ? null : new Text(endRow.toString()), endRowInclusive);
  }
  
  /**
   * @param startKey
   *          set this to null when negative infinity is needed
   * @param startKeyInclusive
   *          determines if the ranges includes the start key
   * @param endKey
   *          set this to null when infinity is needed
   * @param endKeyInclusive
   *          determines if the range includes the end key
   */
  public Range(Key startKey, boolean startKeyInclusive, Key endKey, boolean endKeyInclusive) {
    this.start = startKey;
    this.startKeyInclusive = startKeyInclusive;
    this.infiniteStartKey = startKey == null;
    this.stop = endKey;
    this.stopKeyInclusive = endKeyInclusive;
    this.infiniteStopKey = stop == null;
    
    if (!infiniteStartKey && !infiniteStopKey && beforeStartKey(endKey)) {
      throw new IllegalArgumentException("Start key must be less than end key in range (" + startKey + ", " + endKey + ")");
    }
  }
  
  /**
   * Copy constructor
   */
  public Range(Range range) {
    this(range.start, range.stop, range.startKeyInclusive, range.stopKeyInclusive, range.infiniteStartKey, range.infiniteStopKey);
  }
  
  public Range(Key start, Key stop, boolean startKeyInclusive, boolean stopKeyInclusive, boolean infiniteStartKey, boolean infiniteStopKey) {
    if (infiniteStartKey && start != null)
      throw new IllegalArgumentException();
    
    if (infiniteStopKey && stop != null)
      throw new IllegalArgumentException();
    
    this.start = start;
    this.stop = stop;
    this.startKeyInclusive = startKeyInclusive;
    this.stopKeyInclusive = stopKeyInclusive;
    this.infiniteStartKey = infiniteStartKey;
    this.infiniteStopKey = infiniteStopKey;
  }
  
  public Range(TRange trange) {
    this(trange.start == null ? null : new Key(trange.start), trange.stop == null ? null : new Key(trange.stop), trange.startKeyInclusive,
        trange.stopKeyInclusive, trange.infiniteStartKey, trange.infiniteStopKey);
  }
  
  public Key getStartKey() {
    if (infiniteStartKey) {
      return null;
    }
    return start;
  }
  
  public boolean beforeStartKey(Key key) {
    if (infiniteStartKey) {
      return false;
    }
    
    if (startKeyInclusive)
      return key.compareTo(start) < 0;
    return key.compareTo(start) <= 0;
  }
  
  public Key getEndKey() {
    if (infiniteStopKey) {
      return null;
    }
    return stop;
  }
  
  public boolean afterEndKey(Key key) {
    if (infiniteStopKey)
      return false;
    
    if (stopKeyInclusive)
      return stop.compareTo(key) < 0;
    return stop.compareTo(key) <= 0;
  }
  
  public int hashCode() {
    int startHash = infiniteStartKey ? 0 : start.hashCode() + (startKeyInclusive ? 1 : 0);
    int stopHash = infiniteStopKey ? 0 : stop.hashCode() + (stopKeyInclusive ? 1 : 0);
    
    return startHash + stopHash;
  }
  
  public boolean equals(Object o) {
    if (o instanceof Range)
      return equals((Range) o);
    return false;
  }
  
  public boolean equals(Range otherRange) {
    
    return compareTo(otherRange) == 0;
  }
  
  public int compareTo(Range o) {
    int comp;
    
    if (infiniteStartKey)
      if (o.infiniteStartKey)
        comp = 0;
      else
        comp = -1;
    else if (o.infiniteStartKey)
      comp = 1;
    else {
      comp = start.compareTo(o.start);
      if (comp == 0)
        if (startKeyInclusive && !o.startKeyInclusive)
          comp = -1;
        else if (!startKeyInclusive && o.startKeyInclusive)
          comp = 1;
      
    }
    
    if (comp == 0)
      if (infiniteStopKey)
        if (o.infiniteStopKey)
          comp = 0;
        else
          comp = 1;
      else if (o.infiniteStopKey)
        comp = -1;
      else {
        comp = stop.compareTo(o.stop);
        if (comp == 0)
          if (stopKeyInclusive && !o.stopKeyInclusive)
            comp = 1;
          else if (!stopKeyInclusive && o.stopKeyInclusive)
            comp = -1;
      }
    
    return comp;
  }
  
  public boolean contains(Key key) {
    return !beforeStartKey(key) && !afterEndKey(key);
  }
  
  public static List<Range> mergeOverlapping(Collection<Range> ranges) {
    if (ranges.size() == 0)
      return Collections.emptyList();
    
    List<Range> ral = new ArrayList<Range>(ranges);
    Collections.sort(ral);
    
    ArrayList<Range> ret = new ArrayList<Range>(ranges.size());
    
    Range currentRange = ral.get(0);
    boolean currentStartKeyInclusive = ral.get(0).startKeyInclusive;
    
    for (int i = 1; i < ral.size(); i++) {
      // because of inclusive switch, equal keys may not be seen
      
      if (currentRange.infiniteStopKey) {
        // this range has the minimal start key and
        // an infinite end key so it will contain all
        // other ranges
        break;
      }
      
      Range range = ral.get(i);
      
      boolean startKeysEqual;
      if (range.infiniteStartKey) {
        // previous start key must be infinite because it is sorted
        assert (currentRange.infiniteStartKey);
        startKeysEqual = true;
      } else if (currentRange.infiniteStartKey) {
        startKeysEqual = false;
      } else if (currentRange.start.compareTo(range.start) == 0) {
        startKeysEqual = true;
      } else {
        startKeysEqual = false;
      }
      
      if (startKeysEqual || currentRange.contains(range.start)
          || (!currentRange.stopKeyInclusive && range.startKeyInclusive && range.start.equals(currentRange.stop))) {
        int cmp;
        
        if (range.infiniteStopKey || (cmp = range.stop.compareTo(currentRange.stop)) > 0 || (cmp == 0 && range.stopKeyInclusive)) {
          currentRange = new Range(currentRange.getStartKey(), currentStartKeyInclusive, range.getEndKey(), range.stopKeyInclusive);
        }/* else currentRange contains ral.get(i) */
      } else {
        ret.add(currentRange);
        currentRange = range;
        currentStartKeyInclusive = range.startKeyInclusive;
      }
    }
    
    ret.add(currentRange);
    
    return ret;
  }
  
  public Range clip(Range range) {
    return clip(range, false);
  }
  
  public Range clip(Range range, boolean returnNullIfDisjoint) {
    
    Key sk = range.getStartKey();
    boolean ski = range.isStartKeyInclusive();
    
    Key ek = range.getEndKey();
    boolean eki = range.isEndKeyInclusive();
    
    if (range.getStartKey() == null) {
      if (getStartKey() != null) {
        sk = getStartKey();
        ski = isStartKeyInclusive();
      }
    } else if (afterEndKey(range.getStartKey())
        || (getEndKey() != null && range.getStartKey().equals(getEndKey()) && !(range.isStartKeyInclusive() && isEndKeyInclusive()))) {
      if (returnNullIfDisjoint)
        return null;
      throw new IllegalArgumentException("Range " + range + " does not overlap " + this);
    } else if (beforeStartKey(range.getStartKey())) {
      sk = getStartKey();
      ski = isStartKeyInclusive();
    }
    
    if (range.getEndKey() == null) {
      if (getEndKey() != null) {
        ek = getEndKey();
        eki = isEndKeyInclusive();
      }
    } else if (beforeStartKey(range.getEndKey())
        || (getStartKey() != null && range.getEndKey().equals(getStartKey()) && !(range.isEndKeyInclusive() && isStartKeyInclusive()))) {
      if (returnNullIfDisjoint)
        return null;
      throw new IllegalArgumentException("Range " + range + " does not overlap " + this);
    } else if (afterEndKey(range.getEndKey())) {
      ek = getEndKey();
      eki = isEndKeyInclusive();
    }
    
    return new Range(sk, ski, ek, eki);
  }
  
  public Range bound(Column min, Column max) {
    
    if (min.compareTo(max) > 0) {
      throw new IllegalArgumentException("min column > max column " + min + " " + max);
    }
    
    Key sk = getStartKey();
    boolean ski = isStartKeyInclusive();
    
    if (sk != null) {
      
      ByteSequence cf = sk.getColumnFamilyData();
      ByteSequence cq = sk.getColumnQualifierData();
      
      ByteSequence mincf = new ArrayByteSequence(min.columnFamily);
      ByteSequence mincq;
      
      if (min.columnQualifier != null)
        mincq = new ArrayByteSequence(min.columnQualifier);
      else
        mincq = new ArrayByteSequence(new byte[0]);
      
      int cmp = cf.compareTo(mincf);
      
      if (cmp < 0 || (cmp == 0 && cq.compareTo(mincq) < 0)) {
        ski = true;
        sk = new Key(sk.getRowData().toArray(), mincf.toArray(), mincq.toArray(), new byte[0], Long.MAX_VALUE, true);
      }
    }
    
    Key ek = getEndKey();
    boolean eki = isEndKeyInclusive();
    
    if (ek != null) {
      ByteSequence row = ek.getRowData();
      ByteSequence cf = ek.getColumnFamilyData();
      ByteSequence cq = ek.getColumnQualifierData();
      ByteSequence cv = ek.getColumnVisibilityData();
      
      ByteSequence maxcf = new ArrayByteSequence(max.columnFamily);
      ByteSequence maxcq = null;
      if (max.columnQualifier != null)
        maxcq = new ArrayByteSequence(max.columnQualifier);
      
      boolean set = false;
      
      int comp = cf.compareTo(maxcf);
      
      if (comp > 0) {
        set = true;
      } else if (comp == 0 && maxcq != null && cq.compareTo(maxcq) > 0) {
        set = true;
      } else if (!eki && row.length() > 0 && row.byteAt(row.length() - 1) == 0 && cf.length() == 0 && cq.length() == 0 && cv.length() == 0
          && ek.getTimestamp() == Long.MAX_VALUE) {
        row = row.subSequence(0, row.length() - 1);
        set = true;
      }
      
      if (set) {
        eki = false;
        if (maxcq == null)
          ek = new Key(row.toArray(), maxcf.toArray(), new byte[0], new byte[0], 0, false).followingKey(PartialKey.ROW_COLFAM);
        else
          ek = new Key(row.toArray(), maxcf.toArray(), maxcq.toArray(), new byte[0], 0, false).followingKey(PartialKey.ROW_COLFAM_COLQUAL);
      }
    }
    
    return new Range(sk, ski, ek, eki);
  }
  
  public String toString() {
    return ((startKeyInclusive && start != null) ? "[" : "(") + (start == null ? "-inf" : start) + "," + (stop == null ? "+inf" : stop)
        + ((stopKeyInclusive && stop != null) ? "]" : ")");
  }
  
  public void readFields(DataInput in) throws IOException {
    infiniteStartKey = in.readBoolean();
    infiniteStopKey = in.readBoolean();
    if (!infiniteStartKey) {
      start = new Key();
      start.readFields(in);
    } else {
      start = null;
    }
    
    if (!infiniteStopKey) {
      stop = new Key();
      stop.readFields(in);
    } else {
      stop = null;
    }
    
    startKeyInclusive = in.readBoolean();
    stopKeyInclusive = in.readBoolean();
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(infiniteStartKey);
    out.writeBoolean(infiniteStopKey);
    if (!infiniteStartKey)
      start.write(out);
    if (!infiniteStopKey)
      stop.write(out);
    out.writeBoolean(startKeyInclusive);
    out.writeBoolean(stopKeyInclusive);
  }
  
  public boolean isStartKeyInclusive() {
    return startKeyInclusive;
  }
  
  public boolean isEndKeyInclusive() {
    return stopKeyInclusive;
  }
  
  public TRange toThrift() {
    return new TRange(start == null ? null : start.toThrift(), stop == null ? null : stop.toThrift(), startKeyInclusive, stopKeyInclusive, infiniteStartKey,
        infiniteStopKey);
  }
  
  public boolean isInfiniteStartKey() {
    return infiniteStartKey;
  }
  
  public boolean isInfiniteStopKey() {
    return infiniteStopKey;
  }
}

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
import java.io.InvalidObjectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class is used to specify a range of Accumulo Keys.
 *
 */

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
   * Creates a range from startRow to endRow
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
   * Creates a range from startRow to endRow
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
   * Creates a range from startKey to endKey
   *
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
   * Copies a range
   */
  public Range(Range range) {
    this(range.start, range.startKeyInclusive, range.infiniteStartKey, range.stop, range.stopKeyInclusive, range.infiniteStopKey);
  }

  /**
   * Creates a range from start to stop.
   *
   * @param start
   *          set this to null when negative infinity is needed
   * @param stop
   *          set this to null when infinity is needed
   * @param startKeyInclusive
   *          determines if the ranges includes the start key
   * @param stopKeyInclusive
   *          determines if the range includes the end key
   * @param infiniteStartKey
   *          true if start key is negative infinity (null)
   * @param infiniteStopKey
   *          true if stop key is positive infinity (null)
   * @throws IllegalArgumentException
   *           if stop is before start, or infiniteStartKey is true but start is not null, or infiniteStopKey is true but stop is not null
   */
  public Range(Key start, Key stop, boolean startKeyInclusive, boolean stopKeyInclusive, boolean infiniteStartKey, boolean infiniteStopKey) {
    this(start, startKeyInclusive, infiniteStartKey, stop, stopKeyInclusive, infiniteStopKey);
    if (!infiniteStartKey && !infiniteStopKey && beforeStartKey(stop)) {
      throw new IllegalArgumentException("Start key must be less than end key in range (" + start + ", " + stop + ")");
    }
  }

  /**
   * Creates a range from start to stop. Unlike the public six-argument method, this one does not assure that stop is after start, which helps performance in
   * cases where that assurance is already in place.
   *
   * @param start
   *          set this to null when negative infinity is needed
   * @param startKeyInclusive
   *          determines if the ranges includes the start key
   * @param infiniteStartKey
   *          true if start key is negative infinity (null)
   * @param stop
   *          set this to null when infinity is needed
   * @param stopKeyInclusive
   *          determines if the range includes the end key
   * @param infiniteStopKey
   *          true if stop key is positive infinity (null)
   * @throws IllegalArgumentException
   *           if infiniteStartKey is true but start is not null, or infiniteStopKey is true but stop is not null
   */
  protected Range(Key start, boolean startKeyInclusive, boolean infiniteStartKey, Key stop, boolean stopKeyInclusive, boolean infiniteStopKey) {
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
    this(trange.start == null ? null : new Key(trange.start), trange.startKeyInclusive, trange.infiniteStartKey, trange.stop == null ? null : new Key(
        trange.stop), trange.stopKeyInclusive, trange.infiniteStopKey);
    if (!infiniteStartKey && !infiniteStopKey && beforeStartKey(stop)) {
      throw new IllegalArgumentException("Start key must be less than end key in range (" + start + ", " + stop + ")");
    }
  }

  /**
   * @return the first key in the range, null if the key is infinite
   */
  public Key getStartKey() {
    if (infiniteStartKey) {
      return null;
    }
    return start;
  }

  /**
   *
   * @return true if the given key is before the range, otherwise false
   */
  public boolean beforeStartKey(Key key) {
    if (infiniteStartKey) {
      return false;
    }

    if (startKeyInclusive)
      return key.compareTo(start) < 0;
    return key.compareTo(start) <= 0;
  }

  /**
   * @return the last key in the range, null if the end key is infinite
   */

  public Key getEndKey() {
    if (infiniteStopKey) {
      return null;
    }
    return stop;
  }

  /**
   * @return true if the given key is after the range, otherwise false
   */

  public boolean afterEndKey(Key key) {
    if (infiniteStopKey)
      return false;

    if (stopKeyInclusive)
      return stop.compareTo(key) < 0;
    return stop.compareTo(key) <= 0;
  }

  @Override
  public int hashCode() {
    int startHash = infiniteStartKey ? 0 : start.hashCode() + (startKeyInclusive ? 1 : 0);
    int stopHash = infiniteStopKey ? 0 : stop.hashCode() + (stopKeyInclusive ? 1 : 0);

    return startHash + stopHash;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Range)
      return equals((Range) o);
    return false;
  }

  public boolean equals(Range otherRange) {

    return compareTo(otherRange) == 0;
  }

  /**
   * Compares this range to another range. Compares in the order start key, inclusiveness of start key, end key, inclusiveness of end key. Infinite keys sort
   * first, and non-infinite keys are compared with {@link Key#compareTo(Key)}. Inclusive sorts before non-inclusive.
   */
  @Override
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

  /**
   *
   * @return true if the given key falls within the range
   */
  public boolean contains(Key key) {
    return !beforeStartKey(key) && !afterEndKey(key);
  }

  /**
   * Takes a collection on range and merges overlapping and adjacent ranges. For example given the following input
   *
   * <pre>
   * [a,c], (c, d], (g,m), (j,t]
   * </pre>
   *
   * the following ranges would be returned
   *
   * <pre>
   * [a,d], (g,t]
   * </pre>
   *
   * @return list of merged ranges
   */

  public static List<Range> mergeOverlapping(Collection<Range> ranges) {
    if (ranges.size() == 0)
      return Collections.emptyList();
    if (ranges.size() == 1)
      return Collections.singletonList(ranges.iterator().next());

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
        assert currentRange.infiniteStartKey;
        startKeysEqual = true;
      } else if (currentRange.infiniteStartKey) {
        startKeysEqual = false;
      } else if (currentRange.start.equals(range.start)) {
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

  /**
   * Creates a range which represents the intersection of this range and the passed in range. The following example will print true.
   *
   * <pre>
   * Range range1 = new Range(&quot;a&quot;, &quot;f&quot;);
   * Range range2 = new Range(&quot;c&quot;, &quot;n&quot;);
   * Range range3 = range1.clip(range2);
   * System.out.println(range3.equals(new Range(&quot;c&quot;, &quot;f&quot;)));
   * </pre>
   *
   * @return the intersection
   * @throws IllegalArgumentException
   *           if ranges does not overlap
   */

  public Range clip(Range range) {
    return clip(range, false);
  }

  /**
   * Same as other clip function except if gives the option to return null of the ranges do not overlap instead of throwing an exception.
   *
   * @see Range#clip(Range)
   * @param returnNullIfDisjoint
   *          If the ranges do not overlap and true is passed, then null is returned otherwise an exception is thrown.
   * @return the intersection
   */

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

  /**
   * Creates a new range that is bounded by the columns passed in. The stary key in the returned range will have a column >= to the minimum column. The end key
   * in the returned range will have a column <= the max column.
   *
   * @return a column bounded range
   * @throws IllegalArgumentException
   *           if min > max
   */

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

  @Override
  public String toString() {
    return ((startKeyInclusive && start != null) ? "[" : "(") + (start == null ? "-inf" : start) + "," + (stop == null ? "+inf" : stop)
        + ((stopKeyInclusive && stop != null) ? "]" : ")");
  }

  @Override
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

    if (!infiniteStartKey && !infiniteStopKey && beforeStartKey(stop)) {
      throw new InvalidObjectException("Start key must be less than end key in range (" + start + ", " + stop + ")");
    }
  }

  @Override
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

  /**
   * Creates a range that covers an exact row Returns the same Range as new Range(row)
   *
   * @param row
   *          all keys in the range will have this row
   */
  public static Range exact(Text row) {
    return new Range(row);
  }

  /**
   * Creates a range that covers an exact row and column family
   *
   * @param row
   *          all keys in the range will have this row
   *
   * @param cf
   *          all keys in the range will have this column family
   */
  public static Range exact(Text row, Text cf) {
    Key startKey = new Key(row, cf);
    return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM), false);
  }

  /**
   * Creates a range that covers an exact row, column family, and column qualifier
   *
   * @param row
   *          all keys in the range will have this row
   *
   * @param cf
   *          all keys in the range will have this column family
   *
   * @param cq
   *          all keys in the range will have this column qualifier
   */
  public static Range exact(Text row, Text cf, Text cq) {
    Key startKey = new Key(row, cf, cq);
    return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
  }

  /**
   * Creates a range that covers an exact row, column family, column qualifier, and visibility
   *
   * @param row
   *          all keys in the range will have this row
   *
   * @param cf
   *          all keys in the range will have this column family
   *
   * @param cq
   *          all keys in the range will have this column qualifier
   *
   * @param cv
   *          all keys in the range will have this column visibility
   */
  public static Range exact(Text row, Text cf, Text cq, Text cv) {
    Key startKey = new Key(row, cf, cq, cv);
    return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS), false);
  }

  /**
   * Creates a range that covers an exact row, column family, column qualifier, visibility, and timestamp
   *
   * @param row
   *          all keys in the range will have this row
   *
   * @param cf
   *          all keys in the range will have this column family
   *
   * @param cq
   *          all keys in the range will have this column qualifier
   *
   * @param cv
   *          all keys in the range will have this column visibility
   *
   * @param ts
   *          all keys in the range will have this timestamp
   */
  public static Range exact(Text row, Text cf, Text cq, Text cv, long ts) {
    Key startKey = new Key(row, cf, cq, cv, ts);
    return new Range(startKey, true, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), false);
  }

  /**
   * Returns a Text that sorts just after all Texts beginning with a prefix
   */
  public static Text followingPrefix(Text prefix) {
    byte[] prefixBytes = prefix.getBytes();

    // find the last byte in the array that is not 0xff
    int changeIndex = prefix.getLength() - 1;
    while (changeIndex >= 0 && prefixBytes[changeIndex] == (byte) 0xff)
      changeIndex--;
    if (changeIndex < 0)
      return null;

    // copy prefix bytes into new array
    byte[] newBytes = new byte[changeIndex + 1];
    System.arraycopy(prefixBytes, 0, newBytes, 0, changeIndex + 1);

    // increment the selected byte
    newBytes[changeIndex]++;
    return new Text(newBytes);
  }

  /**
   * Returns a Range that covers all rows beginning with a prefix
   *
   * @param rowPrefix
   *          all keys in the range will have rows that begin with this prefix
   */
  public static Range prefix(Text rowPrefix) {
    Text fp = followingPrefix(rowPrefix);
    return new Range(new Key(rowPrefix), true, fp == null ? null : new Key(fp), false);
  }

  /**
   * Returns a Range that covers all column families beginning with a prefix within a given row
   *
   * @param row
   *          all keys in the range will have this row
   *
   * @param cfPrefix
   *          all keys in the range will have column families that begin with this prefix
   */
  public static Range prefix(Text row, Text cfPrefix) {
    Text fp = followingPrefix(cfPrefix);
    return new Range(new Key(row, cfPrefix), true, fp == null ? new Key(row).followingKey(PartialKey.ROW) : new Key(row, fp), false);
  }

  /**
   * Returns a Range that covers all column qualifiers beginning with a prefix within a given row and column family
   *
   * @param row
   *          all keys in the range will have this row
   *
   * @param cf
   *          all keys in the range will have this column family
   *
   * @param cqPrefix
   *          all keys in the range will have column qualifiers that begin with this prefix
   */
  public static Range prefix(Text row, Text cf, Text cqPrefix) {
    Text fp = followingPrefix(cqPrefix);
    return new Range(new Key(row, cf, cqPrefix), true, fp == null ? new Key(row, cf).followingKey(PartialKey.ROW_COLFAM) : new Key(row, cf, fp), false);
  }

  /**
   * Returns a Range that covers all column visibilities beginning with a prefix within a given row, column family, and column qualifier
   *
   * @param row
   *          all keys in the range will have this row
   *
   * @param cf
   *          all keys in the range will have this column family
   *
   * @param cq
   *          all keys in the range will have this column qualifier
   *
   * @param cvPrefix
   *          all keys in the range will have column visibilities that begin with this prefix
   */
  public static Range prefix(Text row, Text cf, Text cq, Text cvPrefix) {
    Text fp = followingPrefix(cvPrefix);
    return new Range(new Key(row, cf, cq, cvPrefix), true, fp == null ? new Key(row, cf, cq).followingKey(PartialKey.ROW_COLFAM_COLQUAL) : new Key(row, cf, cq,
        fp), false);
  }

  /**
   * Creates a range that covers an exact row
   *
   * @see Range#exact(Text)
   */
  public static Range exact(CharSequence row) {
    return Range.exact(new Text(row.toString()));
  }

  /**
   * Creates a range that covers an exact row and column family
   *
   * @see Range#exact(Text, Text)
   */
  public static Range exact(CharSequence row, CharSequence cf) {
    return Range.exact(new Text(row.toString()), new Text(cf.toString()));
  }

  /**
   * Creates a range that covers an exact row, column family, and column qualifier
   *
   * @see Range#exact(Text, Text, Text)
   */
  public static Range exact(CharSequence row, CharSequence cf, CharSequence cq) {
    return Range.exact(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()));
  }

  /**
   * Creates a range that covers an exact row, column family, column qualifier, and visibility
   *
   * @see Range#exact(Text, Text, Text, Text)
   */
  public static Range exact(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv) {
    return Range.exact(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cv.toString()));
  }

  /**
   * Creates a range that covers an exact row, column family, column qualifier, visibility, and timestamp
   *
   * @see Range#exact(Text, Text, Text, Text, long)
   */
  public static Range exact(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cv, long ts) {
    return Range.exact(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cv.toString()), ts);
  }

  /**
   * Returns a Range that covers all rows beginning with a prefix
   *
   * @see Range#prefix(Text)
   */
  public static Range prefix(CharSequence rowPrefix) {
    return Range.prefix(new Text(rowPrefix.toString()));
  }

  /**
   * Returns a Range that covers all column families beginning with a prefix within a given row
   *
   * @see Range#prefix(Text, Text)
   */
  public static Range prefix(CharSequence row, CharSequence cfPrefix) {
    return Range.prefix(new Text(row.toString()), new Text(cfPrefix.toString()));
  }

  /**
   * Returns a Range that covers all column qualifiers beginning with a prefix within a given row and column family
   *
   * @see Range#prefix(Text, Text, Text)
   */
  public static Range prefix(CharSequence row, CharSequence cf, CharSequence cqPrefix) {
    return Range.prefix(new Text(row.toString()), new Text(cf.toString()), new Text(cqPrefix.toString()));
  }

  /**
   * Returns a Range that covers all column visibilities beginning with a prefix within a given row, column family, and column qualifier
   *
   * @see Range#prefix(Text, Text, Text, Text)
   */
  public static Range prefix(CharSequence row, CharSequence cf, CharSequence cq, CharSequence cvPrefix) {
    return Range.prefix(new Text(row.toString()), new Text(cf.toString()), new Text(cq.toString()), new Text(cvPrefix.toString()));
  }
}

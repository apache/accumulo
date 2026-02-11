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
package org.apache.accumulo.core.util;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.RowRange;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class RowRangeUtil {

  /**
   * Validates a range is a valid KeyExtent data range, which is a special type of row range. These
   * ranges are created by calling {@code new Range(startRow, false, endRow, true);} which is what
   * {@link KeyExtent#toDataRange()} does.
   *
   * A KeyExtent data row range is defined as:
   * <ul>
   * <li>A range that has an inclusive start and exclusive end</li>
   * <li>A range that only has the row portion set</li>
   * <li>A range where both the start and end key end with a zero byte</li>
   * </ul>
   *
   * @param range The range to validate
   * @return The original range
   */
  public static Range requireKeyExtentDataRange(Range range) {
    String errorMsg = "Range is not a KeyExtent data range";

    if (!range.isInfiniteStartKey()) {
      Preconditions.checkArgument(range.isStartKeyInclusive(),
          "%s, start key must be inclusive. %s", errorMsg, range);
      Preconditions.checkArgument(isOnlyRowSet(range.getStartKey()),
          "%s, start key must only contain a row. %s", errorMsg, range);
      Preconditions.checkArgument(isRowSuffixZeroByte(range.getStartKey()),
          "%s, start key does not end with zero byte. %s, ", errorMsg, range);
    }

    if (!range.isInfiniteStopKey()) {
      Preconditions.checkArgument(!range.isEndKeyInclusive(), "%s, end key must be exclusive. %s",
          errorMsg, range);
      Preconditions.checkArgument(isOnlyRowSet(range.getEndKey()),
          "%s, end key must only contain a row. %s", errorMsg, range);
      Preconditions.checkArgument(isRowSuffixZeroByte(range.getEndKey()),
          "%s, end key does not end with a zero byte. %s, ", errorMsg, range);
    }

    return range;
  }

  public static boolean isOnlyRowSet(Key key) {
    return key.getColumnFamilyData().length() == 0 && key.getColumnQualifierData().length() == 0
        && key.getColumnVisibilityData().length() == 0 && key.getTimestamp() == Long.MAX_VALUE;
  }

  public static boolean isRowSuffixZeroByte(Key key) {
    var row = key.getRowData();
    return row.length() > 0 && row.byteAt(row.length() - 1) == (byte) 0x00;
  }

  public static ByteSequence stripZeroTail(ByteSequence row) {
    if (row.byteAt(row.length() - 1) == (byte) 0x00) {
      return row.subSequence(0, row.length() - 1);
    }
    return row;
  }

  /**
   * @throws IllegalArgumentException if the range was not created using row constructors.
   */
  public static Range requireRowRange(Range range) {
    String errorMsg = "Range is not a row range";

    if (!range.isInfiniteStartKey()) {
      Preconditions.checkArgument(range.isStartKeyInclusive(),
          "%s, start key must be inclusive. %s", errorMsg, range);
      Preconditions.checkArgument(isOnlyRowSet(range.getStartKey()),
          "%s, start key must only contain a row. %s", errorMsg, range);
    }

    if (!range.isInfiniteStopKey()) {
      Preconditions.checkArgument(!range.isEndKeyInclusive(), "%s, end key must be exclusive. %s",
          errorMsg, range);
      Preconditions.checkArgument(isOnlyRowSet(range.getEndKey()),
          "%s, end key must only contain a row. %s", errorMsg, range);
    }

    return range;
  }

  /**
   * Converts a {@link Range} created using row constructors to a {@link RowRange}.
   *
   * @throws IllegalArgumentException if the range was not created using row constructors.
   */
  public static RowRange toRowRange(Range range) {
    requireRowRange(range);

    Text start;
    boolean startInclusive;
    Text end;
    boolean endInclusive;

    // This code reverses the transformations done by the Range row constructor. Like when that
    // constructor adds a trailing zero to a row, this code strips it to recover the original row
    // passed to the constructor.

    if (range.isInfiniteStartKey()) {
      start = null;
      startInclusive = true;
    } else {
      if (isRowSuffixZeroByte(range.getStartKey())) {
        start = new Text(stripZeroTail(range.getStartKey().getRowData()).toArray());
        startInclusive = false;
      } else {
        start = range.getStartKey().getRow();
        startInclusive = true;
      }
    }

    if (range.isInfiniteStopKey()) {
      end = null;
      endInclusive = true;
    } else {
      if (isRowSuffixZeroByte(range.getEndKey())) {
        end = new Text(stripZeroTail(range.getEndKey().getRowData()).toArray());
        endInclusive = true;
      } else {
        end = range.getEndKey().getRow();
        endInclusive = false;
      }
    }

    return RowRange.range(start, startInclusive, end, endInclusive);
  }
}

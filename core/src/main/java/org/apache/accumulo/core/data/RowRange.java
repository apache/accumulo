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

import java.nio.ByteBuffer;

import org.apache.accumulo.core.dataImpl.thrift.TRowRange;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

/**
 * This class is used to specify a range of rows.
 */
public class RowRange {
  final private Text startRow;
  final private Text endRow;
  final private boolean startRowInclusive;
  final private boolean endRowInclusive;
  final private boolean infiniteStartRow;
  final private boolean infiniteEndRow;

  /**
   * Creates a range that includes all possible rows.
   */
  public static RowRange all() {
    return range((Text) null, true, null, true);
  }

  /**
   * Creates a range of rows from startRow exclusive to endRow exclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange open(Text startRow, Text endRow) {
    return range(startRow, false, endRow, false);
  }

  /**
   * Creates a range of rows from startRow exclusive to endRow exclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange open(CharSequence startRow, CharSequence endRow) {
    return range(startRow, false, endRow, false);
  }

  /**
   * Creates a range of rows from startRow inclusive to endRow inclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange closed(Text startRow, Text endRow) {
    return range(startRow, true, endRow, true);
  }

  /**
   * Creates a range of rows from startRow inclusive to endRow inclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange closed(CharSequence startRow, CharSequence endRow) {
    return range(startRow, true, endRow, true);
  }

  /**
   * Creates a range that covers an entire row.
   *
   * @param row row to cover; set to null to cover all rows
   */
  public static RowRange closed(Text row) {
    return range(row, true, row, true);
  }

  /**
   * Creates a range that covers an entire row.
   *
   * @param row row to cover; set to null to cover all rows
   */
  public static RowRange closed(CharSequence row) {
    return range(row, true, row, true);
  }

  /**
   * Creates a range of rows from startRow exclusive to endRow inclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange openClosed(Text startRow, Text endRow) {
    return range(startRow, false, endRow, true);
  }

  /**
   * Creates a range of rows from startRow exclusive to endRow inclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange openClosed(CharSequence startRow, CharSequence endRow) {
    return range(startRow, false, endRow, true);
  }

  /**
   * Creates a range of rows from startRow inclusive to endRow exclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange closedOpen(Text startRow, Text endRow) {
    return range(startRow, true, endRow, false);
  }

  /**
   * Creates a range of rows from startRow inclusive to endRow exclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange closedOpen(CharSequence startRow, CharSequence endRow) {
    return range(startRow, true, endRow, false);
  }

  /**
   * Creates a range of rows strictly greater than startRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   */
  public static RowRange greaterThan(Text startRow) {
    return range(startRow, false, null, true);
  }

  /**
   * Creates a range of rows strictly greater than startRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   */
  public static RowRange greaterThan(CharSequence startRow) {
    return range(startRow, false, null, true);
  }

  /**
   * Creates a range of rows greater than or equal to startRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   */
  public static RowRange atLeast(Text startRow) {
    return range(startRow, true, null, true);
  }

  /**
   * Creates a range of rows greater than or equal to startRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   */
  public static RowRange atLeast(CharSequence startRow) {
    return range(startRow, true, null, true);
  }

  /**
   * Creates a range of rows strictly less than endRow.
   *
   * @param endRow ending row; set to null for positive infinity
   */
  public static RowRange lessThan(Text endRow) {
    return range(null, true, endRow, false);
  }

  /**
   * Creates a range of rows strictly less than endRow.
   *
   * @param endRow ending row; set to null for positive infinity
   */
  public static RowRange lessThan(CharSequence endRow) {
    return range(null, true, endRow, false);
  }

  /**
   * Creates a range of rows less than or equal to endRow.
   *
   * @param endRow ending row; set to null for positive infinity
   */
  public static RowRange atMost(Text endRow) {
    return range(null, true, endRow, true);
  }

  /**
   * Creates a range of rows less than or equal to endRow.
   *
   * @param endRow ending row; set to null for positive infinity
   */
  public static RowRange atMost(CharSequence endRow) {
    return range(null, true, endRow, true);
  }

  /**
   * Creates a range of rows from startRow to endRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param startInclusive true to include start row, false to skip
   * @param endRow ending row; set to null for positive infinity
   * @param endInclusive true to include end row, false to skip
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange range(Text startRow, boolean startInclusive, Text endRow,
      boolean endInclusive) {
    return new RowRange(startRow, startInclusive, endRow, endInclusive);
  }

  /**
   * Creates a range of rows from startRow to endRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param startInclusive true to include start row, false to skip
   * @param endRow ending row; set to null for positive infinity
   * @param endInclusive true to include end row, false to skip
   * @throws IllegalArgumentException if end row is before start row
   */
  public static RowRange range(CharSequence startRow, boolean startInclusive, CharSequence endRow,
      boolean endInclusive) {
    return new RowRange(startRow == null ? null : new Text(startRow.toString()), startInclusive,
        endRow == null ? null : new Text(endRow.toString()), endInclusive);
  }

  /**
   * Creates a range of rows from startRow to endRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param startInclusive true to include start row, false to skip
   * @param endRow ending row; set to null for positive infinity
   * @param endInclusive true to include end row, false to skip
   * @throws IllegalArgumentException if end row is before start row
   */
  private RowRange(Text startRow, boolean startInclusive, Text endRow, boolean endInclusive) {
    this.startRow = startRow;
    this.startRowInclusive = startInclusive;
    this.infiniteStartRow = startRow == null;
    this.endRow = endRow;
    this.endRowInclusive = endInclusive;
    this.infiniteEndRow = endRow == null;

    if (!infiniteStartRow && !infiniteEndRow && beforeStartRowImpl(endRow)) {
      throw new IllegalArgumentException(
          "Start row must be less than end row in row range (" + startRow + ", " + endRow + ")");
    }
  }

  public Text getStartRow() {
    return startRow;
  }

  public boolean isStartRowInclusive() {
    return startRowInclusive;
  }

  public Text getEndRow() {
    return endRow;
  }

  public boolean isEndRowInclusive() {
    return endRowInclusive;
  }

  /**
   * Converts this row range to Thrift.
   *
   * @return Thrift row range
   */
  public TRowRange toThrift() {
    final ByteBuffer startRow = TextUtil.getByteBuffer(this.startRow);
    final ByteBuffer endRow = TextUtil.getByteBuffer(this.endRow);
    return new TRowRange(startRow, endRow);
  }

  /**
   * Creates a row range from Thrift.
   *
   * @param rowRange Thrift row range
   */
  public static RowRange fromThrift(TRowRange rowRange) {
    final Text startRow = ByteBufferUtil.toText(rowRange.startRow);
    final Text endRow = ByteBufferUtil.toText(rowRange.endRow);
    return range(startRow, true, endRow, false);
  }

  /**
   * Determines if the given row is before the start row of this row range.
   *
   * @param row row to check
   * @return true if the given row is before the row range, otherwise false
   */
  public boolean beforeStartRow(Text row) {
    return beforeStartRowImpl(row);
  }

  /**
   * Implements logic of {@link #beforeStartRow(Text)}, but in a private method, so that it can be
   * safely used by constructors if a subclass overrides that {@link #beforeStartRow(Text)}
   */
  private boolean beforeStartRowImpl(Text row) {
    if (this.infiniteStartRow) {
      return false;
    }

    if (startRowInclusive) {
      return row.compareTo(startRow) < 0;
    }
    return row.compareTo(startRow) <= 0;
  }

  @Override
  public String toString() {
    final String startRangeSymbol = (startRowInclusive && startRow != null) ? "[" : "(";
    final String startRow = this.startRow == null ? "-inf" : this.startRow.toString();
    final String endRow = this.endRow == null ? "+inf" : this.endRow.toString();
    final String endRangeSymbol = (endRowInclusive && this.endRow != null) ? "]" : ")";
    return startRangeSymbol + startRow + "," + endRow + endRangeSymbol;
  }
}

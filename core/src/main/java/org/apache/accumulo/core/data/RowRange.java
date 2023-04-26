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

import org.apache.hadoop.io.Text;

/**
 * This class is used to specify a range of rows.
 */
public class RowRange {
  final private Text startRow;
  final private Text endRow;
  final private boolean startRowInclusive;
  final private boolean endRowInclusive;
  private final boolean infiniteStartRow;
  private final boolean infiniteEndRow;

  /**
   * Creates a range that includes all possible rows.
   */
  public RowRange() {
    this(null, true, null, true);
  }

  /**
   * Creates a range of rows from startRow inclusive to endRow inclusive.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   * @param endRow ending row; set to null for positive infinity
   * @throws IllegalArgumentException if end row is before start row
   */
  public RowRange(Text startRow, Text endRow) {
    this(startRow, true, endRow, true);
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
  public RowRange(Text startRow, boolean startInclusive, Text endRow, boolean endInclusive) {
    this.startRow = startRow;
    this.startRowInclusive = startInclusive;
    this.infiniteStartRow = startRow == null;
    this.endRow = endRow;
    this.endRowInclusive = endInclusive;
    this.infiniteEndRow = endRow == null;

    if (!infiniteStartRow && !infiniteEndRow && beforeStartKeyImpl(endRow)) {
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
   * Determines if the given row is before the start row of this row range.
   *
   * @param row row to check
   * @return true if the given row is before the row range, otherwise false
   */
  public boolean beforeStartRow(Text row) {
    return beforeStartKeyImpl(row);
  }

  /**
   * Implements logic of {@link #beforeStartRow(Text)}, but in a private method, so that it can be
   * safely used by constructors if a subclass overrides that {@link #beforeStartRow(Text)}
   */
  private boolean beforeStartKeyImpl(Text row) {
    if (infiniteStartRow) {
      return false;
    }

    if (startRowInclusive) {
      return row.compareTo(startRow) < 0;
    }
    return row.compareTo(startRow) <= 0;
  }
}

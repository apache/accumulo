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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;

/**
 * This class is used to specify a range of rows.
 *
 * @since 3.0.0
 */
public class RowRange implements Comparable<RowRange> {

  private static final Comparator<Text> START_ROW_COMPARATOR =
      Comparator.nullsFirst(Text::compareTo);
  private static final Comparator<Text> END_ROW_COMPARATOR = Comparator.nullsLast(Text::compareTo);
  public static final Comparator<RowRange> ROW_RANGE_COMPARATOR = (r1, r2) -> {
    if (r1.startRow == null && r2.startRow == null) {
      return 0;
    } else if (r1.startRow == null) {
      return -1;
    } else if (r2.startRow == null) {
      return 1;
    }
    return r1.compareTo(r2);
  };

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
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.lessThan(row)?");
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.greaterThan(row)?");
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
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.lessThan(row)?");
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.greaterThan(row)?");
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
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.atMost(row)?");
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.atLeast(row)?");
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
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.atMost(row)?");
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.atLeast(row)?");
    return range(startRow, true, endRow, true);
  }

  /**
   * Creates a range that covers an entire row.
   *
   * @param row row to cover; set to null to cover all rows
   */
  public static RowRange closed(Text row) {
    Objects.requireNonNull(row, "Did you mean to use RowRange.all()?");
    return range(row, true, row, true);
  }

  /**
   * Creates a range that covers an entire row.
   *
   * @param row row to cover; set to null to cover all rows
   */
  public static RowRange closed(CharSequence row) {
    Objects.requireNonNull(row, "Did you mean to use RowRange.all()?");
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
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.atMost(row)?");
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.atLeast(row)?");
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
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.atMost(row)?");
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.atLeast(row)?");
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
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.lessThan(row)?");
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.moreThan(row)?");
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
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.lessThan(row)?");
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.moreThan(row)?");
    return range(startRow, true, endRow, false);
  }

  /**
   * Creates a range of rows strictly greater than startRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   */
  public static RowRange greaterThan(Text startRow) {
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.all()?");
    return range(startRow, false, null, true);
  }

  /**
   * Creates a range of rows strictly greater than startRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   */
  public static RowRange greaterThan(CharSequence startRow) {
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.all()?");
    return range(startRow, false, null, true);
  }

  /**
   * Creates a range of rows greater than or equal to startRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   */
  public static RowRange atLeast(Text startRow) {
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.all()?");
    return range(startRow, true, null, true);
  }

  /**
   * Creates a range of rows greater than or equal to startRow.
   *
   * @param startRow starting row; set to null for the smallest possible row (an empty one)
   */
  public static RowRange atLeast(CharSequence startRow) {
    Objects.requireNonNull(startRow, "Did you mean to use RowRange.all()?");
    return range(startRow, true, null, true);
  }

  /**
   * Creates a range of rows strictly less than endRow.
   *
   * @param endRow ending row; set to null for positive infinity
   */
  public static RowRange lessThan(Text endRow) {
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.all()?");
    return range(null, true, endRow, false);
  }

  /**
   * Creates a range of rows strictly less than endRow.
   *
   * @param endRow ending row; set to null for positive infinity
   */
  public static RowRange lessThan(CharSequence endRow) {
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.all()?");
    return range(null, true, endRow, false);
  }

  /**
   * Creates a range of rows less than or equal to endRow.
   *
   * @param endRow ending row; set to null for positive infinity
   */
  public static RowRange atMost(Text endRow) {
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.all()?");
    return range(null, true, endRow, true);
  }

  /**
   * Creates a range of rows less than or equal to endRow.
   *
   * @param endRow ending row; set to null for positive infinity
   */
  public static RowRange atMost(CharSequence endRow) {
    Objects.requireNonNull(endRow, "Did you mean to use RowRange.all()?");
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
   * Converts this row range to a {@link Range} object.
   */
  public Range toRange() {
    return new Range(startRow, startRowInclusive, endRow, endRowInclusive);
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
   * Determines if the given row is after the end row of this row range.
   *
   * @param row row to check
   * @return true if the given row is after the row range, otherwise false
   */
  public boolean afterEndRow(Text row) {
    if (infiniteEndRow) {
      return false;
    }

    if (endRowInclusive) {
      return row.compareTo(endRow) > 0;
    }
    return row.compareTo(endRow) >= 0;
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

  @Override
  public int hashCode() {
    int startHash = infiniteStartRow ? 0 : startRow.hashCode() + (startRowInclusive ? 1 : 0);
    int stopHash = infiniteEndRow ? 0 : endRow.hashCode() + (endRowInclusive ? 1 : 0);

    return startHash + stopHash;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof RowRange)) {
      return false;
    }
    return equals((RowRange) other);
  }

  /**
   * Determines if this row range equals another.
   *
   * @param other range to compare
   * @return true if row ranges are equals, false otherwise
   */
  public boolean equals(RowRange other) {
    return compareTo(other) == 0;
  }

  /**
   * Compares this row range to another row range. Compares in order: start row, inclusiveness of
   * start row, end row, inclusiveness of end row. Infinite rows sort first, and non-infinite rows
   * are compared with {@link Text#compareTo(BinaryComparable)}. Inclusive sorts before
   * non-inclusive.
   *
   * @param other range row to compare
   * @return comparison result
   */
  @Override
  public int compareTo(RowRange other) {
    // Compare infinite start rows
    int comp = Boolean.compare(this.infiniteStartRow, other.infiniteStartRow);

    if (comp == 0) {
      // Compare non-infinite start rows and start row inclusiveness
      if (!this.infiniteStartRow) {
        comp = START_ROW_COMPARATOR.compare(this.startRow, other.startRow);
        if (comp == 0) {
          comp = Boolean.compare(other.startRowInclusive, this.startRowInclusive);
        }
      }
    }

    if (comp == 0) {
      // Compare infinite end rows
      comp = Boolean.compare(this.infiniteEndRow, other.infiniteEndRow);

      // Compare non-infinite end rows and end row inclusiveness
      if (comp == 0 && !this.infiniteEndRow) {
        comp = END_ROW_COMPARATOR.compare(this.endRow, other.endRow);
        if (comp == 0) {
          comp = Boolean.compare(this.endRowInclusive, other.endRowInclusive);
        }
      }
    }

    return comp;
  }

  /**
   * Determines if this row range contains the given row.
   *
   * @param row row to check
   * @return true if the row is contained in the row range, false otherwise
   */
  public boolean contains(Text row) {
    if (infiniteStartRow) {
      return !afterEndRow(row);
    } else if (infiniteEndRow) {
      return !beforeStartRow(row);
    } else {
      return !beforeStartRow(row) && !afterEndRow(row);
    }
  }

  /**
   * Merges overlapping and adjacent row ranges. For example, given the following input:
   *
   * <pre>
   * [a,c], (c, d], (g,m), (j,t]
   * </pre>
   *
   * the following row ranges would be returned:
   *
   * <pre>
   * [a,d], (g,t]
   * </pre>
   *
   * @param rowRanges the collection of row ranges to merge
   * @return a list of merged row ranges
   */
  public static List<RowRange> mergeOverlapping(Collection<RowRange> rowRanges) {
    if (rowRanges.isEmpty()) {
      return Collections.emptyList();
    }
    if (rowRanges.size() == 1) {
      return Collections.singletonList(rowRanges.iterator().next());
    }

    List<RowRange> sortedRowRanges = new ArrayList<>(rowRanges);
    // Sort row ranges by their startRow values
    sortedRowRanges.sort(ROW_RANGE_COMPARATOR);

    ArrayList<RowRange> mergedRowRanges = new ArrayList<>(rowRanges.size());

    // Initialize the current range for merging
    RowRange currentRange = sortedRowRanges.get(0);
    boolean currentStartRowInclusive = sortedRowRanges.get(0).startRowInclusive;

    // Iterate through the sorted row ranges, merging overlapping and adjacent ranges
    for (int i = 1; i < sortedRowRanges.size(); i++) {
      if (currentRange.infiniteStartRow && currentRange.infiniteEndRow) {
        // The current range covers all possible rows, no further merging needed
        break;
      }

      RowRange nextRange = sortedRowRanges.get(i);

      boolean startRowsEqual = (currentRange.startRow == null && nextRange.startRow == null)
          || (currentRange.startRow != null && currentRange.startRow.equals(nextRange.startRow));

      int comparison;
      if (startRowsEqual || currentRange.infiniteEndRow
          || (nextRange.startRow != null && (currentRange.endRow == null
              || currentRange.endRow.compareTo(nextRange.startRow) > 0
              || (currentRange.endRow.equals(nextRange.startRow)
                  && (!currentRange.endRowInclusive || nextRange.startRowInclusive))))) {
        if (nextRange.infiniteEndRow) {
          comparison = 1;
        } else if (currentRange.endRow == null) {
          comparison = -1;
        } else {
          comparison = nextRange.endRow.compareTo(currentRange.endRow);
        }
        if (comparison > 0 || (comparison == 0 && nextRange.endRowInclusive)) {
          currentRange = RowRange.range(currentRange.startRow, currentStartRowInclusive,
              nextRange.endRow, nextRange.endRowInclusive);
        } // else current range contains the next range
      } else {
        // No overlap or adjacency, add the current range to the merged list and update the current
        // range
        mergedRowRanges.add(currentRange);
        currentRange = nextRange;
        currentStartRowInclusive = nextRange.startRowInclusive;
      }
    }

    // Add the final current range to the merged list
    mergedRowRanges.add(currentRange);

    return mergedRowRanges;
  }

  /**
   * Creates a row range which represents the intersection of this row range and the passed in row
   * range. The following example will print true.
   *
   * <pre>
   * RowRange rowRange1 = RowRange.closed(&quot;a&quot;, &quot;f&quot;);
   * RowRange rowRange2 = RowRange.closed(&quot;c&quot;, &quot;n&quot;);
   * RowRange rowRange3 = rowRange1.clip(rowRange2);
   * System.out.println(rowRange3.equals(RowRange.closed(&quot;c&quot;, &quot;f&quot;)));
   * </pre>
   *
   * @param rowRange row range to clip to
   * @return the intersection of this row range and the given row range
   * @throws IllegalArgumentException if row ranges do not overlap
   */
  public RowRange clip(RowRange rowRange) {
    return clip(rowRange, false);
  }

  /**
   * Creates a row range which represents the intersection of this row range and the passed in row
   * range. Unlike {@link #clip(RowRange)}, this method can optionally return null if the row ranges
   * do not overlap, instead of throwing an exception. The returnNullIfDisjoint parameter controls
   * this behavior.
   *
   * @param rowRange row range to clip to
   * @param returnNullIfDisjoint true to return null if row ranges are disjoint, false to throw an
   *        exception
   * @return the intersection of this row range and the given row range, or null if row ranges do
   *         not overlap and returnNullIfDisjoint is true
   * @throws IllegalArgumentException if row ranges do not overlap and returnNullIfDisjoint is false
   * @see #clip(RowRange)
   */
  public RowRange clip(RowRange rowRange, boolean returnNullIfDisjoint) {
    // Initialize start and end row values with the current instance's values
    Text startRow = this.startRow;
    boolean startRowInclusive = this.startRowInclusive;
    Text endRow = this.endRow;
    boolean endRowInclusive = this.endRowInclusive;

    // If the input rowRange has a defined startRow, update startRow and startRowInclusive if needed
    if (rowRange.startRow != null) {
      // If the input rowRange's startRow is after this instance's endRow or equal but not
      // inclusive, they do not overlap
      if (afterEndRow(rowRange.startRow) || (rowRange.startRow.equals(this.endRow)
          && !(rowRange.startRowInclusive && this.endRowInclusive))) {
        if (returnNullIfDisjoint) {
          return null;
        }
        throw new IllegalArgumentException("RowRange " + rowRange + " does not overlap " + this);
      } else if (!beforeStartRow(rowRange.startRow)) {
        // If the input rowRange's startRow is within this instance's range, use it as the new
        // startRow
        startRow = rowRange.startRow;
        startRowInclusive = rowRange.startRowInclusive;
      }
    }

    // If the input rowRange has a defined endRow, update endRow and endRowInclusive if needed
    if (rowRange.endRow != null) {
      // If the input rowRange's endRow is before this instance's startRow or equal but not
      // inclusive, they do not overlap
      if (beforeStartRow(rowRange.endRow) || (rowRange.endRow.equals(this.startRow)
          && !(rowRange.endRowInclusive && this.startRowInclusive))) {
        if (returnNullIfDisjoint) {
          return null;
        }
        throw new IllegalArgumentException("RowRange " + rowRange + " does not overlap " + this);
      } else if (!afterEndRow(rowRange.endRow)) {
        // If the input rowRange's endRow is within this instance's range, use it as the new endRow
        endRow = rowRange.endRow;
        endRowInclusive = rowRange.endRowInclusive;
      }
    }

    // Return a new RowRange instance representing the intersection of the two ranges
    return RowRange.range(startRow, startRowInclusive, endRow, endRowInclusive);
  }

}

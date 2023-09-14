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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;

/**
 * This class is used to specify a range of rows.
 *
 * @since 3.0.0
 */
public class RowRange implements Comparable<RowRange> {

  private static final Comparator<Text> LOWER_BOUND_COMPARATOR =
      Comparator.nullsFirst(Text::compareTo);
  private static final Comparator<Text> UPPER_BOUND_COMPARATOR =
      Comparator.nullsLast(Text::compareTo);
  private static final Comparator<RowRange> ROW_RANGE_COMPARATOR = (r1, r2) -> {
    if (r1.lowerBound == null && r2.lowerBound == null) {
      return 0;
    } else if (r1.lowerBound == null) {
      return -1;
    } else if (r2.lowerBound == null) {
      return 1;
    }
    return r1.compareTo(r2);
  };

  final private Text lowerBound;
  final private Text upperBound;
  final private boolean lowerBoundInclusive;
  final private boolean upperBoundInclusive;
  final private boolean infiniteLowerBound;
  final private boolean infiniteUpperBound;

  /**
   * Creates a range that includes all possible rows.
   */
  public static RowRange all() {
    return range((Text) null, true, null, true);
  }

  /**
   * Creates a range of rows from lowerBound exclusive to upperBound exclusive.
   *
   * @param lowerBound starting row
   * @param upperBound ending row
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange open(Text lowerBound, Text upperBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.lessThan(row)?");
    requireNonNull(upperBound, "Did you mean to use RowRange.greaterThan(row)?");
    return range(lowerBound, false, upperBound, false);
  }

  /**
   * Creates a range of rows from lowerBound exclusive to upperBound exclusive.
   *
   * @param lowerBound starting row
   * @param upperBound ending row
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange open(CharSequence lowerBound, CharSequence upperBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.lessThan(row)?");
    requireNonNull(upperBound, "Did you mean to use RowRange.greaterThan(row)?");
    return range(lowerBound, false, upperBound, false);
  }

  /**
   * Creates a range of rows from lowerBound inclusive to upperBound inclusive.
   *
   * @param lowerBound starting row
   * @param upperBound ending row
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange closed(Text lowerBound, Text upperBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.atMost(row)?");
    requireNonNull(upperBound, "Did you mean to use RowRange.atLeast(row)?");
    return range(lowerBound, true, upperBound, true);
  }

  /**
   * Creates a range of rows from lowerBound inclusive to upperBound inclusive.
   *
   * @param lowerBound starting row
   * @param upperBound ending row
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange closed(CharSequence lowerBound, CharSequence upperBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.atMost(row)?");
    requireNonNull(upperBound, "Did you mean to use RowRange.atLeast(row)?");
    return range(lowerBound, true, upperBound, true);
  }

  /**
   * Creates a range that covers an entire row.
   *
   * @param row row to cover
   */
  public static RowRange closed(Text row) {
    requireNonNull(row, "Did you mean to use RowRange.all()?");
    return range(row, true, row, true);
  }

  /**
   * Creates a range that covers an entire row.
   *
   * @param row row to cover
   */
  public static RowRange closed(CharSequence row) {
    requireNonNull(row, "Did you mean to use RowRange.all()?");
    return range(row, true, row, true);
  }

  /**
   * Creates a range of rows from lowerBound exclusive to upperBound inclusive.
   *
   * @param lowerBound starting row
   * @param upperBound ending row
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange openClosed(Text lowerBound, Text upperBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.atMost(row)?");
    requireNonNull(upperBound, "Did you mean to use RowRange.greaterThan(row)?");
    return range(lowerBound, false, upperBound, true);
  }

  /**
   * Creates a range of rows from lowerBound exclusive to upperBound inclusive.
   *
   * @param lowerBound starting row
   * @param upperBound ending row
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange openClosed(CharSequence lowerBound, CharSequence upperBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.atMost(row)?");
    requireNonNull(upperBound, "Did you mean to use RowRange.greaterThan(row)?");
    return range(lowerBound, false, upperBound, true);
  }

  /**
   * Creates a range of rows from lowerBound inclusive to upperBound exclusive.
   *
   * @param lowerBound starting row
   * @param upperBound ending row
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange closedOpen(Text lowerBound, Text upperBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.lessThan(row)?");
    requireNonNull(upperBound, "Did you mean to use RowRange.atLeast(row)?");
    return range(lowerBound, true, upperBound, false);
  }

  /**
   * Creates a range of rows from lowerBound inclusive to upperBound exclusive.
   *
   * @param lowerBound starting row
   * @param upperBound ending row
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange closedOpen(CharSequence lowerBound, CharSequence upperBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.lessThan(row)?");
    requireNonNull(upperBound, "Did you mean to use RowRange.atLeast(row)?");
    return range(lowerBound, true, upperBound, false);
  }

  /**
   * Creates a range of rows strictly greater than the given row.
   *
   * @param lowerBound starting row
   */
  public static RowRange greaterThan(Text lowerBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.all()?");
    return range(lowerBound, false, null, true);
  }

  /**
   * Creates a range of rows strictly greater than the given row.
   *
   * @param lowerBound starting row
   */
  public static RowRange greaterThan(CharSequence lowerBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.all()?");
    return range(lowerBound, false, null, true);
  }

  /**
   * Creates a range of rows greater than or equal to the given row.
   *
   * @param lowerBound starting row
   */
  public static RowRange atLeast(Text lowerBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.all()?");
    return range(lowerBound, true, null, true);
  }

  /**
   * Creates a range of rows greater than or equal to the given row.
   *
   * @param lowerBound starting row
   */
  public static RowRange atLeast(CharSequence lowerBound) {
    requireNonNull(lowerBound, "Did you mean to use RowRange.all()?");
    return range(lowerBound, true, null, true);
  }

  /**
   * Creates a range of rows strictly less than the given row.
   *
   * @param upperBound ending row
   */
  public static RowRange lessThan(Text upperBound) {
    requireNonNull(upperBound, "Did you mean to use RowRange.all()?");
    return range(null, true, upperBound, false);
  }

  /**
   * Creates a range of rows strictly less than the given row.
   *
   * @param upperBound ending row
   */
  public static RowRange lessThan(CharSequence upperBound) {
    requireNonNull(upperBound, "Did you mean to use RowRange.all()?");
    return range(null, true, upperBound, false);
  }

  /**
   * Creates a range of rows less than or equal to the given row.
   *
   * @param upperBound ending row
   */
  public static RowRange atMost(Text upperBound) {
    requireNonNull(upperBound, "Did you mean to use RowRange.all()?");
    return range(null, true, upperBound, true);
  }

  /**
   * Creates a range of rows less than or equal to the given row.
   *
   * @param upperBound ending row
   */
  public static RowRange atMost(CharSequence upperBound) {
    requireNonNull(upperBound, "Did you mean to use RowRange.all()?");
    return range(null, true, upperBound, true);
  }

  /**
   * Creates a range of rows from the given lowerBound to the given upperBound.
   *
   * @param lowerBound starting row; set to null for the smallest possible row (an empty one)
   * @param lowerBoundInclusive set to true to include lower bound, false to skip
   * @param upperBound ending row; set to null for positive infinity
   * @param upperBoundInclusive set to true to include upper bound, false to skip
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange range(Text lowerBound, boolean lowerBoundInclusive, Text upperBound,
      boolean upperBoundInclusive) {
    return new RowRange(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);
  }

  /**
   * Creates a range of rows from the given lowerBound to the given upperBound.
   *
   * @param lowerBound starting row; set to null for the smallest possible row (an empty one)
   * @param lowerBoundInclusive set to true to include lower bound, false to skip
   * @param upperBound ending row; set to null for positive infinity
   * @param upperBoundInclusive set to true to include upper bound, false to skip
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  public static RowRange range(CharSequence lowerBound, boolean lowerBoundInclusive,
      CharSequence upperBound, boolean upperBoundInclusive) {
    return new RowRange(lowerBound == null ? null : new Text(lowerBound.toString()),
        lowerBoundInclusive, upperBound == null ? null : new Text(upperBound.toString()),
        upperBoundInclusive);
  }

  /**
   * Creates a range of rows from the given lowerBound to the given upperBound.
   *
   * @param lowerBound starting row; set to null for the smallest possible row (an empty one)
   * @param lowerBoundInclusive set to true to include lower bound, false to skip
   * @param upperBound ending row; set to null for positive infinity
   * @param upperBoundInclusive set to true to include upper bound, false to skip
   * @throws IllegalArgumentException if upper bound is before lower bound
   */
  private RowRange(Text lowerBound, boolean lowerBoundInclusive, Text upperBound,
      boolean upperBoundInclusive) {
    this.lowerBound = lowerBound;
    this.lowerBoundInclusive = lowerBoundInclusive;
    this.infiniteLowerBound = lowerBound == null;
    this.upperBound = upperBound;
    this.upperBoundInclusive = upperBoundInclusive;
    this.infiniteUpperBound = upperBound == null;

    if (!infiniteLowerBound && !infiniteUpperBound && isAfterImpl(upperBound)) {
      throw new IllegalArgumentException(
          "Lower bound must be less than upper bound in row range " + this);
    }
  }

  public Text getLowerBound() {
    return lowerBound;
  }

  /**
   * @return true if the lower bound is inclusive or null, otherwise false
   */
  public boolean isLowerBoundInclusive() {
    return lowerBoundInclusive || lowerBound == null;
  }

  public Text getUpperBound() {
    return upperBound;
  }

  /**
   * @return true if the upper bound is inclusive or null, otherwise false
   */
  public boolean isUpperBoundInclusive() {
    return upperBoundInclusive || upperBound == null;
  }

  /**
   * Converts this row range to a {@link Range} object.
   */
  public Range asRange() {
    return new Range(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);
  }

  /**
   * @param that row to check
   * @return true if this row range's lower bound is greater than the given row, otherwise false
   */
  public boolean isAfter(Text that) {
    return isAfterImpl(that);
  }

  /**
   * @param that row to check
   * @return true if this row range's upper bound is less than the given row, otherwise false
   */
  public boolean isBefore(Text that) {
    if (infiniteUpperBound) {
      return false;
    }

    if (upperBoundInclusive) {
      return that.compareTo(upperBound) > 0;
    }
    return that.compareTo(upperBound) >= 0;
  }

  /**
   * Implements logic of {@link #isAfter(Text)}, but in a private method, so that it can be safely
   * used by constructors if a subclass overrides that {@link #isAfter(Text)}
   */
  private boolean isAfterImpl(Text row) {
    if (this.infiniteLowerBound) {
      return false;
    }

    if (lowerBoundInclusive) {
      return row.compareTo(lowerBound) < 0;
    }
    return row.compareTo(lowerBound) <= 0;
  }

  @Override
  public String toString() {
    final String startRangeSymbol = (lowerBoundInclusive && lowerBound != null) ? "[" : "(";
    final String lowerBound = this.lowerBound == null ? "-inf" : this.lowerBound.toString();
    final String upperBound = this.upperBound == null ? "+inf" : this.upperBound.toString();
    final String endRangeSymbol = (upperBoundInclusive && this.upperBound != null) ? "]" : ")";
    return startRangeSymbol + lowerBound + "," + upperBound + endRangeSymbol;
  }

  @Override
  public int hashCode() {
    int lowerHash = infiniteLowerBound ? 0 : lowerBound.hashCode() + (lowerBoundInclusive ? 1 : 0);
    int upperHash = infiniteUpperBound ? 0 : upperBound.hashCode() + (upperBoundInclusive ? 1 : 0);

    return lowerHash + upperHash;
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
   * @return true if row ranges are equal, otherwise false
   */
  public boolean equals(RowRange other) {
    return compareTo(other) == 0;
  }

  /**
   * Compares this row range to another row range. Compares in order: lower bound, inclusiveness of
   * lower bound, upper bound, inclusiveness of upper bound. Infinite rows sort first, and
   * non-infinite rows are compared with {@link Text#compareTo(BinaryComparable)}. Inclusive sorts
   * before non-inclusive.
   *
   * @param other row range to compare
   * @return comparison result
   */
  @Override
  public int compareTo(RowRange other) {
    // Compare infinite lower bounds
    int comp = Boolean.compare(this.infiniteLowerBound, other.infiniteLowerBound);

    if (comp == 0) {
      // Compare non-infinite lower bounds and lower bound inclusiveness
      if (!this.infiniteLowerBound) {
        comp = LOWER_BOUND_COMPARATOR.compare(this.lowerBound, other.lowerBound);
        if (comp == 0) {
          comp = Boolean.compare(other.lowerBoundInclusive, this.lowerBoundInclusive);
        }
      }
    }

    if (comp == 0) {
      // Compare infinite upper bounds
      comp = Boolean.compare(this.infiniteUpperBound, other.infiniteUpperBound);

      // Compare non-infinite upper bounds and upper bound inclusiveness
      if (comp == 0 && !this.infiniteUpperBound) {
        comp = UPPER_BOUND_COMPARATOR.compare(this.upperBound, other.upperBound);
        if (comp == 0) {
          comp = Boolean.compare(this.upperBoundInclusive, other.upperBoundInclusive);
        }
      }
    }

    return comp;
  }

  /**
   * Determines if this row range contains the given row.
   *
   * @param row row to check
   * @return true if the row is contained in the row range, otherwise false
   */
  public boolean contains(Text row) {
    if (infiniteLowerBound) {
      return !isBefore(row);
    } else if (infiniteUpperBound) {
      return !isAfter(row);
    } else {
      return !isAfter(row) && !isBefore(row);
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
    // Sort row ranges by their lowerBound values
    sortedRowRanges.sort(ROW_RANGE_COMPARATOR);

    ArrayList<RowRange> mergedRowRanges = new ArrayList<>(rowRanges.size());

    // Initialize the current range for merging
    RowRange currentRange = sortedRowRanges.get(0);
    boolean currentLowerBoundInclusive = sortedRowRanges.get(0).lowerBoundInclusive;

    // Iterate through the sorted row ranges, merging overlapping and adjacent ranges
    for (int i = 1; i < sortedRowRanges.size(); i++) {
      if (currentRange.infiniteLowerBound && currentRange.infiniteUpperBound) {
        // The current range covers all possible rows, no further merging needed
        break;
      }

      RowRange nextRange = sortedRowRanges.get(i);

      boolean lowerBoundsEqual = (currentRange.lowerBound == null && nextRange.lowerBound == null)
          || (currentRange.lowerBound != null
              && currentRange.lowerBound.equals(nextRange.lowerBound));

      int comparison;
      if (lowerBoundsEqual || currentRange.infiniteUpperBound
          || (nextRange.lowerBound != null && (currentRange.upperBound == null
              || currentRange.upperBound.compareTo(nextRange.lowerBound) > 0
              || (currentRange.upperBound.equals(nextRange.lowerBound)
                  && (!currentRange.upperBoundInclusive || nextRange.lowerBoundInclusive))))) {
        if (nextRange.infiniteUpperBound) {
          comparison = 1;
        } else if (currentRange.upperBound == null) {
          comparison = -1;
        } else {
          comparison = nextRange.upperBound.compareTo(currentRange.upperBound);
        }
        if (comparison > 0 || (comparison == 0 && nextRange.upperBoundInclusive)) {
          currentRange = RowRange.range(currentRange.lowerBound, currentLowerBoundInclusive,
              nextRange.upperBound, nextRange.upperBoundInclusive);
        } // else current range contains the next range
      } else {
        // No overlap or adjacency
        // add the current range to the merged list and update the current range
        mergedRowRanges.add(currentRange);
        currentRange = nextRange;
        currentLowerBoundInclusive = nextRange.lowerBoundInclusive;
      }
    }

    // Add the final current range to the merged list
    mergedRowRanges.add(currentRange);

    return mergedRowRanges;
  }

  /**
   * Creates a row range which represents the intersection of this row range and the given row
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
   * Creates a row range which represents the intersection of this row range and the given row
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
    // Initialize lower bound and upper bound values with the current instance's values
    Text lowerBound = this.lowerBound;
    boolean lowerBoundInclusive = this.lowerBoundInclusive;
    Text upperBound = this.upperBound;
    boolean upperBoundInclusive = this.upperBoundInclusive;

    // If the input rowRange has a defined lowerBound, update lowerBound and lowerBoundInclusive if
    // needed
    if (rowRange.lowerBound != null) {
      // If the input rowRange's lowerBound is after this instance's upperBound or equal but not
      // inclusive, they do not overlap
      if (isBefore(rowRange.lowerBound) || (rowRange.lowerBound.equals(this.upperBound)
          && !(rowRange.lowerBoundInclusive && this.upperBoundInclusive))) {
        if (returnNullIfDisjoint) {
          return null;
        }
        throw new IllegalArgumentException("RowRange " + rowRange + " does not overlap " + this);
      } else if (!isAfter(rowRange.lowerBound)) {
        // If the input rowRange's lowerBound is within this instance's range, use it as the new
        // lowerBound
        lowerBound = rowRange.lowerBound;
        lowerBoundInclusive = rowRange.lowerBoundInclusive;
      }
    }

    // If the input rowRange has a defined upperBound, update upperBound and upperBoundInclusive if
    // needed
    if (rowRange.upperBound != null) {
      // If the input rowRange's upperBound is before this instance's lowerBound or equal but not
      // inclusive, they do not overlap
      if (isAfter(rowRange.upperBound) || (rowRange.upperBound.equals(this.lowerBound)
          && !(rowRange.upperBoundInclusive && this.lowerBoundInclusive))) {
        if (returnNullIfDisjoint) {
          return null;
        }
        throw new IllegalArgumentException("RowRange " + rowRange + " does not overlap " + this);
      } else if (!isBefore(rowRange.upperBound)) {
        // If the input rowRange's upperBound is within this instance's range, use it as the new
        // upperBound
        upperBound = rowRange.upperBound;
        upperBoundInclusive = rowRange.upperBoundInclusive;
      }
    }

    // Return a new RowRange instance representing the intersection of the two ranges
    return RowRange.range(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);
  }

}

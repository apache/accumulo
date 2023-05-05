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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RowRangeTest {

  @Nested
  class TestBasic {

    @Test
    void testGetters() {
      Text startRow = new Text("r1");
      Text endRow = new Text("r1");
      boolean isStartInclusive = true;
      boolean isEndInclusive = false;

      RowRange range = RowRange.range(startRow, isStartInclusive, endRow, isEndInclusive);

      assertEquals(startRow, range.getStartRow());
      assertEquals(isStartInclusive, range.isStartRowInclusive());
      assertEquals(endRow, range.getEndRow());
      assertEquals(isEndInclusive, range.isEndRowInclusive());

      startRow = new Text("r11");
      endRow = new Text("r22");
      isStartInclusive = false;
      isEndInclusive = true;

      range = RowRange.range(startRow, isStartInclusive, endRow, isEndInclusive);

      assertEquals(startRow, range.getStartRow());
      assertEquals(isStartInclusive, range.isStartRowInclusive());
      assertEquals(endRow, range.getEndRow());
      assertEquals(isEndInclusive, range.isEndRowInclusive());
    }

    @Test
    void testEndRowBeforeStartRow() {
      final Text startRow = new Text("r1");
      final Text endRow = new Text("r0");

      assertTrue(startRow.compareTo(endRow) > 0);

      assertThrows(IllegalArgumentException.class,
          () -> RowRange.range(startRow, true, endRow, false));
    }

    @Test
    void testBeforeStartRowWithInfiniteStartRow() {
      RowRange lessThanR1 = RowRange.lessThan("r1");

      assertFalse(lessThanR1.beforeStartRow(new Text("r0")));
      assertFalse(lessThanR1.beforeStartRow(new Text("r1")));
      assertFalse(lessThanR1.beforeStartRow(new Text("r2")));

      RowRange rowRangeR1 = RowRange.closed("r1");

      assertTrue(rowRangeR1.beforeStartRow(new Text("r")));
      assertTrue(rowRangeR1.beforeStartRow(new Text("r0")));
      assertFalse(rowRangeR1.beforeStartRow(new Text("r1")));
      assertFalse(rowRangeR1.beforeStartRow(new Text("r2")));
    }

    @Test
    public void testAfterEndRowWithEndRowInclusive() {
      RowRange range = RowRange.closed(new Text("a"), new Text("c"));
      assertFalse(range.afterEndRow(new Text("c")));
      assertFalse(range.afterEndRow(new Text("b")));
      assertFalse(range.afterEndRow(new Text("a")));
      assertTrue(range.afterEndRow(new Text("d")));
    }

    @Test
    public void testAfterEndRowWithEndRowExclusive() {
      RowRange range = RowRange.open(new Text("a"), new Text("c"));
      assertTrue(range.afterEndRow(new Text("c")));
      assertFalse(range.afterEndRow(new Text("b")));
      assertFalse(range.afterEndRow(new Text("a")));
    }

    @Test
    public void testAfterEndRowWithInfiniteEndRow() {
      RowRange range = RowRange.greaterThan(new Text("a"));
      assertFalse(range.afterEndRow(new Text("a")));
      assertFalse(range.afterEndRow(new Text("b")));
    }

    @Test
    public void testToRange() {
      // Closed range
      RowRange closedRange = RowRange.closed(new Text("a"), new Text("c"));
      Range expectedClosedRange = new Range(new Text("a"), true, new Text("c"), true);
      assertEquals(expectedClosedRange, closedRange.toRange());

      // Open range
      RowRange openRange = RowRange.open(new Text("a"), new Text("c"));
      Range expectedOpenRange = new Range(new Text("a"), false, new Text("c"), false);
      assertEquals(expectedOpenRange, openRange.toRange());

      // Range with infinite start
      RowRange infiniteStartRange = RowRange.greaterThan(new Text("a"));
      Range expectedInfiniteStartRange = new Range(new Text("a"), false, null, true);
      assertEquals(expectedInfiniteStartRange, infiniteStartRange.toRange());

      // Range with infinite end
      RowRange infiniteEndRange = RowRange.lessThan(new Text("c"));
      Range expectedInfiniteEndRange = new Range(null, true, new Text("c"), false);
      assertEquals(expectedInfiniteEndRange, infiniteEndRange.toRange());

      // All rows range
      RowRange allRange = RowRange.all();
      Range expectedAllRange = new Range();
      assertEquals(expectedAllRange, allRange.toRange());
    }

    @Test
    public void testStaticMethodsThrowExceptionOnNullArgument() {
      Stream<Runnable> methods = Stream.of(() -> RowRange.open(null, ""),
          () -> RowRange.open("", null), () -> RowRange.closed(null, ""),
          () -> RowRange.closed("", null), () -> RowRange.closed((Text) null),
          () -> RowRange.openClosed(null, ""), () -> RowRange.openClosed("", null),
          () -> RowRange.closedOpen(null, ""), () -> RowRange.closedOpen("", null),
          () -> RowRange.greaterThan((Text) null), () -> RowRange.atLeast((Text) null),
          () -> RowRange.lessThan((Text) null), () -> RowRange.atMost((Text) null));

      methods.forEach(method -> assertThrows(NullPointerException.class, method::run));
    }

  }

  @Nested
  class StaticEntryPointTests {

    @Test
    void testOpenEquality() {
      RowRange range1 = RowRange.open("r1", "row5");
      RowRange range2 = RowRange.open(new Text("r1"), new Text("row5"));
      RowRange range3 = RowRange.range(new Text("r1"), false, new Text("row5"), false);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testClosedEquality() {
      RowRange range1 = RowRange.closed("r1", "row5");
      RowRange range2 = RowRange.closed(new Text("r1"), new Text("row5"));
      RowRange range3 = RowRange.range(new Text("r1"), true, new Text("row5"), true);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testClosedSingleRowEquality() {
      RowRange range1 = RowRange.closed("r1");
      RowRange range2 = RowRange.closed(new Text("r1"));
      RowRange range3 = RowRange.range(new Text("r1"), true, new Text("r1"), true);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testClosedOpenEquality() {
      RowRange range1 = RowRange.closedOpen("r1", "row5");
      RowRange range2 = RowRange.closedOpen(new Text("r1"), new Text("row5"));
      RowRange range3 = RowRange.range(new Text("r1"), true, new Text("row5"), false);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testOpenClosedEquality() {
      RowRange range1 = RowRange.openClosed("r1", "row5");
      RowRange range2 = RowRange.openClosed(new Text("r1"), new Text("row5"));
      RowRange range3 = RowRange.range(new Text("r1"), false, new Text("row5"), true);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testGreaterThanEquality() {
      RowRange range1 = RowRange.greaterThan("r1");
      RowRange range2 = RowRange.greaterThan(new Text("r1"));
      RowRange range3 = RowRange.range(new Text("r1"), false, null, false);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testAtLeastEquality() {
      RowRange range1 = RowRange.atLeast("r1");
      RowRange range2 = RowRange.atLeast(new Text("r1"));
      RowRange range3 = RowRange.range(new Text("r1"), true, null, false);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testLessThanEquality() {
      RowRange range1 = RowRange.lessThan("row5");
      RowRange range2 = RowRange.lessThan(new Text("row5"));
      RowRange range3 = RowRange.range(null, false, new Text("row5"), false);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testAtMostEquality() {
      RowRange range1 = RowRange.atMost("row5");
      RowRange range2 = RowRange.atMost(new Text("row5"));
      RowRange range3 = RowRange.range(null, false, new Text("row5"), true);

      assertTrue(range1.equals(range2));
      assertTrue(range1.equals(range3));
      assertTrue(range2.equals(range3));
    }

    @Test
    void testAllEquality() {
      RowRange range1 = RowRange.all();
      RowRange range2 = RowRange.range((Text) null, false, null, false);

      assertTrue(range1.equals(range2));
    }
  }

  @Nested
  class EqualsTests {

    @Test
    void testEqualsWithObject() {
      Object rowRange = RowRange.closedOpen("r1", "row5");
      Object rowRange1 = RowRange.closedOpen("r1", "row5");
      RowRange rowRange2 = RowRange.closedOpen("r1", "row5");

      assertEquals(rowRange, rowRange1);
      assertEquals(rowRange, rowRange2);

      String badRange = "r1";
      assertNotEquals(rowRange, badRange);
    }

    @Test
    void testEqualsWithDifferentRanges() {
      RowRange range1 = RowRange.closedOpen("r1", "row5");
      RowRange range2 = RowRange.closedOpen("r2", "row4");
      assertFalse(range1.equals(range2));
    }

    @Test
    void testEqualsWithSameRange() {
      RowRange range1 = RowRange.closedOpen("r1", "row5");
      RowRange range2 = RowRange.closedOpen("r1", "row5");
      assertTrue(range1.equals(range2));
    }

    @Test
    void testEqualsWithDifferentStartRowInclusiveness() {
      RowRange range1 = RowRange.closedOpen("r1", "row5");
      RowRange range2 = RowRange.openClosed("r1", "row5");
      assertFalse(range1.equals(range2));
    }

    @Test
    void testEqualsWithDifferentEndRowInclusiveness() {
      RowRange range1 = RowRange.closedOpen("r1", "row5");
      RowRange range2 = RowRange.closedOpen("r1", "row4");
      assertFalse(range1.equals(range2));
    }

    @Test
    void testEqualsWithDifferentStartRowAndEndRowInclusiveness() {
      RowRange range1 = RowRange.closedOpen("r1", "row5");
      RowRange range2 = RowRange.openClosed("r1", "row4");
      assertFalse(range1.equals(range2));
    }

    @Test
    void testOverloadEquality() {
      RowRange range1 = RowRange.closedOpen("r1", "row5");
      RowRange range2 = RowRange.closedOpen(new Text("r1"), new Text("row5"));
      RowRange range3 = RowRange.atLeast("row8");
      RowRange range4 = RowRange.atLeast(new Text("row8"));
      RowRange range5 = RowRange.lessThan("r2");
      RowRange range6 = RowRange.lessThan(new Text("r2"));
      RowRange range7 = RowRange.atMost("r3");
      RowRange range8 = RowRange.atMost(new Text("r3"));

      // Test that all ranges created using different entry point methods are equal
      assertTrue(range1.equals(range2));
      assertTrue(range3.equals(range4));
      assertTrue(range5.equals(range6));
      assertTrue(range7.equals(range8));

      // Test that ranges with different properties are not equal
      assertFalse(range1.equals(range3));
      assertFalse(range1.equals(range5));
      assertFalse(range1.equals(range7));
      assertFalse(range3.equals(range5));
      assertFalse(range3.equals(range7));
      assertFalse(range5.equals(range7));
    }
  }

  @Nested
  class CompareToTests {

    @Test
    void testCompareWithSameRange() {
      RowRange range1 = RowRange.open("r1", "r3");
      RowRange range2 = RowRange.open("r1", "r3");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.closed("r1", "r3");
      range2 = RowRange.closed("r1", "r3");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.closedOpen("r1", "r3");
      range2 = RowRange.closedOpen("r1", "r3");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.closed("r1");
      range2 = RowRange.closed("r1");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.openClosed("r1", "r3");
      range2 = RowRange.openClosed("r1", "r3");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.greaterThan("r1");
      range2 = RowRange.greaterThan("r1");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.atLeast("r1");
      range2 = RowRange.atLeast("r1");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.lessThan("r1");
      range2 = RowRange.lessThan("r1");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.atMost("r1");
      range2 = RowRange.atMost("r1");

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));

      range1 = RowRange.all();
      range2 = RowRange.all();

      assertEquals(0, range1.compareTo(range2));
      assertEquals(0, range2.compareTo(range1));
    }

    @Test
    void testCompareWithDifferentStartRow() {
      RowRange open_r1_r3 = RowRange.open("r1", "r3");
      RowRange open_r2_r3 = RowRange.open("r2", "r3");

      assertTrue(open_r1_r3.compareTo(open_r2_r3) < 0);
      assertTrue(open_r2_r3.compareTo(open_r1_r3) > 0);
    }

    @Test
    void testCompareWithDifferentEndRow() {
      RowRange open_r1_r3 = RowRange.open("r1", "r3");
      RowRange open_r1_r4 = RowRange.open("r1", "r4");

      assertTrue(open_r1_r3.compareTo(open_r1_r4) < 0);
      assertTrue(open_r1_r4.compareTo(open_r1_r3) > 0);
    }

    @Test
    void testCompareWithDifferentStartRowInclusiveness() {
      RowRange open = RowRange.open("r1", "r3");
      RowRange closedOpen = RowRange.closedOpen("r1", "r3");

      assertTrue(open.compareTo(closedOpen) > 0);
      assertTrue(closedOpen.compareTo(open) < 0);
    }

    @Test
    void testCompareWithDifferentEndRowInclusiveness() {
      RowRange open = RowRange.open("r1", "r3");
      RowRange openClosed = RowRange.openClosed("r1", "r3");

      assertTrue(open.compareTo(openClosed) < 0);
      assertTrue(openClosed.compareTo(open) > 0);
    }

    @Test
    void testCompareWithInfiniteEndRow() {
      RowRange atLeast_r1 = RowRange.atLeast("r1");
      RowRange greaterThan_r1 = RowRange.greaterThan("r1");
      RowRange all = RowRange.all();

      assertTrue(atLeast_r1.compareTo(all) < 0);
      assertTrue(all.compareTo(atLeast_r1) > 0);

      assertTrue(greaterThan_r1.compareTo(all) < 0);
      assertTrue(all.compareTo(greaterThan_r1) > 0);

      assertTrue(atLeast_r1.compareTo(greaterThan_r1) < 0);
      assertTrue(greaterThan_r1.compareTo(atLeast_r1) > 0);
    }

    @Test
    void testCompareWithInfiniteStartRow() {
      RowRange atMost_r1 = RowRange.atMost("r1");
      RowRange lessThan_r1 = RowRange.lessThan("r1");
      RowRange all = RowRange.all();

      assertTrue(atMost_r1.compareTo(all) < 0);
      assertTrue(all.compareTo(atMost_r1) > 0);

      assertTrue(lessThan_r1.compareTo(all) < 0);
      assertTrue(all.compareTo(lessThan_r1) > 0);

      assertTrue(atMost_r1.compareTo(lessThan_r1) > 0);
      assertTrue(lessThan_r1.compareTo(atMost_r1) < 0);
    }
  }

  @Nested
  class ContainsTests {

    @Test
    void testContainsWithAllRange() {
      RowRange range = RowRange.all();
      assertTrue(range.contains(new Text(new byte[] {})));
      assertTrue(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertTrue(range.contains(new Text("row3")));
    }

    @Test
    void testContainsWithOpenRange() {
      RowRange range = RowRange.open("r1", "r3");
      assertFalse(range.contains(new Text(new byte[] {})));
      assertFalse(range.contains(new Text("r0")));
      assertFalse(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text(new byte[] {'r', '1', 0})));
      assertTrue(range.contains(new Text("r11")));
      assertTrue(range.contains(new Text("r2")));
      assertFalse(range.contains(new Text(new byte[] {'r', '3', 0})));
      assertFalse(range.contains(new Text("r3")));
    }

    @Test
    void testContainsWithClosedRange() {
      RowRange range = RowRange.closed("r1", "r3");
      assertFalse(range.contains(new Text("r0")));
      assertTrue(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertTrue(range.contains(new Text("r3")));
    }

    @Test
    void testContainsWithOpenClosedRange() {
      RowRange range = RowRange.openClosed("r1", "r3");
      assertFalse(range.contains(new Text("r0")));
      assertFalse(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertTrue(range.contains(new Text("r3")));
      assertFalse(range.contains(new Text("r30")));
    }

    @Test
    void testContainsWithClosedOpenRange() {
      RowRange range = RowRange.closedOpen("r1", "r3");
      assertFalse(range.contains(new Text("r0")));
      assertTrue(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertFalse(range.contains(new Text("r3")));
    }

    @Test
    void testContainsWithSingleRowRange() {
      RowRange range = RowRange.closed("r1");
      assertTrue(range.contains(new Text("r1")));
      assertFalse(range.contains(new Text("r2")));
    }

    @Test
    void testContainsWithAtLeastRange() {
      RowRange range = RowRange.atLeast("r1");
      assertTrue(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertFalse(range.contains(new Text("")));
    }

    @Test
    void testContainsWithAtMostRange() {
      RowRange range = RowRange.atMost("r1");
      assertTrue(range.contains(new Text(new byte[] {})));
      assertTrue(range.contains(new Text("r1")));
      assertFalse(range.contains(new Text("r10")));
      assertFalse(range.contains(new Text("r2")));
      assertTrue(range.contains(new Text("")));
    }
  }

  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  @Nested
  class MergeOverlappingTests {

    @ParameterizedTest
    @MethodSource({"rowRangeProvider", "rowRangeProvider1"})
    public void testMergeOverlapping(List<RowRange> rowRangesToMerge, List<RowRange> expected) {
      List<RowRange> actual = RowRange.mergeOverlapping(rowRangesToMerge);
      verifyMerge(expected, actual);
    }

    private void verifyMerge(List<RowRange> expectedList, List<RowRange> actualList) {
      HashSet<RowRange> expectedSet = new HashSet<>(expectedList);
      HashSet<RowRange> actualSet = new HashSet<>(actualList);
      assertEquals(expectedSet, actualSet, "Expected: " + expectedSet + " Actual: " + actualSet);
    }

    Stream<Arguments> rowRangeProvider() {
      return Stream.of(
          // [a,c] [a,b] -> [a,c]
          Arguments.of(List.of(RowRange.closed("a", "c"), RowRange.closed("a", "b")),
              List.of(RowRange.closed("a", "c"))),
          // [a,c] [d,f] -> [a,c] [d,f]
          Arguments.of(List.of(RowRange.closed("a", "c"), RowRange.closed("d", "f")),
              List.of(RowRange.closed("a", "c"), RowRange.closed("d", "f"))),
          // [a,e] [b,f] [c,r] [g,j] [t,x] -> [a,r] [t,x]
          Arguments.of(
              List.of(RowRange.closed("a", "e"), RowRange.closed("b", "f"),
                  RowRange.closed("c", "r"), RowRange.closed("g", "j"), RowRange.closed("t", "x")),
              List.of(RowRange.closed("a", "r"), RowRange.closed("t", "x"))),
          // [a,e] [b,f] [c,r] [g,j] -> [a,r]
          Arguments.of(
              List.of(RowRange.closed("a", "e"), RowRange.closed("b", "f"),
                  RowRange.closed("c", "r"), RowRange.closed("g", "j")),
              List.of(RowRange.closed("a", "r"))),
          // [a,e] -> [a,e]
          Arguments.of(List.of(RowRange.closed("a", "e")), List.of(RowRange.closed("a", "e"))),
          // [] -> []
          Arguments.of(List.of(), List.of()),
          // [a,e] [g,q] [r,z] -> [a,e] [g,q] [r,z]
          Arguments.of(
              List.of(RowRange.closed("a", "e"), RowRange.closed("g", "q"),
                  RowRange.closed("r", "z")),
              List.of(RowRange.closed("a", "e"), RowRange.closed("g", "q"),
                  RowRange.closed("r", "z"))),
          // [a,c] [a,c] -> [a,c]
          Arguments.of(List.of(RowRange.closed("a", "c"), RowRange.closed("a", "c")),
              List.of(RowRange.closed("a", "c"))),
          // [ALL] -> [ALL]
          Arguments.of(List.of(RowRange.all()), List.of(RowRange.all())),
          // [ALL] [a,c] -> [ALL]
          Arguments.of(List.of(RowRange.all(), RowRange.closed("a", "c")), List.of(RowRange.all())),
          // [a,c] [ALL] -> [ALL]
          Arguments.of(List.of(RowRange.closed("a", "c"), RowRange.all()), List.of(RowRange.all())),
          // [b,d] [c,+inf) -> [b,+inf)
          Arguments.of(List.of(RowRange.closed("b", "d"), RowRange.atLeast("c")),
              List.of(RowRange.atLeast("b"))),
          // [b,d] [a,+inf) -> [a,+inf)
          Arguments.of(List.of(RowRange.closed("b", "d"), RowRange.atLeast("a")),
              List.of(RowRange.atLeast("a"))),
          // [b,d] [e,+inf) -> [b,d] [e,+inf)
          Arguments.of(List.of(RowRange.closed("b", "d"), RowRange.atLeast("e")),
              List.of(RowRange.closed("b", "d"), RowRange.atLeast("e"))),
          // [b,d] [e,+inf) [c,f] -> [b,+inf)
          Arguments.of(
              List.of(RowRange.closed("b", "d"), RowRange.atLeast("e"), RowRange.closed("c", "f")),
              List.of(RowRange.atLeast("b"))),
          // [b,d] [f,+inf) [c,e] -> [b,e] [f,+inf)
          Arguments.of(
              List.of(RowRange.closed("b", "d"), RowRange.atLeast("f"), RowRange.closed("c", "e")),
              List.of(RowRange.closed("b", "e"), RowRange.atLeast("f"))),
          // [b,d] [r,+inf) [c,e] [g,t] -> [b,e] [g,+inf)
          Arguments.of(
              List.of(RowRange.closed("b", "d"), RowRange.atLeast("r"), RowRange.closed("c", "e"),
                  RowRange.closed("g", "t")),
              List.of(RowRange.closed("b", "e"), RowRange.atLeast("g"))),
          // (-inf,d] [r,+inf) [c,e] [g,t] -> (-inf,e] [g,+inf)
          Arguments.of(List.of(RowRange.atMost("d"), RowRange.atLeast("r"),
              RowRange.closed("c", "e"), RowRange.closed("g", "t")),
              List.of(RowRange.atMost("e"), RowRange.atLeast("g"))),
          // (-inf,d] [r,+inf) [c,e] [g,t] [d,h] -> (-inf,+inf)
          Arguments.of(List.of(RowRange.atMost("d"), RowRange.atLeast("r"),
              RowRange.closed("c", "e"), RowRange.closed("g", "t"), RowRange.closed("d", "h")),
              List.of(RowRange.all())),
          // [a,b) (b,c) -> [a,c)
          Arguments.of(List.of(RowRange.closedOpen("a", "b"), RowRange.open("b", "c")),
              List.of(RowRange.closedOpen("a", "c"))),
          // [a,b) [b,c) -> [a,c)
          Arguments.of(List.of(RowRange.closedOpen("a", "b"), RowRange.closedOpen("b", "c")),
              List.of(RowRange.closedOpen("a", "c"))),
          // [a,b] (b,c) -> [a,b], (b,c)
          Arguments.of(List.of(RowRange.closed("a", "b"), RowRange.open("b", "c")),
              List.of(RowRange.closed("a", "b"), RowRange.open("b", "c"))),
          // [a,b] [b,c) -> [a,c)
          Arguments.of(List.of(RowRange.closed("a", "b"), RowRange.closedOpen("b", "c")),
              List.of(RowRange.closedOpen("a", "c"))),
          // (-inf,b] (-inf,c] -> (-inf,c]
          Arguments.of(List.of(RowRange.atMost("b"), RowRange.atMost("c")),
              List.of(RowRange.atMost("c"))),
          // (-inf,b] (-inf,c] [a,d] -> (-inf,d]
          Arguments.of(
              List.of(RowRange.atMost("b"), RowRange.atMost("c"), RowRange.closed("a", "d")),
              List.of(RowRange.atMost("d"))),
          // (-inf,b] (-inf,c] [a,d] [e,f] -> (-inf,d] [e,f]
          Arguments.of(
              List.of(RowRange.atMost("b"), RowRange.atMost("c"), RowRange.closed("a", "d"),
                  RowRange.closed("e", "f")),
              List.of(RowRange.atMost("d"), RowRange.closed("e", "f"))),
          // [a] [b] -> [a] [b]
          Arguments.of(List.of(RowRange.closed("a"), RowRange.closed("b")),
              List.of(RowRange.closed("a"), RowRange.closed("b"))),
          // (a,b] [b,c] -> (a,c]
          Arguments.of(List.of(RowRange.openClosed("a", "b"), RowRange.closed("b", "c")),
              List.of(RowRange.openClosed("a", "c"))),
          // (a,b] (b,+inf) -> (a,+inf)
          Arguments.of(List.of(RowRange.openClosed("a", "b"), RowRange.atLeast("b")),
              List.of(RowRange.greaterThan("a"))),
          // (-inf,b) (-inf,c) -> (-inf,c)
          Arguments.of(List.of(RowRange.lessThan("b"), RowRange.lessThan("c")),
              List.of(RowRange.lessThan("c"))));
    }

    Stream<Arguments> rowRangeProvider1() {
      Stream.Builder<Arguments> builder = Stream.builder();

      for (boolean b1 : new boolean[] {true, false}) {
        for (boolean b2 : new boolean[] {true, false}) {
          for (boolean b3 : new boolean[] {true, false}) {
            for (boolean b4 : new boolean[] {true, false}) {
              List<RowRange> rl =
                  List.of(RowRange.range("a", b1, "m", b2), RowRange.range("b", b3, "n", b4));
              List<RowRange> expected = List.of(RowRange.range("a", b1, "n", b4));
              builder.add(Arguments.of(rl, expected));

              rl = List.of(RowRange.range("a", b1, "m", b2), RowRange.range("a", b3, "n", b4));
              expected = List.of(RowRange.range("a", b1 || b3, "n", b4));
              builder.add(Arguments.of(rl, expected));

              rl = List.of(RowRange.range("a", b1, "n", b2), RowRange.range("b", b3, "n", b4));
              expected = List.of(RowRange.range("a", b1, "n", b2 || b4));
              builder.add(Arguments.of(rl, expected));

              rl = List.of(RowRange.range("a", b1, "n", b2), RowRange.range("a", b3, "n", b4));
              expected = List.of(RowRange.range("a", b1 || b3, "n", b2 || b4));
              builder.add(Arguments.of(rl, expected));
            }
          }
        }
      }

      return builder.build();
    }

  }

  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  @Nested
  class ClipTests {

    @ParameterizedTest
    @MethodSource("clipTestArguments")
    void testClip(RowRange fence, RowRange range, RowRange expected) {
      if (expected != null) {
        RowRange clipped = fence.clip(range);
        assertEquals(expected, clipped);
      } else {
        assertThrows(IllegalArgumentException.class, () -> fence.clip(range));
      }
    }

    private Stream<Arguments> clipTestArguments() {
      RowRange fenceOpen = RowRange.open("a", "c");
      RowRange fenceClosedOpen = RowRange.closedOpen("a", "c");
      RowRange fenceOpenClosed = RowRange.openClosed("a", "c");
      RowRange fenceClosed = RowRange.closed("a", "c");

      RowRange fenceOpenCN = RowRange.open("c", "n");
      RowRange fenceClosedCN = RowRange.closed("c", "n");
      RowRange fenceClosedB = RowRange.closed("b");

      return Stream.of(
          // (a,c) (a,c) -> (a,c)
          Arguments.of(fenceOpen, RowRange.open("a", "c"), RowRange.open("a", "c")),
          // (a,c) [a,c) -> (a,c)
          Arguments.of(fenceOpen, RowRange.closedOpen("a", "c"), RowRange.open("a", "c")),
          // (a,c) (a,c] -> (a,c)
          Arguments.of(fenceOpen, RowRange.openClosed("a", "c"), RowRange.open("a", "c")),
          // (a,c) [a,c] -> (a,c)
          Arguments.of(fenceOpen, RowRange.closed("a", "c"), RowRange.open("a", "c")),

          // [a,c) (a,c) -> (a,c)
          Arguments.of(fenceClosedOpen, RowRange.open("a", "c"), RowRange.open("a", "c")),
          // [a,c) [a,c) -> [a,c)
          Arguments.of(fenceClosedOpen, RowRange.closedOpen("a", "c"),
              RowRange.closedOpen("a", "c")),
          // [a,c) (a,c] -> (a,c)
          Arguments.of(fenceClosedOpen, RowRange.openClosed("a", "c"), RowRange.open("a", "c")),
          // [a,c) [a,c] -> [a,c)
          Arguments.of(fenceClosedOpen, RowRange.closed("a", "c"), RowRange.closedOpen("a", "c")),

          // (a,c] (a,c) -> (a,c)
          Arguments.of(fenceOpenClosed, RowRange.open("a", "c"), RowRange.open("a", "c")),
          // (a,c] [a,c) -> (a,c)
          Arguments.of(fenceOpenClosed, RowRange.closedOpen("a", "c"), RowRange.open("a", "c")),
          // (a,c] (a,c] -> (a,c]
          Arguments.of(fenceOpenClosed, RowRange.openClosed("a", "c"),
              RowRange.openClosed("a", "c")),
          // (a,c] [a,c] -> (a,c]
          Arguments.of(fenceOpenClosed, RowRange.closed("a", "c"), RowRange.openClosed("a", "c")),
          // (a,c] (-inf, c) -> (a,c)
          Arguments.of(fenceOpenClosed, RowRange.lessThan("c"), RowRange.open("a", "c")),
          // (a,c] (-inf, a) -> empty
          Arguments.of(fenceOpenClosed, RowRange.lessThan("a"), null),

          // [a,c] (a,c) -> (a,c)
          Arguments.of(fenceClosed, RowRange.open("a", "c"), RowRange.open("a", "c")),
          // [a,c] [a,c) -> [a,c)
          Arguments.of(fenceClosed, RowRange.closedOpen("a", "c"), RowRange.closedOpen("a", "c")),
          // [a,c] (a,c] -> (a,c]
          Arguments.of(fenceClosed, RowRange.openClosed("a", "c"), RowRange.openClosed("a", "c")),
          // [a,c] [a,c] -> [a,c]
          Arguments.of(fenceClosed, RowRange.closed("a", "c"), RowRange.closed("a", "c")),

          // (a,c) (-inf, +inf) -> (a,c)
          Arguments.of(fenceOpen, RowRange.all(), fenceOpen),
          // (a,c) [a, +inf) -> (a,c)
          Arguments.of(fenceOpen, RowRange.atLeast("a"), fenceOpen),
          // (a,c) (-inf, c] -> (a,c)
          Arguments.of(fenceOpen, RowRange.atMost("c"), fenceOpen),
          // (a,c) [a,c] -> (a,c)
          Arguments.of(fenceOpen, RowRange.closed("a", "c"), fenceOpen),

          // (a,c) (0,z) -> (a,c)
          Arguments.of(fenceOpen, RowRange.open("0", "z"), fenceOpen),
          // (a,c) [0,z) -> (a,c)
          Arguments.of(fenceOpen, RowRange.closedOpen("0", "z"), fenceOpen),
          // (a,c) (0,z] -> (a,c)
          Arguments.of(fenceOpen, RowRange.openClosed("0", "z"), fenceOpen),
          // (a,c) [0,z] -> (a,c)
          Arguments.of(fenceOpen, RowRange.closed("0", "z"), fenceOpen),

          // (a,c) (0,b) -> (a,b)
          Arguments.of(fenceOpen, RowRange.open("0", "b"), RowRange.open("a", "b")),
          // (a,c) [0,b) -> (a,b)
          Arguments.of(fenceOpen, RowRange.closedOpen("0", "b"), RowRange.open("a", "b")),
          // (a,c) (0,b] -> (a,b]
          Arguments.of(fenceOpen, RowRange.openClosed("0", "b"), RowRange.openClosed("a", "b")),
          // (a,c) [0,b] -> (a,b]
          Arguments.of(fenceOpen, RowRange.closed("0", "b"), RowRange.openClosed("a", "b")),

          // (a,c) (a1,z) -> (a1,c)
          Arguments.of(fenceOpen, RowRange.open("a1", "z"), RowRange.open("a1", "c")),
          // (a,c) [a1,z) -> [a1,c)
          Arguments.of(fenceOpen, RowRange.closedOpen("a1", "z"), RowRange.closedOpen("a1", "c")),
          // (a,c) (a1,z] -> (a1,c)
          Arguments.of(fenceOpen, RowRange.openClosed("a1", "z"), RowRange.open("a1", "c")),
          // (a,c) [a1,z] -> [a1,c)
          Arguments.of(fenceOpen, RowRange.closed("a1", "z"), RowRange.closedOpen("a1", "c")),

          // (a,c) (a1,b) -> (a1,b)
          Arguments.of(fenceOpen, RowRange.open("a1", "b"), RowRange.open("a1", "b")),
          // (a,c) [a1,b) -> [a1,b)
          Arguments.of(fenceOpen, RowRange.closedOpen("a1", "b"), RowRange.closedOpen("a1", "b")),
          // (a,c) (a1,b] -> (a1,b]
          Arguments.of(fenceOpen, RowRange.openClosed("a1", "b"), RowRange.openClosed("a1", "b")),
          // (a,c) [a1,b] -> [a1,b]
          Arguments.of(fenceOpen, RowRange.closed("a1", "b"), RowRange.closed("a1", "b")),
          // (a,c) (a,+inf) -> (a,c)
          Arguments.of(fenceOpen, RowRange.greaterThan("a"), RowRange.open("a", "c")),
          // (a,c) (1,+inf) -> (a,c)
          Arguments.of(fenceOpen, RowRange.greaterThan("1"), RowRange.open("a", "c")),

          // (c,n) (a,c) -> empty
          Arguments.of(fenceOpenCN, RowRange.open("a", "c"), null),
          // (c,n) (a,c] -> empty
          Arguments.of(fenceOpenCN, RowRange.closedOpen("a", "c"), null),
          // (c,n) (n,r) -> empty
          Arguments.of(fenceOpenCN, RowRange.open("n", "r"), null),
          // (c,n) [n,r) -> empty
          Arguments.of(fenceOpenCN, RowRange.closedOpen("n", "r"), null),
          // (c,n) (a,b) -> empty
          Arguments.of(fenceOpenCN, RowRange.open("a", "b"), null),
          // (c,n) (a,b] -> empty
          Arguments.of(fenceOpenCN, RowRange.closedOpen("a", "b"), null),

          // [c,n] (a,c) -> empty
          Arguments.of(fenceClosedCN, RowRange.open("a", "c"), null),
          // [c,n] (a,c] -> (c,c)
          Arguments.of(fenceClosedCN, RowRange.openClosed("a", "c"), RowRange.closed("c")),
          // [c,n] (n,r) -> (n,n)
          Arguments.of(fenceClosedCN, RowRange.open("n", "r"), null),
          // [c,n] [n,r) -> (n,n)
          Arguments.of(fenceClosedCN, RowRange.closedOpen("n", "r"), RowRange.closed("n")),
          // [c,n] (q,r) -> empty
          Arguments.of(fenceClosedCN, RowRange.open("q", "r"), null),
          // [c,n] [q,r) -> empty
          Arguments.of(fenceClosedCN, RowRange.closedOpen("q", "r"), null),

          // [b] (b,c) -> empty
          Arguments.of(fenceClosedB, RowRange.open("b", "c"), null),
          // [b] [b,c) -> [b]
          Arguments.of(fenceClosedB, RowRange.closedOpen("b", "c"), RowRange.closed("b")),
          // [b] (a,b) -> empty
          Arguments.of(fenceClosedB, RowRange.open("a", "b"), null),
          // [b] (a,b] -> [b]
          Arguments.of(fenceClosedB, RowRange.openClosed("a", "b"), RowRange.closed("b")));
    }

    @Test
    public void clipReturnsNull() {
      RowRange range = RowRange.open("a", "b");
      RowRange afterRange = RowRange.closed("c");
      RowRange beforeRange = RowRange.closed("a");

      RowRange clipped = range.clip(afterRange, true);
      assertNull(clipped);

      clipped = range.clip(beforeRange, true);
      assertNull(clipped);
    }
  }

}

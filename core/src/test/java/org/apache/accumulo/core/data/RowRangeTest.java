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
  class StaticEntryPointTests {

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
    }

    @Test
    void testCompareWithDifferentStartRow() {
      RowRange range1 = RowRange.open("r1", "r3");
      RowRange range2 = RowRange.open("r2", "r3");
      assertTrue(range1.compareTo(range2) < 0);
    }

    @Test
    void testCompareWithDifferentEndRow() {
      RowRange range1 = RowRange.open("r1", "r3");
      RowRange range2 = RowRange.open("r1", "r4");
      assertTrue(range1.compareTo(range2) < 0);
    }

    @Test
    void testCompareWithDifferentStartRowInclusiveness() {
      RowRange range1 = RowRange.open("r1", "r3");
      RowRange range2 = RowRange.closedOpen("r1", "r3");
      assertTrue(range1.compareTo(range2) > 0);
    }

    @Test
    void testCompareWithDifferentEndRowInclusiveness() {
      RowRange range1 = RowRange.open("r1", "r3");
      RowRange range2 = RowRange.openClosed("r1", "r3");
      assertTrue(range1.compareTo(range2) < 0);
    }

    @Test
    void testCompareWithInfiniteStartRow() {
      RowRange range1 = RowRange.atLeast("r1");
      RowRange range2 = RowRange.all();
      assertTrue(range1.compareTo(range2) < 0);
    }

    @Test
    void testCompareWithInfiniteEndRow() {
      RowRange range1 = RowRange.all();
      RowRange range2 = RowRange.atLeast("r1");
      assertTrue(range1.compareTo(range2) > 0);
    }
  }

  @Nested
  class ContainsTests {

    @Test
    void testContainsWithAllRange() {
      RowRange range = RowRange.all();
      assertTrue(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertTrue(range.contains(new Text("row3")));
    }

    @Test
    void testContainsWithOpenRange() {
      RowRange range = RowRange.open("r1", "r3");
      assertFalse(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertFalse(range.contains(new Text("r3")));
    }

    @Test
    void testContainsWithClosedRange() {
      RowRange range = RowRange.closed("r1", "r3");
      assertTrue(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertTrue(range.contains(new Text("r3")));
    }

    @Test
    void testContainsWithOpenClosedRange() {
      RowRange range = RowRange.openClosed("r1", "r3");
      assertFalse(range.contains(new Text("r1")));
      assertTrue(range.contains(new Text("r2")));
      assertTrue(range.contains(new Text("r3")));
    }

    @Test
    void testContainsWithClosedOpenRange() {
      RowRange range = RowRange.closedOpen("r1", "r3");
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
      assertTrue(range.contains(new Text("r1")));
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
              List.of(RowRange.closedOpen("a", "c"))));
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

}

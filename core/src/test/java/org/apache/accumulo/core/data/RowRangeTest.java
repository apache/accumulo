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

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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

}

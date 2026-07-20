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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.RowRange;
import org.junit.jupiter.api.Test;

public class RowRangeUtilTest {
  @Test
  public void testConversions() {
    var ranges = List.of(new Range("a", false, "d", false), new Range("a", false, "d", true),
        new Range("a", true, "d", false), new Range("a", true, "d", true), new Range(),
        new Range("a", true, null, true), new Range("a", false, null, true),
        new Range(null, true, "d", true), new Range(null, true, "d", false));

    for (var range : ranges) {
      var rowRange = RowRangeUtil.toRowRange(range);
      for (var row : List.of("12", "a", "aa", "c", "ca", "d", "da", "z", "AA", "ZZ")) {
        // ensure the row range and key range are in agreement
        assertEquals(range.contains(new Key(row)), rowRange.contains(row));
      }
    }

    // Ensure the transformations done by the range constructor are properly reversed.
    assertEquals(RowRange.all(), RowRangeUtil.toRowRange(new Range()));
    assertEquals(RowRange.atLeast("a"), RowRangeUtil.toRowRange(new Range("a", true, null, true)));
    assertEquals(RowRange.greaterThan("a"),
        RowRangeUtil.toRowRange(new Range("a", false, null, true)));
    assertEquals(RowRange.atMost("a"), RowRangeUtil.toRowRange(new Range(null, true, "a", true)));
    assertEquals(RowRange.lessThan("a"),
        RowRangeUtil.toRowRange(new Range(null, true, "a", false)));
    assertEquals(RowRange.closed("a"), RowRangeUtil.toRowRange(new Range("a", true, "a", true)));
    assertEquals(RowRange.closed("a", "d"),
        RowRangeUtil.toRowRange(new Range("a", true, "d", true)));
    assertEquals(RowRange.open("a", "d"),
        RowRangeUtil.toRowRange(new Range("a", false, "d", false)));
    assertEquals(RowRange.openClosed("a", "d"),
        RowRangeUtil.toRowRange(new Range("a", false, "d", true)));
    assertEquals(RowRange.closedOpen("a", "d"),
        RowRangeUtil.toRowRange(new Range("a", true, "d", false)));

    // ensure non row ranges fail
    assertThrows(IllegalArgumentException.class,
        () -> RowRangeUtil.toRowRange(new Range(new Key("r", "f"), false, new Key("x"), false)));
    assertThrows(IllegalArgumentException.class,
        () -> RowRangeUtil.toRowRange(new Range(new Key("r", "f"), false, new Key("x"), true)));
    assertThrows(IllegalArgumentException.class,
        () -> RowRangeUtil.toRowRange(new Range(new Key("r", "f"), true, new Key("x"), false)));
    assertThrows(IllegalArgumentException.class,
        () -> RowRangeUtil.toRowRange(new Range(new Key("r", "f"), true, new Key("x"), true)));

    assertThrows(IllegalArgumentException.class,
        () -> RowRangeUtil.toRowRange(new Range(new Key("r"), false, new Key("x", "f"), false)));
    assertThrows(IllegalArgumentException.class,
        () -> RowRangeUtil.toRowRange(new Range(new Key("r"), false, new Key("x", "f"), true)));
    assertThrows(IllegalArgumentException.class,
        () -> RowRangeUtil.toRowRange(new Range(new Key("r"), true, new Key("x", "f"), false)));
    assertThrows(IllegalArgumentException.class,
        () -> RowRangeUtil.toRowRange(new Range(new Key("r"), true, new Key("x", "f"), true)));
  }
}

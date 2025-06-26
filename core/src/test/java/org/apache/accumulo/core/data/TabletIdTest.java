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
import static org.junit.jupiter.api.Assertions.assertNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class TabletIdTest {
  /**
   * Tests the {@link TabletId#of(TableId, String, String)} method to verify that a {@link TabletId}
   * object is created
   */
  @Test
  void testOfGivenStringArgs() {
    TabletId tabletId = TabletId.of(TableId.of("2"), "b", "a");
    assertEquals("2", tabletId.getTable().canonical(), "Expected the Table ID to be 2");
    assertEquals(new Text("b"), tabletId.getEndRow(), "Expected endRow to be b");
    assertEquals(new Text("a"), tabletId.getPrevEndRow(), "Expected prevEndRow to be a");
  }

  /**
   * Tests the {@link TabletId#of(TableId, Text, Text)} method to verify that a {@link TabletId}
   * object is created.
   */
  @Test
  void testOfGivenTextArgs() {
    TabletId tabletId = TabletId.of(TableId.of("2"), new Text("b"), new Text("a"));
    assertEquals("2", tabletId.getTable().canonical(), "Expected the Table ID to be 2");
    assertEquals(new Text("b"), tabletId.getEndRow(), "Expected endRow to be b");
    assertEquals(new Text("a"), tabletId.getPrevEndRow(), "Expected prevEndRow to be a");
  }

  /**
   * Tests the {@link TabletId#of(TableId, byte[], byte[])} method to verify that a {@link TabletId}
   * object is created.
   */
  @Test
  void testOfGivenByteArrayArgs() {
    TabletId tabletId = TabletId.of(TableId.of("2"), ("b").getBytes(UTF_8), ("a").getBytes(UTF_8));
    assertEquals("2", tabletId.getTable().canonical(), "Expected the Table ID to be 2");
    assertEquals(new Text("b"), tabletId.getEndRow(), "Expected endRow to be b");
    assertEquals(new Text("a"), tabletId.getPrevEndRow(), "Expected prevEndRow to be a");
  }

  /**
   * Test when null is given for the endRow/prevEndRow, that it is treated as an infinite range, and
   * any other TabletId objects with the same TableId are within range, no matter the values for
   * endRow and prevEndRow. The values of endRow and prevEndRow must be null or cast to a type to
   * avoid ambiguous references.
   */
  @Test
  void testInfiniteRanges() {
    Text endRow = null;
    Text prevEndRow = null;
    TabletId tabletId = TabletId.of(TableId.of("2"), endRow, prevEndRow);
    TabletId tabletId1 = TabletId.of(TableId.of("2"), "b", "a");
    TabletId tabletId2 = TabletId.of(TableId.of("2"), "z", "a");
    TabletId tabletId3 = TabletId.of(TableId.of("2"), new Text("z"), null);

    assertTrue((((TabletIdImpl) tabletId).toKeyExtent())
        .overlaps(((TabletIdImpl) tabletId1).toKeyExtent()));
    assertTrue((((TabletIdImpl) tabletId).toKeyExtent())
        .overlaps(((TabletIdImpl) tabletId2).toKeyExtent()));
    assertTrue((((TabletIdImpl) tabletId).toKeyExtent())
        .overlaps(((TabletIdImpl) tabletId3).toKeyExtent()));
  }

  /**
   * Tests that given a endRow equal to null, that {@link TabletId#getEndRow()} is equal to null.
   */
  @Test
  void testEndRowIsNull() {
    Text endRow = null;
    TabletId tabletId = TabletId.of(TableId.of("2"), endRow, new Text("a"));

    assertNull(tabletId.getEndRow(), "Expected endRow to be null");
    // Checking other fields just in case a bug with null sets other fields to null
    assertEquals("2", tabletId.getTable().canonical(), "Expected the Table ID to be 2");
    assertEquals(new Text("a"), tabletId.getPrevEndRow(), "Expected prevEndRow to be a");
  }

  /**
   * Tests that given a prevEndRow equal to null, that {@link TabletId#getPrevEndRow()} is equal to
   * null.
   */
  @Test
  void testPrevEndRowIsNull() {
    Text prevEndRow = null;
    TabletId tabletId = TabletId.of(TableId.of("2"), new Text("b"), prevEndRow);

    assertNull(tabletId.getPrevEndRow(), "Expected prevEndRow to be null");
    // Checking other fields just in case
    assertEquals("2", tabletId.getTable().canonical(), "Expected the Table ID to be 2");
    assertEquals(new Text("b"), tabletId.getEndRow(), "Expected endRow to be b");
  }

  /**
   * Tests that the inputted TableId of the TabletId is equal to its {@link TabletId#getTable()},
   * verifying the {@link TabletId} is correctly created.
   */
  @Test
  void testTableIdEqualsTableId() {
    TabletId tabletOne = TabletId.of(TableId.of("2"), "b", "a");
    TabletId tabletTwo = TabletId.of(TableId.of("Hello"), "b", "a");
    TabletId tabletThree = TabletId.of(TableId.of("---String---"), "b", "a");

    assertEquals("2", tabletOne.getTable().canonical(), "Expected the Table ID to be 2");
    assertEquals("Hello", tabletTwo.getTable().canonical(), "Expected the Table ID to be Hello");
    assertEquals("---String---", tabletThree.getTable().canonical(),
        "Expected the Table ID to be ---String---");
  }

  @Test
  void testCompareToOfTabletId(){
    /*  One way to do this is to create TabletIds w/ different table ids, different rows (null and not null),
    put them all in a list and sort the list and check the list is in the correct order */
    // TableId -> endRow -> prevEndRow
    ArrayList<TabletId> tablets = new ArrayList<>();
    tablets.add(TabletId.of(TableId.of("d"), "w", "a"));
    tablets.add(TabletId.of(TableId.of("e"), "z", "g"));
    tablets.add(TabletId.of(TableId.of("g"), "b", "a"));
    tablets.add(TabletId.of(TableId.of("c"), (String) null, null));
    tablets.add(TabletId.of(TableId.of("a"), "m", "l"));
    tablets.add(TabletId.of(TableId.of("a"), null, "b"));
    tablets.add(TabletId.of(TableId.of("a"), null, (String) null));
    tablets.add(TabletId.of(TableId.of("a"), null, "c"));
    tablets.add(TabletId.of(TableId.of("a"), "b", "a"));
    tablets.add(TabletId.of(TableId.of("a"), "z", null));
    tablets.add(TabletId.of(TableId.of("f"), null, "a"));
    tablets.add(TabletId.of(TableId.of("b"), "d", null));
    Collections.sort(tablets);

    for (int curr = 1; curr < tablets.size() - 1; curr++) {
      int prev = curr - 1;
      int comparison = tablets.get(prev).compareTo(tablets.get(curr));

      if (comparison == 0) {
        assertTrue(tablets.get(prev).equals(tablets.get(curr)));
      } else {
        assertTrue(comparison < 0);
      }
    }

  }

  @Test
  void testEqualsOfTabletId(){ // Comparing equals to t
    TabletId t = TabletId.of(TableId.of("2"), "b", "a");
    TabletId equalsT = TabletId.of(TableId.of("2"), "b", "a");
    ArrayList<TabletId> tablets = new ArrayList<>();

    tablets.add(TabletId.of(TableId.of("5"), "b", "a"));
    tablets.add(TabletId.of(TableId.of("5"), "b", "a"));
    tablets.add(TabletId.of(TableId.of("6"), "d", "a" ));
    tablets.add(TabletId.of(TableId.of("1"), "g", "e"));
    tablets.add(TabletId.of(TableId.of("2"), null, "a"));
    tablets.add(TabletId.of(TableId.of("2"), "b", null));
    tablets.add(TabletId.of(TableId.of("2"), (String) null, null));


      for (TabletId tablet : tablets) {
        assertFalse(t.equals(tablet));
      }

    assertTrue(t.equals(equalsT));

  }

  @Test
  void testHashCodeOfTabletId() {
    // Testing consistency
    TabletId tabletId = TabletId.of(TableId.of("2"), "b", "a");
    int hashCode1 = tabletId.hashCode();
    int hashCode2 = tabletId.hashCode();
    assertEquals(hashCode1, hashCode2);

    //Testing equality
    TabletId tabletOne = TabletId.of(TableId.of("2"), "b", "a");
    TabletId tabletTwo = TabletId.of(TableId.of("2"), "b", "a");
    assertEquals(tabletOne.hashCode(), tabletTwo.hashCode());

    //Testing even distribution
    List<TabletId> tablets = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      tablets.add(TabletId.of(TableId.of("value" + i), "b", "a"));
    }
    Set<Integer> hashCodes = new HashSet<>();
    for (TabletId tabs : tablets) {
      hashCodes.add(tabs.hashCode());
    }
    assertEquals(tablets.size(), hashCodes.size(), 10);
  }

}

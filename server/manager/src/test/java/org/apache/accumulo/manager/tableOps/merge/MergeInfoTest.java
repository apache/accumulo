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
package org.apache.accumulo.manager.tableOps.merge;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MergeInfoTest {

  private KeyExtent keyExtent;
  private MergeInfo mi;

  @BeforeEach
  public void setUp() {
    keyExtent = createMock(KeyExtent.class);
  }

  @Test
  public void testNeedsToBeChopped_DifferentTables() {
    expect(keyExtent.tableId()).andReturn(TableId.of("table1"));
    replay(keyExtent);
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent2.tableId()).andReturn(TableId.of("table2"));
    replay(keyExtent2);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.MERGE);
    assertFalse(mi.needsToBeChopped(keyExtent2));
  }

  @Test
  public void testNeedsToBeChopped_Delete_NotFollowing() {
    testNeedsToBeChopped_Delete("somerow", false);
  }

  @Test
  public void testNeedsToBeChopped_Delete_Following() {
    testNeedsToBeChopped_Delete("prev", true);
  }

  @Test
  public void testNeedsToBeChopped_Delete_NoPrevEndRow() {
    testNeedsToBeChopped_Delete(null, false);
  }

  private void testNeedsToBeChopped_Delete(String prevEndRow, boolean expected) {
    expect(keyExtent.tableId()).andReturn(TableId.of("table1"));
    expect(keyExtent.endRow()).andReturn(new Text("prev"));
    replay(keyExtent);
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent2.tableId()).andReturn(TableId.of("table1"));
    expect(keyExtent2.prevEndRow()).andReturn(prevEndRow != null ? new Text(prevEndRow) : null);
    expectLastCall().anyTimes();
    replay(keyExtent2);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.DELETE);
    assertEquals(expected, mi.needsToBeChopped(keyExtent2));
  }

  @Test
  public void testOverlaps_ExtentsOverlap() {
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent.overlaps(keyExtent2)).andReturn(true);
    replay(keyExtent);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.MERGE);
    assertTrue(mi.overlaps(keyExtent2));
  }

  @Test
  public void testOverlaps_DoesNotNeedChopping() {
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent.overlaps(keyExtent2)).andReturn(false);
    expect(keyExtent.tableId()).andReturn(TableId.of("table1"));
    replay(keyExtent);
    expect(keyExtent2.tableId()).andReturn(TableId.of("table2"));
    replay(keyExtent2);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.MERGE);
    assertFalse(mi.overlaps(keyExtent2));
  }

  @Test
  public void testOverlaps_NeedsChopping() {
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent.overlaps(keyExtent2)).andReturn(false);
    expect(keyExtent.tableId()).andReturn(TableId.of("table1"));
    expect(keyExtent.endRow()).andReturn(new Text("prev"));
    replay(keyExtent);
    expect(keyExtent2.tableId()).andReturn(TableId.of("table1"));
    expect(keyExtent2.prevEndRow()).andReturn(new Text("prev"));
    expectLastCall().anyTimes();
    replay(keyExtent2);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.DELETE);
    assertTrue(mi.overlaps(keyExtent2));
  }

  private static KeyExtent ke(String tableId, String endRow, String prevEndRow) {
    return new KeyExtent(TableId.of(tableId), endRow == null ? null : new Text(endRow),
        prevEndRow == null ? null : new Text(prevEndRow));
  }

  @Test
  public void testNeedsToBeChopped() {
    MergeInfo info = new MergeInfo(ke("x", "b", "a"), MergeInfo.Operation.DELETE);
    assertTrue(info.needsToBeChopped(ke("x", "c", "b")));
    assertTrue(info.overlaps(ke("x", "c", "b")));
    assertFalse(info.needsToBeChopped(ke("y", "c", "b")));
    assertFalse(info.needsToBeChopped(ke("x", "c", "bb")));
    assertFalse(info.needsToBeChopped(ke("x", "b", "a")));
  }

}

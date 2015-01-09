/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.master.state;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class MergeInfoTest {
  private KeyExtent keyExtent;
  private MergeInfo mi;

  @Before
  public void setUp() throws Exception {
    keyExtent = createMock(KeyExtent.class);
  }

  @Test
  public void testConstruction_NoArgs() {
    mi = new MergeInfo();
    assertEquals(MergeState.NONE, mi.getState());
    assertNull(mi.getExtent());
    assertEquals(MergeInfo.Operation.MERGE, mi.getOperation());
    assertFalse(mi.isDelete());
  }

  @Test
  public void testConstruction_2Args() {
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.DELETE);
    assertEquals(MergeState.NONE, mi.getState());
    assertSame(keyExtent, mi.getExtent());
    assertEquals(MergeInfo.Operation.DELETE, mi.getOperation());
    assertTrue(mi.isDelete());
  }

  @Test
  public void testSerialization() throws Exception {
    Text table = new Text("table");
    Text endRow = new Text("end");
    Text prevEndRow = new Text("begin");
    keyExtent = new KeyExtent(table, endRow, prevEndRow);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.DELETE);
    mi.setState(MergeState.STARTED);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    mi.write(dos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    mi = new MergeInfo();
    mi.readFields(dis);
    assertSame(MergeState.STARTED, mi.getState());
    assertEquals(keyExtent, mi.getExtent());
    assertSame(MergeInfo.Operation.DELETE, mi.getOperation());
  }

  @Test
  public void testNeedsToBeChopped_DifferentTables() {
    expect(keyExtent.getTableId()).andReturn(new Text("table1"));
    replay(keyExtent);
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent2.getTableId()).andReturn(new Text("table2"));
    replay(keyExtent2);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.MERGE);
    assertFalse(mi.needsToBeChopped(keyExtent2));
  }

  @Test
  public void testNeedsToBeChopped_NotDelete() {
    expect(keyExtent.getTableId()).andReturn(new Text("table1"));
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent2.getTableId()).andReturn(new Text("table1"));
    replay(keyExtent2);
    expect(keyExtent.overlaps(keyExtent2)).andReturn(true);
    replay(keyExtent);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.MERGE);
    assertTrue(mi.needsToBeChopped(keyExtent2));
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
    expect(keyExtent.getTableId()).andReturn(new Text("table1"));
    expect(keyExtent.getEndRow()).andReturn(new Text("prev"));
    replay(keyExtent);
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent2.getTableId()).andReturn(new Text("table1"));
    expect(keyExtent2.getPrevEndRow()).andReturn(prevEndRow != null ? new Text(prevEndRow) : null);
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
    expect(keyExtent.getTableId()).andReturn(new Text("table1"));
    replay(keyExtent);
    expect(keyExtent2.getTableId()).andReturn(new Text("table2"));
    replay(keyExtent2);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.MERGE);
    assertFalse(mi.overlaps(keyExtent2));
  }

  @Test
  public void testOverlaps_NeedsChopping() {
    KeyExtent keyExtent2 = createMock(KeyExtent.class);
    expect(keyExtent.overlaps(keyExtent2)).andReturn(false);
    expect(keyExtent.getTableId()).andReturn(new Text("table1"));
    expect(keyExtent.getEndRow()).andReturn(new Text("prev"));
    replay(keyExtent);
    expect(keyExtent2.getTableId()).andReturn(new Text("table1"));
    expect(keyExtent2.getPrevEndRow()).andReturn(new Text("prev"));
    expectLastCall().anyTimes();
    replay(keyExtent2);
    mi = new MergeInfo(keyExtent, MergeInfo.Operation.DELETE);
    assertTrue(mi.overlaps(keyExtent2));
  }
}

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
package org.apache.accumulo.master.state;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class MergeInfoTest {

  MergeInfo readWrite(MergeInfo info) throws Exception {
    DataOutputBuffer buffer = new DataOutputBuffer();
    info.write(buffer);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(buffer.getData(), 0, buffer.getLength());
    MergeInfo info2 = new MergeInfo();
    info2.readFields(in);
    Assert.assertEquals(info.getExtent(), info2.getExtent());
    Assert.assertEquals(info.getState(), info2.getState());
    Assert.assertEquals(info.getOperation(), info2.getOperation());
    return info2;
  }

  KeyExtent ke(String tableId, String endRow, String prevEndRow) {
    return new KeyExtent(new Text(tableId), endRow == null ? null : new Text(endRow), prevEndRow == null ? null : new Text(prevEndRow));
  }

  @Test
  public void testWritable() throws Exception {
    MergeInfo info;
    info = readWrite(new MergeInfo(ke("a", null, "b"), MergeInfo.Operation.MERGE));
    info = readWrite(new MergeInfo(ke("a", "b", null), MergeInfo.Operation.MERGE));
    info = readWrite(new MergeInfo(ke("x", "b", "a"), MergeInfo.Operation.MERGE));
    info = readWrite(new MergeInfo(ke("x", "b", "a"), MergeInfo.Operation.DELETE));
    Assert.assertTrue(info.isDelete());
    info.setState(MergeState.COMPLETE);
  }

  @Test
  public void testNeedsToBeChopped() throws Exception {
    MergeInfo info = new MergeInfo(ke("x", "b", "a"), MergeInfo.Operation.DELETE);
    Assert.assertTrue(info.needsToBeChopped(ke("x", "c", "b")));
    Assert.assertTrue(info.overlaps(ke("x", "c", "b")));
    Assert.assertFalse(info.needsToBeChopped(ke("y", "c", "b")));
    Assert.assertFalse(info.needsToBeChopped(ke("x", "c", "bb")));
    Assert.assertFalse(info.needsToBeChopped(ke("x", "b", "a")));
    info = new MergeInfo(ke("x", "b", "a"), MergeInfo.Operation.MERGE);
    Assert.assertTrue(info.needsToBeChopped(ke("x", "c", "a")));
    Assert.assertTrue(info.needsToBeChopped(ke("x", "aa", "a")));
    Assert.assertTrue(info.needsToBeChopped(ke("x", null, null)));
    Assert.assertFalse(info.needsToBeChopped(ke("x", "c", "b")));
    Assert.assertFalse(info.needsToBeChopped(ke("y", "c", "b")));
    Assert.assertFalse(info.needsToBeChopped(ke("x", "c", "bb")));
    Assert.assertTrue(info.needsToBeChopped(ke("x", "b", "a")));
  }

}

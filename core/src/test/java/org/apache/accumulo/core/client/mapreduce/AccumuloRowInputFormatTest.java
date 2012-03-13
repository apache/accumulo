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
package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ContextFactory;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

public class AccumuloRowInputFormatTest {
  List<Entry<Key,Value>> row1;
  List<Entry<Key,Value>> row2;
  List<Entry<Key,Value>> row3;
  
  {
    row1 = new ArrayList<Entry<Key,Value>>();
    row1.add(new KeyValue(new Key("row1", "colf1", "colq1"), "v1".getBytes()));
    row1.add(new KeyValue(new Key("row1", "colf1", "colq2"), "v2".getBytes()));
    row1.add(new KeyValue(new Key("row1", "colf2", "colq3"), "v3".getBytes()));
    row2 = new ArrayList<Entry<Key,Value>>();
    row2.add(new KeyValue(new Key("row2", "colf1", "colq4"), "v4".getBytes()));
    row3 = new ArrayList<Entry<Key,Value>>();
    row3.add(new KeyValue(new Key("row3", "colf1", "colq5"), "v5".getBytes()));
  }
  
  public static void checkLists(List<Entry<Key,Value>> a, List<Entry<Key,Value>> b) {
    assertEquals(a.size(), b.size());
    for (int i = 0; i < a.size(); i++) {
      assertEquals(a.get(i).getKey(), b.get(i).getKey());
      assertEquals(a.get(i).getValue(), b.get(i).getValue());
    }
  }
  
  public static void checkLists(List<Entry<Key,Value>> a, Iterator<Entry<Key,Value>> b) {
    int i = 0;
    while (b.hasNext()) {
      Entry<Key,Value> e = b.next();
      assertEquals(a.get(i).getKey(), e.getKey());
      assertEquals(a.get(i).getValue(), e.getValue());
      i++;
    }
  }
  
  public static void insertList(BatchWriter bw, List<Entry<Key,Value>> list) throws Exception {
    for (Entry<Key,Value> e : list) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
  }
  
  @Test
  public void test() throws Exception {
    MockInstance instance = new MockInstance("instance1");
    Connector conn = instance.getConnector("root", "".getBytes());
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", 100000l, 100l, 5);
    
    insertList(bw, row1);
    insertList(bw, row2);
    insertList(bw, row3);
    bw.close();
    
    JobContext job = ContextFactory.createJobContext();
    AccumuloRowInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "test", new Authorizations());
    AccumuloRowInputFormat.setMockInstance(job.getConfiguration(), "instance1");
    AccumuloRowInputFormat crif = new AccumuloRowInputFormat();
    RangeInputSplit ris = new RangeInputSplit();
    TaskAttemptContext tac = ContextFactory.createTaskAttemptContext(job);
    RecordReader<Text,PeekingIterator<Entry<Key,Value>>> rr = crif.createRecordReader(ris, tac);
    rr.initialize(ris, tac);
    
    assertTrue(rr.nextKeyValue());
    assertEquals(new Text("row1"), rr.getCurrentKey());
    checkLists(row1, rr.getCurrentValue());
    assertTrue(rr.nextKeyValue());
    assertEquals(new Text("row2"), rr.getCurrentKey());
    checkLists(row2, rr.getCurrentValue());
    assertTrue(rr.nextKeyValue());
    assertEquals(new Text("row3"), rr.getCurrentKey());
    checkLists(row3, rr.getCurrentValue());
    assertFalse(rr.nextKeyValue());
  }
}

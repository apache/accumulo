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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
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
  private static final String ROW1 = "row1";
  private static final String ROW2 = "row2";
  private static final String ROW3 = "row3";
  private static final String COLF1 = "colf1";
  private transient final List<Entry<Key,Value>> row1;
  private transient final List<Entry<Key,Value>> row2;
  private transient final List<Entry<Key,Value>> row3;
  
  public AccumuloRowInputFormatTest() {
    row1 = new ArrayList<Entry<Key,Value>>();
    row1.add(new KeyValue(new Key(ROW1, COLF1, "colq1"), "v1".getBytes()));
    row1.add(new KeyValue(new Key(ROW1, COLF1, "colq2"), "v2".getBytes()));
    row1.add(new KeyValue(new Key(ROW1, "colf2", "colq3"), "v3".getBytes()));
    row2 = new ArrayList<Entry<Key,Value>>();
    row2.add(new KeyValue(new Key(ROW2, COLF1, "colq4"), "v4".getBytes()));
    row3 = new ArrayList<Entry<Key,Value>>();
    row3.add(new KeyValue(new Key(ROW3, COLF1, "colq5"), "v5".getBytes()));
  }

  public static void checkLists(final List<Entry<Key,Value>> first, final List<Entry<Key,Value>> second) {
    assertEquals("Sizes should be the same.", first.size(), second.size());
    for (int i = 0; i < first.size(); i++) {
      assertEquals("Keys should be equal.", first.get(i).getKey(), second.get(i).getKey());
      assertEquals("Values should be equal.", first.get(i).getValue(), second.get(i).getValue());
    }
  }
  
  public static void checkLists(final List<Entry<Key,Value>> first, final Iterator<Entry<Key,Value>> second) {
    int entryIndex = 0; // NOPMD
    while (second.hasNext()) {
      final Entry<Key,Value> entry = second.next();
      assertEquals("Keys should be equal", first.get(entryIndex).getKey(), entry.getKey());
      assertEquals("Values should be equal", first.get(entryIndex).getValue(), entry.getValue());
      entryIndex++; // NOPMD
    }
  }
  
  public static void insertList(final BatchWriter writer, final List<Entry<Key,Value>> list) throws MutationsRejectedException {
    for (Entry<Key,Value> e : list) {
      final Key key = e.getKey();
      final Mutation mutation = new Mutation(key.getRow());  // NOPMD
      ColumnVisibility colVisibility = new ColumnVisibility(key.getColumnVisibility()); // NOPMD
      mutation.put(key.getColumnFamily(), key.getColumnQualifier(), colVisibility, key.getTimestamp(), e.getValue());
      writer.addMutation(mutation);
    }
  }
  
  @Test
  public void test() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException, InterruptedException {
    final MockInstance instance = new MockInstance("instance1");
    final Connector conn = instance.getConnector("root", "".getBytes());
    conn.tableOperations().create("test");
    BatchWriter writer = null; // NOPMD
    try {
      writer = conn.createBatchWriter("test", new BatchWriterConfig());
        insertList(writer, row1);
        insertList(writer, row2);
        insertList(writer, row3);
    } finally {
      if (writer != null) {
    	  writer.close();
      }
    }
    final JobContext job = ContextFactory.createJobContext();
    AccumuloRowInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "test", new Authorizations());
    AccumuloRowInputFormat.setMockInstance(job.getConfiguration(), "instance1");
    final AccumuloRowInputFormat crif = new AccumuloRowInputFormat();
    final RangeInputSplit ris = new RangeInputSplit();
    final TaskAttemptContext tac = ContextFactory.createTaskAttemptContext(job);
    final RecordReader<Text,PeekingIterator<Entry<Key,Value>>> recReader = crif.createRecordReader(ris, tac);
    recReader.initialize(ris, tac);
    
    assertTrue("Next key value should be true.", recReader.nextKeyValue());
    assertEquals("Current key should be " + ROW1, new Text(ROW1), recReader.getCurrentKey());
    checkLists(row1, recReader.getCurrentValue());
    assertTrue("Next key value should be true.", recReader.nextKeyValue());
    assertEquals("Current key should be " + ROW2, new Text(ROW2), recReader.getCurrentKey());
    checkLists(row2, recReader.getCurrentValue());
    assertTrue("Next key value should be true.", recReader.nextKeyValue());
    assertEquals("Current key should be " + ROW3, new Text(ROW3), recReader.getCurrentKey());
    checkLists(row3, recReader.getCurrentValue());
    assertFalse("Next key value should be false.", recReader.nextKeyValue());
  }
}

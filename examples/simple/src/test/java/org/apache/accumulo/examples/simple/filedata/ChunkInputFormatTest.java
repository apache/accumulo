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
package org.apache.accumulo.examples.simple.filedata;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ContextFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public class ChunkInputFormatTest extends TestCase {
  private static final Logger log = Logger.getLogger(ChunkInputStream.class);
  List<Entry<Key,Value>> data;
  List<Entry<Key,Value>> baddata;
  
  {
    data = new ArrayList<Entry<Key,Value>>();
    ChunkInputStreamTest.addData(data, "a", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamTest.addData(data, "a", "refs", "ida\0name", "A&B", "name");
    ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
    ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 1, "A&B", "");
    ChunkInputStreamTest.addData(data, "b", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamTest.addData(data, "b", "refs", "ida\0name", "A&B", "name");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "A&B", "");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "B&C", "");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "D", "");
    baddata = new ArrayList<Entry<Key,Value>>();
    ChunkInputStreamTest.addData(baddata, "c", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamTest.addData(baddata, "c", "refs", "ida\0name", "A&B", "name");
  }
  
  public static void entryEquals(Entry<Key,Value> e1, Entry<Key,Value> e2) {
    assertEquals(e1.getKey(), e2.getKey());
    assertEquals(e1.getValue(), e2.getValue());
  }
  
  public void test() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    MockInstance instance = new MockInstance("instance1");
    Connector conn = instance.getConnector("root", "".getBytes());
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", 100000l, 100l, 5);
    
    for (Entry<Key,Value> e : data) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();
    
    JobContext job = ContextFactory.createJobContext();
    ChunkInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "test", new Authorizations("A", "B", "C", "D"));
    ChunkInputFormat.setMockInstance(job.getConfiguration(), "instance1");
    ChunkInputFormat cif = new ChunkInputFormat();
    RangeInputSplit ris = new RangeInputSplit();
    TaskAttemptContext tac = ContextFactory.createTaskAttemptContext(job.getConfiguration());
    RecordReader<List<Entry<Key,Value>>,InputStream> rr = cif.createRecordReader(ris, tac);
    rr.initialize(ris, tac);
    
    assertTrue(rr.nextKeyValue());
    List<Entry<Key,Value>> info = rr.getCurrentKey();
    InputStream cis = rr.getCurrentValue();
    byte[] b = new byte[20];
    int read;
    assertEquals(info.size(), 2);
    entryEquals(info.get(0), data.get(0));
    entryEquals(info.get(1), data.get(1));
    assertEquals(read = cis.read(b), 8);
    assertEquals(new String(b, 0, read), "asdfjkl;");
    assertEquals(read = cis.read(b), -1);
    cis.close();
    
    assertTrue(rr.nextKeyValue());
    info = rr.getCurrentKey();
    cis = rr.getCurrentValue();
    assertEquals(info.size(), 2);
    entryEquals(info.get(0), data.get(4));
    entryEquals(info.get(1), data.get(5));
    assertEquals(read = cis.read(b), 10);
    assertEquals(new String(b, 0, read), "qwertyuiop");
    assertEquals(read = cis.read(b), -1);
    cis.close();
    
    assertFalse(rr.nextKeyValue());
  }
  
  public void testErrorOnNextWithoutClose() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException {
    MockInstance instance = new MockInstance("instance2");
    Connector conn = instance.getConnector("root", "".getBytes());
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", 100000l, 100l, 5);
    
    for (Entry<Key,Value> e : data) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();
    
    JobContext job = ContextFactory.createJobContext();
    ChunkInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "test", new Authorizations("A", "B", "C", "D"));
    ChunkInputFormat.setMockInstance(job.getConfiguration(), "instance2");
    ChunkInputFormat cif = new ChunkInputFormat();
    RangeInputSplit ris = new RangeInputSplit();
    TaskAttemptContext tac = ContextFactory.createTaskAttemptContext(job.getConfiguration());
    RecordReader<List<Entry<Key,Value>>,InputStream> crr = cif.createRecordReader(ris, tac);
    crr.initialize(ris, tac);
    
    assertTrue(crr.nextKeyValue());
    InputStream cis = crr.getCurrentValue();
    byte[] b = new byte[5];
    int read;
    assertEquals(read = cis.read(b), 5);
    assertEquals(new String(b, 0, read), "asdfj");
    
    try {
      crr.nextKeyValue();
      assertNotNull(null);
    } catch (Exception e) {
      log.debug("EXCEPTION " + e.getMessage());
      assertNull(null);
    }
  }
  
  public void testInfoWithoutChunks() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException {
    MockInstance instance = new MockInstance("instance3");
    Connector conn = instance.getConnector("root", "".getBytes());
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", 100000l, 100l, 5);
    for (Entry<Key,Value> e : baddata) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();
    
    JobContext job = ContextFactory.createJobContext();
    ChunkInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "test", new Authorizations("A", "B", "C", "D"));
    ChunkInputFormat.setMockInstance(job.getConfiguration(), "instance3");
    ChunkInputFormat cif = new ChunkInputFormat();
    RangeInputSplit ris = new RangeInputSplit();
    TaskAttemptContext tac = ContextFactory.createTaskAttemptContext(job.getConfiguration());
    RecordReader<List<Entry<Key,Value>>,InputStream> crr = cif.createRecordReader(ris, tac);
    crr.initialize(ris, tac);
    
    assertTrue(crr.nextKeyValue());
    List<Entry<Key,Value>> info = crr.getCurrentKey();
    InputStream cis = crr.getCurrentValue();
    byte[] b = new byte[20];
    assertEquals(info.size(), 2);
    entryEquals(info.get(0), baddata.get(0));
    entryEquals(info.get(1), baddata.get(1));
    try {
      cis.read(b);
      assertNotNull(null);
    } catch (Exception e) {
      log.debug("EXCEPTION " + e.getMessage());
      assertNull(null);
    }
    try {
      cis.close();
      assertNotNull(null);
    } catch (Exception e) {
      log.debug("EXCEPTION " + e.getMessage());
      assertNull(null);
    }
  }
}

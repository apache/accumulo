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
package org.apache.accumulo.core.client.mock;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.Assert;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MockConnectorTest {
  Random random = new Random();
  
  static Text asText(int i) {
    return new Text(Integer.toHexString(i));
  }
  
  @Test
  public void testSunnyDay() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("test");
    BatchWriter bw = c.createBatchWriter("test", 10000L, 1000L, 4);
    for (int i = 0; i < 100; i++) {
      int r = random.nextInt();
      Mutation m = new Mutation(asText(r));
      m.put(asText(random.nextInt()), asText(random.nextInt()), new Value(Integer.toHexString(r).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    BatchScanner s = c.createBatchScanner("test", Constants.NO_AUTHS, 2);
    s.setRanges(Collections.singletonList(new Range()));
    Key key = null;
    int count = 0;
    for (Entry<Key,Value> entry : s) {
      if (key != null)
        assertTrue(key.compareTo(entry.getKey()) < 0);
      assertEquals(entry.getKey().getRow(), new Text(entry.getValue().get()));
      key = entry.getKey();
      count++;
    }
    assertEquals(100, count);
  }
  
  @Test
  public void testChangeAuths() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    c.securityOperations().createUser("greg", new byte[] {}, new Authorizations("A", "B", "C"));
    assertTrue(c.securityOperations().getUserAuthorizations("greg").contains("A".getBytes()));
    c.securityOperations().changeUserAuthorizations("greg", new Authorizations("X", "Y", "Z"));
    assertTrue(c.securityOperations().getUserAuthorizations("greg").contains("X".getBytes()));
    assertFalse(c.securityOperations().getUserAuthorizations("greg").contains("A".getBytes()));
  }
  
  @Test
  public void testAggregation() throws Exception {
    MockInstance mockInstance = new MockInstance();
    Connector c = mockInstance.getConnector("root", new byte[] {});
    String table = "perDayCounts";
    c.tableOperations().create(table);
    IteratorSetting is = new IteratorSetting(10, "String Summation", SummingCombiner.class);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("day")));
    SummingCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    c.tableOperations().attachIterator(table, is);
    String keys[][] = { {"foo", "day", "20080101"}, {"foo", "day", "20080101"}, {"foo", "day", "20080103"}, {"bar", "day", "20080101"},
        {"bar", "day", "20080101"},};
    BatchWriter bw = c.createBatchWriter("perDayCounts", 1000L, 1000L, 1);
    for (String elt[] : keys) {
      Mutation m = new Mutation(new Text(elt[0]));
      m.put(new Text(elt[1]), new Text(elt[2]), new Value("1".getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    
    Scanner s = c.createScanner("perDayCounts", Constants.NO_AUTHS);
    Iterator<Entry<Key,Value>> iterator = s.iterator();
    assertTrue(iterator.hasNext());
    checkEntry(iterator.next(), "bar", "day", "20080101", "2");
    assertTrue(iterator.hasNext());
    checkEntry(iterator.next(), "foo", "day", "20080101", "2");
    assertTrue(iterator.hasNext());
    checkEntry(iterator.next(), "foo", "day", "20080103", "1");
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void testDelete() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("test");
    BatchWriter bw = c.createBatchWriter("test", 10000L, 1000L, 4);
    
    Mutation m1 = new Mutation("r1");
    
    m1.put("cf1", "cq1", 1, "v1");
    
    bw.addMutation(m1);
    bw.flush();
    
    Mutation m2 = new Mutation("r1");
    
    m2.putDelete("cf1", "cq1", 2);
    
    bw.addMutation(m2);
    bw.flush();
    
    Scanner scanner = c.createScanner("test", Constants.NO_AUTHS);
    
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    
    assertEquals(0, count);
    
    try {
      c.tableOperations().create("test_this_$tableName");
      assertTrue(false);
      
    } catch (IllegalArgumentException iae) {
      
    }
  }
  
  @Test
  public void testDeletewithBatchDeleter() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    
    // make sure we are using a clean table
    if (c.tableOperations().exists("test"))
      c.tableOperations().delete("test");
    c.tableOperations().create("test");
    
    BatchDeleter deleter = c.createBatchDeleter("test", Constants.NO_AUTHS, 2, 10000L, 1000L, 4);
    // first make sure it deletes fine when its empty
    deleter.setRanges(Collections.singletonList(new Range(("r1"))));
    deleter.delete();
    this.checkRemaining(c, "test", 0);
    
    // test deleting just one row
    BatchWriter writer = c.createBatchWriter("test", 10, 10, 1);
    Mutation m = new Mutation("r1");
    m.put("fam", "qual", "value");
    writer.addMutation(m);
    
    // make sure the write goes through
    writer.flush();
    writer.close();
    
    deleter.setRanges(Collections.singletonList(new Range(("r1"))));
    deleter.delete();
    this.checkRemaining(c, "test", 0);
    
    // test multi row deletes
    writer = c.createBatchWriter("test", 10, 10, 1);
    m = new Mutation("r1");
    m.put("fam", "qual", "value");
    writer.addMutation(m);
    Mutation m2 = new Mutation("r2");
    m2.put("fam", "qual", "value");
    writer.addMutation(m2);
    
    // make sure the write goes through
    writer.flush();
    writer.close();
    
    deleter.setRanges(Collections.singletonList(new Range(("r1"))));
    deleter.delete();
    checkRemaining(c, "test", 1);
  }
  
  /**
   * Test to make sure that a certain number of rows remain
   * 
   * @param c
   *          connector to the {@link MockInstance}
   * @param tableName
   *          TODO
   * @param count
   *          number of entries to expect in the table
   * @param count
   *          TODO
   */
  private void checkRemaining(Connector c, String tableName, int count) throws Exception {
    Scanner scanner = c.createScanner(tableName, Constants.NO_AUTHS);
    
    int total = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      total++;
    }
    assertEquals(count, total);
  }
  
  @Test
  public void testCMod() throws Exception {
    // test writing to a table that the is being scanned
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("test");
    BatchWriter bw = c.createBatchWriter("test", 10000L, 1000L, 4);
    
    for (int i = 0; i < 10; i++) {
      Mutation m1 = new Mutation("r" + i);
      m1.put("cf1", "cq1", 1, "v" + i);
      bw.addMutation(m1);
    }
    
    bw.flush();
    
    int count = 10;
    
    Scanner scanner = c.createScanner("test", Constants.NO_AUTHS);
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      Mutation m = new Mutation(key.getRow());
      m.put(key.getColumnFamily().toString(), key.getColumnQualifier().toString(), key.getTimestamp() + 1, "v" + (count));
      count++;
      bw.addMutation(m);
    }
    
    bw.flush();
    
    count = 10;
    
    for (Entry<Key,Value> entry : scanner) {
      assertEquals(entry.getValue().toString(), "v" + (count++));
    }
    
    assertEquals(count, 20);
    
    try {
      c.tableOperations().create("test_this_$tableName");
      assertTrue(false);
      
    } catch (IllegalArgumentException iae) {
      
    }
  }
  
  private void checkEntry(Entry<Key,Value> next, String row, String cf, String cq, String value) {
    assertEquals(row, next.getKey().getRow().toString());
    assertEquals(cf, next.getKey().getColumnFamily().toString());
    assertEquals(cq, next.getKey().getColumnQualifier().toString());
    assertEquals(value, next.getValue().toString());
  }
  
  @Test
  public void testMockMultiTableBatchWriter() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("a");
    c.tableOperations().create("b");
    MultiTableBatchWriter bw = c.createMultiTableBatchWriter(10000L, 1000L, 4);
    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "cq1", 1, "v1");
    BatchWriter b = bw.getBatchWriter("a");
    b.addMutation(m1);
    b.flush();
    b = bw.getBatchWriter("b");
    b.addMutation(m1);
    b.flush();
    
    Scanner scanner = c.createScanner("a", Constants.NO_AUTHS);
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    assertEquals(1, count);
    count = 0;
    scanner = c.createScanner("b", Constants.NO_AUTHS);
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    assertEquals(1, count);

  }
  
  @Test
  public void testUpdate() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("test");
    BatchWriter bw = c.createBatchWriter("test", 1000, 1000l, 1);
    
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation("r1");
      m.put("cf1", "cq1", "" + i);
      bw.addMutation(m);
    }
    
    bw.close();
    
    Scanner scanner = c.createScanner("test", Constants.NO_AUTHS);
    
    Entry<Key,Value> entry = scanner.iterator().next();
    
    assertEquals("9", entry.getValue().toString());

  }
  
  @Test
  public void testMockConnectorReturnsCorrectInstance() throws AccumuloException, 
      AccumuloSecurityException{
    String name = "an-interesting-instance-name";
    Instance mockInstance = new MockInstance(name);
    Assert.assertEquals(mockInstance, mockInstance.getConnector("foo", "bar").getInstance());
    Assert.assertEquals(name, mockInstance.getConnector("foo","bar").getInstance().getInstanceName());
  }

}

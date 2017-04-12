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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterators;

@Deprecated
public class MockConnectorTest {
  Random random = new Random();

  static Text asText(int i) {
    return new Text(Integer.toHexString(i));
  }

  @Test
  public void testSunnyDay() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("test");
    BatchWriter bw = c.createBatchWriter("test", new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      int r = random.nextInt();
      Mutation m = new Mutation(asText(r));
      m.put(asText(random.nextInt()), asText(random.nextInt()), new Value(Integer.toHexString(r).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    BatchScanner s = c.createBatchScanner("test", Authorizations.EMPTY, 2);
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
    c.securityOperations().createLocalUser("greg", new PasswordToken(new byte[0]));
    assertTrue(c.securityOperations().getUserAuthorizations("greg").isEmpty());
    c.securityOperations().changeUserAuthorizations("greg", new Authorizations("A".getBytes()));
    assertTrue(c.securityOperations().getUserAuthorizations("greg").contains("A".getBytes()));
    c.securityOperations().changeUserAuthorizations("greg", new Authorizations("X", "Y", "Z"));
    assertTrue(c.securityOperations().getUserAuthorizations("greg").contains("X".getBytes()));
    assertFalse(c.securityOperations().getUserAuthorizations("greg").contains("A".getBytes()));
  }

  @Test
  public void testBadMutations() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("test");
    BatchWriter bw = c
        .createBatchWriter("test", new BatchWriterConfig().setMaxMemory(10000L).setMaxLatency(1000L, TimeUnit.MILLISECONDS).setMaxWriteThreads(4));

    try {
      bw.addMutation(null);
      Assert.fail("addMutation should throw IAE for null mutation");
    } catch (IllegalArgumentException iae) {}
    try {
      bw.addMutations(null);
      Assert.fail("addMutations should throw IAE for null iterable");
    } catch (IllegalArgumentException iae) {}

    bw.addMutations(Collections.<Mutation> emptyList());

    Mutation bad = new Mutation("bad");
    try {
      bw.addMutation(bad);
      Assert.fail("addMutation should throw IAE for empty mutation");
    } catch (IllegalArgumentException iae) {}

    Mutation good = new Mutation("good");
    good.put(asText(random.nextInt()), asText(random.nextInt()), new Value("good".getBytes()));
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(good);
    mutations.add(bad);
    try {
      bw.addMutations(mutations);
      Assert.fail("addMutations should throw IAE if it contains empty mutation");
    } catch (IllegalArgumentException iae) {}

    bw.close();
  }

  @Test
  public void testAggregation() throws Exception {
    MockInstance mockInstance = new MockInstance();
    Connector c = mockInstance.getConnector("root", new PasswordToken(""));
    String table = "perDayCounts";
    c.tableOperations().create(table);
    IteratorSetting is = new IteratorSetting(10, "String Summation", SummingCombiner.class);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("day")));
    SummingCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    c.tableOperations().attachIterator(table, is);
    String keys[][] = { {"foo", "day", "20080101"}, {"foo", "day", "20080101"}, {"foo", "day", "20080103"}, {"bar", "day", "20080101"},
        {"bar", "day", "20080101"},};
    BatchWriter bw = c.createBatchWriter("perDayCounts", new BatchWriterConfig());
    for (String elt[] : keys) {
      Mutation m = new Mutation(new Text(elt[0]));
      m.put(new Text(elt[1]), new Text(elt[2]), new Value("1".getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    Scanner s = c.createScanner("perDayCounts", Authorizations.EMPTY);
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
    BatchWriter bw = c.createBatchWriter("test", new BatchWriterConfig());

    Mutation m1 = new Mutation("r1");

    m1.put("cf1", "cq1", 1, "v1");

    bw.addMutation(m1);
    bw.flush();

    Mutation m2 = new Mutation("r1");

    m2.putDelete("cf1", "cq1", 2);

    bw.addMutation(m2);
    bw.flush();

    Scanner scanner = c.createScanner("test", Authorizations.EMPTY);

    int count = Iterators.size(scanner.iterator());

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

    BatchDeleter deleter = c.createBatchDeleter("test", Authorizations.EMPTY, 2, new BatchWriterConfig());
    // first make sure it deletes fine when its empty
    deleter.setRanges(Collections.singletonList(new Range(("r1"))));
    deleter.delete();
    this.checkRemaining(c, "test", 0);

    // test deleting just one row
    BatchWriter writer = c.createBatchWriter("test", new BatchWriterConfig());
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
    writer = c.createBatchWriter("test", new BatchWriterConfig());
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
   *          Table to check
   * @param count
   *          number of entries to expect in the table
   */
  private void checkRemaining(Connector c, String tableName, int count) throws Exception {
    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);

    int total = Iterators.size(scanner.iterator());
    assertEquals(count, total);
  }

  @Test
  public void testCMod() throws Exception {
    // test writing to a table that the is being scanned
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("test");
    BatchWriter bw = c.createBatchWriter("test", new BatchWriterConfig());

    for (int i = 0; i < 10; i++) {
      Mutation m1 = new Mutation("r" + i);
      m1.put("cf1", "cq1", 1, "v" + i);
      bw.addMutation(m1);
    }

    bw.flush();

    int count = 10;

    Scanner scanner = c.createScanner("test", Authorizations.EMPTY);
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
    MultiTableBatchWriter bw = c.createMultiTableBatchWriter(new BatchWriterConfig());
    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "cq1", 1, "v1");
    BatchWriter b = bw.getBatchWriter("a");
    b.addMutation(m1);
    b.flush();
    b = bw.getBatchWriter("b");
    b.addMutation(m1);
    b.flush();

    Scanner scanner = c.createScanner("a", Authorizations.EMPTY);
    int count = Iterators.size(scanner.iterator());
    assertEquals(1, count);
    scanner = c.createScanner("b", Authorizations.EMPTY);
    count = Iterators.size(scanner.iterator());
    assertEquals(1, count);

  }

  @Test
  public void testUpdate() throws Exception {
    Connector c = new MockConnector("root", new MockInstance());
    c.tableOperations().create("test");
    BatchWriter bw = c.createBatchWriter("test", new BatchWriterConfig());

    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation("r1");
      m.put("cf1", "cq1", "" + i);
      bw.addMutation(m);
    }

    bw.close();

    Scanner scanner = c.createScanner("test", Authorizations.EMPTY);

    Entry<Key,Value> entry = scanner.iterator().next();

    assertEquals("9", entry.getValue().toString());

  }

  @Test
  public void testMockConnectorReturnsCorrectInstance() throws AccumuloException, AccumuloSecurityException {
    String name = "an-interesting-instance-name";
    Instance mockInstance = new MockInstance(name);
    assertEquals(mockInstance, mockInstance.getConnector("foo", new PasswordToken("bar")).getInstance());
    assertEquals(name, mockInstance.getConnector("foo", new PasswordToken("bar")).getInstance().getInstanceName());
  }

}

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

package org.apache.accumulo.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.test.functional.BadIterator;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * 
 */
public class ConditionalWriterTest {
  
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }
  
  @Test
  public void testBasic() throws Exception {
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create("foo");
    
    ConditionalWriter cw = conn.createConditionalWriter("foo", new ConditionalWriterConfig());
    
    // mutation conditional on column tx:seq not exiting
    ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq"));
    cm0.put("name", "last", "doe");
    cm0.put("name", "first", "john");
    cm0.put("tx", "seq", "1");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
    Assert.assertEquals(Status.REJECTED, cw.write(cm0).getStatus());
    
    // mutation conditional on column tx:seq being 1
    ConditionalMutation cm1 = new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"));
    cm1.put("name", "last", "Doe");
    cm1.put("tx", "seq", "2");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
    
    // test condition where value differs
    ConditionalMutation cm2 = new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"));
    cm2.put("name", "last", "DOE");
    cm2.put("tx", "seq", "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm2).getStatus());
    
    // test condition where column does not exists
    ConditionalMutation cm3 = new ConditionalMutation("99006", new Condition("txtypo", "seq").setValue("1"));
    cm3.put("name", "last", "deo");
    cm3.put("tx", "seq", "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm3).getStatus());
    
    // test two conditions, where one should fail
    ConditionalMutation cm4 = new ConditionalMutation("99006", new Condition("tx", "seq").setValue("2"), new Condition("name", "last").setValue("doe"));
    cm4.put("name", "last", "deo");
    cm4.put("tx", "seq", "3");
    Assert.assertEquals(Status.REJECTED, cw.write(cm4).getStatus());
    
    // test two conditions, where one should fail
    ConditionalMutation cm5 = new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"), new Condition("name", "last").setValue("Doe"));
    cm5.put("name", "last", "deo");
    cm5.put("tx", "seq", "3");
    Assert.assertEquals(Status.REJECTED, cw.write(cm5).getStatus());
    
    // ensure rejected mutations did not write
    Scanner scanner = conn.createScanner("foo", Authorizations.EMPTY);
    scanner.fetchColumn(new Text("name"), new Text("last"));
    scanner.setRange(new Range("99006"));
    Assert.assertEquals("Doe", scanner.iterator().next().getValue().toString());
    
    // test w/ two conditions that are met
    ConditionalMutation cm6 = new ConditionalMutation("99006", new Condition("tx", "seq").setValue("2"), new Condition("name", "last").setValue("Doe"));
    cm6.put("name", "last", "DOE");
    cm6.put("tx", "seq", "3");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());
    
    Assert.assertEquals("DOE", scanner.iterator().next().getValue().toString());
    
    // test a conditional mutation that deletes
    ConditionalMutation cm7 = new ConditionalMutation("99006", new Condition("tx", "seq").setValue("3"));
    cm7.putDelete("name", "last");
    cm7.putDelete("name", "first");
    cm7.putDelete("tx", "seq");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm7).getStatus());
    
    Assert.assertFalse(scanner.iterator().hasNext());
    
    // add the row back
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
    Assert.assertEquals(Status.REJECTED, cw.write(cm0).getStatus());
    
    Assert.assertEquals("doe", scanner.iterator().next().getValue().toString());
  }
  
  @Test
  public void testFields() throws Exception {
    String table = "foo2";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    Authorizations auths = new Authorizations("A", "B");
    
    conn.securityOperations().changeUserAuthorizations("root", auths);
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig().setAuthorizations(auths));
    
    ColumnVisibility cva = new ColumnVisibility("A");
    ColumnVisibility cvb = new ColumnVisibility("B");
    
    ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cva));
    cm0.put("name", "last", cva, "doe");
    cm0.put("name", "first", cva, "john");
    cm0.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
    
    Scanner scanner = conn.createScanner(table, auths);
    scanner.setRange(new Range("99006"));
    // TODO verify all columns
    scanner.fetchColumn(new Text("tx"), new Text("seq"));
    Entry<Key,Value> entry = scanner.iterator().next();
    Assert.assertEquals("1", entry.getValue().toString());
    long ts = entry.getKey().getTimestamp();
    
    // test wrong colf
    ConditionalMutation cm1 = new ConditionalMutation("99006", new Condition("txA", "seq").setVisibility(cva).setValue("1"));
    cm1.put("name", "last", cva, "Doe");
    cm1.put("name", "first", cva, "John");
    cm1.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm1).getStatus());
    
    // test wrong colq
    ConditionalMutation cm2 = new ConditionalMutation("99006", new Condition("tx", "seqA").setVisibility(cva).setValue("1"));
    cm2.put("name", "last", cva, "Doe");
    cm2.put("name", "first", cva, "John");
    cm2.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm2).getStatus());
    
    // test wrong colv
    ConditionalMutation cm3 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
    cm3.put("name", "last", cva, "Doe");
    cm3.put("name", "first", cva, "John");
    cm3.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm3).getStatus());
    
    // test wrong timestamp
    ConditionalMutation cm4 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts + 1).setValue("1"));
    cm4.put("name", "last", cva, "Doe");
    cm4.put("name", "first", cva, "John");
    cm4.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm4).getStatus());
    
    // test wrong timestamp
    ConditionalMutation cm5 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts - 1).setValue("1"));
    cm5.put("name", "last", cva, "Doe");
    cm5.put("name", "first", cva, "John");
    cm5.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm5).getStatus());
    
    // ensure no updates were made
    entry = scanner.iterator().next();
    Assert.assertEquals("1", entry.getValue().toString());
    
    // set all columns correctly
    ConditionalMutation cm6 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts).setValue("1"));
    cm6.put("name", "last", cva, "Doe");
    cm6.put("name", "first", cva, "John");
    cm6.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());
    
    entry = scanner.iterator().next();
    Assert.assertEquals("2", entry.getValue().toString());
    
    // TODO test each field w/ absence
    
  }
  
  @Test
  public void testBadColVis() throws Exception {
    // test when a user sets a col vis in a condition that can never be seen
    String table = "foo3";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    Authorizations auths = new Authorizations("A", "B");
    
    conn.securityOperations().changeUserAuthorizations("root", auths);
    
    Authorizations filteredAuths = new Authorizations("A");
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig().setAuthorizations(filteredAuths));
    
    ColumnVisibility cva = new ColumnVisibility("A");
    ColumnVisibility cvb = new ColumnVisibility("B");
    ColumnVisibility cvc = new ColumnVisibility("C");
    
    // User has authorization, but didn't include it in the writer
    ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb));
    cm0.put("name", "last", cva, "doe");
    cm0.put("name", "first", cva, "john");
    cm0.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm0).getStatus());
    
    ConditionalMutation cm1 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
    cm1.put("name", "last", cva, "doe");
    cm1.put("name", "first", cva, "john");
    cm1.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm1).getStatus());
    
    // User does not have the authorization
    ConditionalMutation cm2 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvc));
    cm2.put("name", "last", cva, "doe");
    cm2.put("name", "first", cva, "john");
    cm2.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm2).getStatus());
    
    ConditionalMutation cm3 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvc).setValue("1"));
    cm3.put("name", "last", cva, "doe");
    cm3.put("name", "first", cva, "john");
    cm3.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm3).getStatus());
    
    // if any visibility is bad, good visibilities don't override
    ConditionalMutation cm4 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb), new Condition("tx", "seq").setVisibility(cva));
    
    cm4.put("name", "last", cva, "doe");
    cm4.put("name", "first", cva, "john");
    cm4.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm4).getStatus());
    
    ConditionalMutation cm5 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb).setValue("1"), new Condition("tx", "seq")
        .setVisibility(cva).setValue("1"));
    cm5.put("name", "last", cva, "doe");
    cm5.put("name", "first", cva, "john");
    cm5.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm5).getStatus());
    
    ConditionalMutation cm6 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb).setValue("1"),
        new Condition("tx", "seq").setVisibility(cva));
    cm6.put("name", "last", cva, "doe");
    cm6.put("name", "first", cva, "john");
    cm6.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm6).getStatus());
    
    ConditionalMutation cm7 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb), new Condition("tx", "seq").setVisibility(cva)
        .setValue("1"));
    cm7.put("name", "last", cva, "doe");
    cm7.put("name", "first", cva, "john");
    cm7.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm7).getStatus());
    
    cw.close();
    
    // test passing auths that exceed users configured auths
    
    Authorizations exceedingAuths = new Authorizations("A", "B", "D");
    ConditionalWriter cw2 = conn.createConditionalWriter(table, new ConditionalWriterConfig().setAuthorizations(exceedingAuths));
    
    ConditionalMutation cm8 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb), new Condition("tx", "seq").setVisibility(cva)
        .setValue("1"));
    cm8.put("name", "last", cva, "doe");
    cm8.put("name", "first", cva, "john");
    cm8.put("tx", "seq", cva, "1");
    
    try {
      cw2.write(cm8).getStatus();
      Assert.fail();
    } catch (AccumuloSecurityException ase) {}
    
    cw2.close();
  }
  
  @Test
  public void testConstraints() throws Exception {
    // ensure constraint violations are properly reported
    String table = "foo5";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    conn.tableOperations().addConstraint(table, AlphaNumKeyConstraint.class.getName());
    conn.tableOperations().clone(table, table + "_clone", true, new HashMap<String,String>(), new HashSet<String>());
    
    Scanner scanner = conn.createScanner(table + "_clone", new Authorizations());
    
    ConditionalWriter cw = conn.createConditionalWriter(table + "_clone", new ConditionalWriterConfig());
    
    ConditionalMutation cm0 = new ConditionalMutation("99006+", new Condition("tx", "seq"));
    cm0.put("tx", "seq", "1");
    
    Assert.assertEquals(Status.VIOLATED, cw.write(cm0).getStatus());
    Assert.assertFalse(scanner.iterator().hasNext());
    
    ConditionalMutation cm1 = new ConditionalMutation("99006", new Condition("tx", "seq"));
    cm1.put("tx", "seq", "1");
    
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
    Assert.assertTrue(scanner.iterator().hasNext());
    
    cw.close();
    
  }
  
  @Test
  public void testIterators() throws Exception {
    String table = "foo4";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table, false);
    
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    
    Mutation m = new Mutation("ACCUMULO-1000");
    m.put("count", "comments", "1");
    bw.addMutation(m);
    bw.addMutation(m);
    bw.addMutation(m);
    
    m = new Mutation("ACCUMULO-1001");
    m.put("count2", "comments", "1");
    bw.addMutation(m);
    bw.addMutation(m);
    
    m = new Mutation("ACCUMULO-1002");
    m.put("count2", "comments", "1");
    bw.addMutation(m);
    bw.addMutation(m);
    
    bw.close();
    
    IteratorSetting iterConfig = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setEncodingType(iterConfig, Type.STRING);
    SummingCombiner.setColumns(iterConfig, Collections.singletonList(new IteratorSetting.Column("count")));
    
    IteratorSetting iterConfig2 = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setEncodingType(iterConfig2, Type.STRING);
    SummingCombiner.setColumns(iterConfig2, Collections.singletonList(new IteratorSetting.Column("count2", "comments")));
    
    IteratorSetting iterConfig3 = new IteratorSetting(5, VersioningIterator.class);
    VersioningIterator.setMaxVersions(iterConfig3, 1);
    
    Scanner scanner = conn.createScanner(table, new Authorizations());
    scanner.addScanIterator(iterConfig);
    scanner.setRange(new Range("ACCUMULO-1000"));
    scanner.fetchColumn(new Text("count"), new Text("comments"));
    
    Assert.assertEquals("3", scanner.iterator().next().getValue().toString());
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig());
    
    ConditionalMutation cm0 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setValue("3"));
    cm0.put("count", "comments", "1");
    Assert.assertEquals(Status.REJECTED, cw.write(cm0).getStatus());
    Assert.assertEquals("3", scanner.iterator().next().getValue().toString());
    
    ConditionalMutation cm1 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setIterators(iterConfig).setValue("3"));
    cm1.put("count", "comments", "1");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
    Assert.assertEquals("4", scanner.iterator().next().getValue().toString());
    
    ConditionalMutation cm2 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setValue("4"));
    cm2.put("count", "comments", "1");
    Assert.assertEquals(Status.REJECTED, cw.write(cm1).getStatus());
    Assert.assertEquals("4", scanner.iterator().next().getValue().toString());
    
    // run test with multiple iterators passed in same batch and condition with two iterators
    
    ConditionalMutation cm3 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setIterators(iterConfig).setValue("4"));
    cm3.put("count", "comments", "1");
    
    ConditionalMutation cm4 = new ConditionalMutation("ACCUMULO-1001", new Condition("count2", "comments").setIterators(iterConfig2).setValue("2"));
    cm4.put("count2", "comments", "1");
    
    ConditionalMutation cm5 = new ConditionalMutation("ACCUMULO-1002", new Condition("count2", "comments").setIterators(iterConfig2, iterConfig3).setValue("2"));
    cm5.put("count2", "comments", "1");
    
    Iterator<Result> results = cw.write(Arrays.asList(cm3, cm4, cm5).iterator());
    Map<String,Status> actual = new HashMap<String,Status>();
    
    while (results.hasNext()) {
      Result result = results.next();
      String k = new String(result.getMutation().getRow());
      Assert.assertFalse(actual.containsKey(k));
      actual.put(k, result.getStatus());
    }
    
    Map<String,Status> expected = new HashMap<String,Status>();
    expected.put("ACCUMULO-1000", Status.ACCEPTED);
    expected.put("ACCUMULO-1001", Status.ACCEPTED);
    expected.put("ACCUMULO-1002", Status.REJECTED);
    
    Assert.assertEquals(expected, actual);
    
    // TODO test w/ table that has iterators configured
    
    cw.close();
  }
  
  @Test
  public void testBatch() throws Exception {
    String table = "foo6";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B"));
    
    ColumnVisibility cvab = new ColumnVisibility("A|B");
    
    ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
    
    ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvab));
    cm0.put("name", "last", cvab, "doe");
    cm0.put("name", "first", cvab, "john");
    cm0.put("tx", "seq", cvab, "1");
    mutations.add(cm0);
    
    ConditionalMutation cm1 = new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvab));
    cm1.put("name", "last", cvab, "doe");
    cm1.put("name", "first", cvab, "jane");
    cm1.put("tx", "seq", cvab, "1");
    mutations.add(cm1);
    
    ConditionalMutation cm2 = new ConditionalMutation("19059", new Condition("tx", "seq").setVisibility(cvab));
    cm2.put("name", "last", cvab, "doe");
    cm2.put("name", "first", cvab, "jack");
    cm2.put("tx", "seq", cvab, "1");
    mutations.add(cm2);
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig().setAuthorizations(new Authorizations("A")));
    Iterator<Result> results = cw.write(mutations.iterator());
    int count = 0;
    while (results.hasNext()) {
      Result result = results.next();
      Assert.assertEquals(Status.ACCEPTED, result.getStatus());
      count++;
    }
    
    Assert.assertEquals(3, count);
    
    Scanner scanner = conn.createScanner(table, new Authorizations("A"));
    scanner.fetchColumn(new Text("tx"), new Text("seq"));
    
    for (String row : new String[] {"99006", "59056", "19059"}) {
      scanner.setRange(new Range(row));
      Assert.assertEquals("1", scanner.iterator().next().getValue().toString());
    }
    
    TreeSet<Text> splits = new TreeSet<Text>();
    splits.add(new Text("7"));
    splits.add(new Text("3"));
    conn.tableOperations().addSplits(table, splits);
    
    mutations.clear();
    
    ConditionalMutation cm3 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvab).setValue("1"));
    cm3.put("name", "last", cvab, "Doe");
    cm3.put("tx", "seq", cvab, "2");
    mutations.add(cm3);
    
    ConditionalMutation cm4 = new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvab));
    cm4.put("name", "last", cvab, "Doe");
    cm4.put("tx", "seq", cvab, "1");
    mutations.add(cm4);
    
    ConditionalMutation cm5 = new ConditionalMutation("19059", new Condition("tx", "seq").setVisibility(cvab).setValue("2"));
    cm5.put("name", "last", cvab, "Doe");
    cm5.put("tx", "seq", cvab, "3");
    mutations.add(cm5);
    
    results = cw.write(mutations.iterator());
    int accepted = 0;
    int rejected = 0;
    while (results.hasNext()) {
      Result result = results.next();
      if (new String(result.getMutation().getRow()).equals("99006")) {
        Assert.assertEquals(Status.ACCEPTED, result.getStatus());
        accepted++;
      } else {
        Assert.assertEquals(Status.REJECTED, result.getStatus());
        rejected++;
      }
    }
    
    Assert.assertEquals(1, accepted);
    Assert.assertEquals(2, rejected);
    
    for (String row : new String[] {"59056", "19059"}) {
      scanner.setRange(new Range(row));
      Assert.assertEquals("1", scanner.iterator().next().getValue().toString());
    }
    
    scanner.setRange(new Range("99006"));
    Assert.assertEquals("2", scanner.iterator().next().getValue().toString());
    
    scanner.clearColumns();
    scanner.fetchColumn(new Text("name"), new Text("last"));
    Assert.assertEquals("Doe", scanner.iterator().next().getValue().toString());
    
    cw.close();
  }
  
  @Test
  public void testBigBatch() throws Exception {
    
    String table = "foo100";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    conn.tableOperations().addSplits(table, nss("2", "4", "6"));
    
    UtilWaitThread.sleep(2000);
    
    int num = 100;
    
    ArrayList<byte[]> rows = new ArrayList<byte[]>(num);
    ArrayList<ConditionalMutation> cml = new ArrayList<ConditionalMutation>(num);
    
    Random r = new Random();
    byte[] e = new byte[0];
    
    for (int i = 0; i < num; i++) {
      rows.add(FastFormat.toZeroPaddedString(Math.abs(r.nextLong()), 16, 16, e));
    }
    
    for (int i = 0; i < num; i++) {
      ConditionalMutation cm = new ConditionalMutation(rows.get(i), new Condition("meta", "seq"));
      
      cm.put("meta", "seq", "1");
      cm.put("meta", "tx", UUID.randomUUID().toString());
      
      cml.add(cm);
    }
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig());
    
    Iterator<Result> results = cw.write(cml.iterator());
    
    int count = 0;
    
    // TODO check got each row back
    while (results.hasNext()) {
      Result result = results.next();
      Assert.assertEquals(Status.ACCEPTED, result.getStatus());
      count++;
    }
    
    Assert.assertEquals(num, count);
    
    ArrayList<ConditionalMutation> cml2 = new ArrayList<ConditionalMutation>(num);
    
    for (int i = 0; i < num; i++) {
      ConditionalMutation cm = new ConditionalMutation(rows.get(i), new Condition("meta", "seq").setValue("1"));
      
      cm.put("meta", "seq", "2");
      cm.put("meta", "tx", UUID.randomUUID().toString());
      
      cml2.add(cm);
    }
    
    count = 0;
    
    results = cw.write(cml2.iterator());
    
    while (results.hasNext()) {
      Result result = results.next();
      Assert.assertEquals(Status.ACCEPTED, result.getStatus());
      count++;
    }
    
    Assert.assertEquals(num, count);
    
    cw.close();
  }
  
  @Test
  public void testBatchErrors() throws Exception {
    
    String table = "foo7";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    conn.tableOperations().addConstraint(table, AlphaNumKeyConstraint.class.getName());
    conn.tableOperations().clone(table, table + "_clone", true, new HashMap<String,String>(), new HashSet<String>());
    
    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B"));
    
    ColumnVisibility cvaob = new ColumnVisibility("A|B");
    ColumnVisibility cvaab = new ColumnVisibility("A&B");
    
    switch ((new Random()).nextInt(3)) {
      case 1:
        conn.tableOperations().addSplits(table, nss("6"));
        break;
      case 2:
        conn.tableOperations().addSplits(table, nss("2", "95"));
        break;
    }
    
    ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
    
    ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvaob));
    cm0.put("name+", "last", cvaob, "doe");
    cm0.put("name", "first", cvaob, "john");
    cm0.put("tx", "seq", cvaob, "1");
    mutations.add(cm0);
    
    ConditionalMutation cm1 = new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvaab));
    cm1.put("name", "last", cvaab, "doe");
    cm1.put("name", "first", cvaab, "jane");
    cm1.put("tx", "seq", cvaab, "1");
    mutations.add(cm1);
    
    ConditionalMutation cm2 = new ConditionalMutation("19059", new Condition("tx", "seq").setVisibility(cvaob));
    cm2.put("name", "last", cvaob, "doe");
    cm2.put("name", "first", cvaob, "jack");
    cm2.put("tx", "seq", cvaob, "1");
    mutations.add(cm2);
    
    ConditionalMutation cm3 = new ConditionalMutation("90909", new Condition("tx", "seq").setVisibility(cvaob).setValue("1"));
    cm3.put("name", "last", cvaob, "doe");
    cm3.put("name", "first", cvaob, "john");
    cm3.put("tx", "seq", cvaob, "2");
    mutations.add(cm3);
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig().setAuthorizations(new Authorizations("A")));
    Iterator<Result> results = cw.write(mutations.iterator());
    HashSet<String> rows = new HashSet<String>();
    while (results.hasNext()) {
      Result result = results.next();
      String row = new String(result.getMutation().getRow());
      if (row.equals("19059")) {
        Assert.assertEquals(Status.ACCEPTED, result.getStatus());
      } else if (row.equals("59056")) {
        Assert.assertEquals(Status.INVISIBLE_VISIBILITY, result.getStatus());
      } else if (row.equals("99006")) {
        Assert.assertEquals(Status.VIOLATED, result.getStatus());
      } else if (row.equals("90909")) {
        Assert.assertEquals(Status.REJECTED, result.getStatus());
      }
      rows.add(row);
    }
    
    Assert.assertEquals(4, rows.size());
    
    Scanner scanner = conn.createScanner(table, new Authorizations("A"));
    scanner.fetchColumn(new Text("tx"), new Text("seq"));
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    Assert.assertEquals("1", iter.next().getValue().toString());
    Assert.assertFalse(iter.hasNext());
    
    cw.close();
  }
  
  @Test
  public void testSameRow() throws Exception {
    // test multiple mutations for same row in same batch
    
    String table = "foo8";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig());
    
    ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
    cm1.put("tx", "seq", "1");
    cm1.put("data", "x", "a");
    
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
    
    ConditionalMutation cm2 = new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
    cm2.put("tx", "seq", "2");
    cm2.put("data", "x", "b");
    
    ConditionalMutation cm3 = new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
    cm3.put("tx", "seq", "2");
    cm3.put("data", "x", "c");
    
    ConditionalMutation cm4 = new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
    cm4.put("tx", "seq", "2");
    cm4.put("data", "x", "d");
    
    Iterator<Result> results = cw.write(Arrays.asList(cm2, cm3, cm4).iterator());
    
    int accepted = 0;
    int rejected = 0;
    int total = 0;
    
    while (results.hasNext()) {
      Status status = results.next().getStatus();
      if (status == Status.ACCEPTED)
        accepted++;
      if (status == Status.REJECTED)
        rejected++;
      total++;
    }
    
    Assert.assertEquals(1, accepted);
    Assert.assertEquals(2, rejected);
    Assert.assertEquals(3, total);
    
    cw.close();
  }
  
  private static class Stats {
    
    ByteSequence row = null;
    int seq;
    long sum;
    int data[] = new int[10];
    
    public Stats(Iterator<Entry<Key,Value>> iterator) {
      while (iterator.hasNext()) {
        Entry<Key,Value> entry = iterator.next();
        
        if (row == null)
          row = entry.getKey().getRowData();
        
        String cf = entry.getKey().getColumnFamilyData().toString();
        String cq = entry.getKey().getColumnQualifierData().toString();
        
        if (cf.equals("data")) {
          data[Integer.parseInt(cq)] = Integer.parseInt(entry.getValue().toString());
        } else if (cf.equals("meta")) {
          if (cq.equals("sum")) {
            sum = Long.parseLong(entry.getValue().toString());
          } else if (cq.equals("seq")) {
            seq = Integer.parseInt(entry.getValue().toString());
          }
        }
      }
      
      long sum2 = 0;
      
      for (int datum : data) {
        sum2 += datum;
      }
      
      Assert.assertEquals(sum2, sum);
    }
    
    public Stats(ByteSequence row) {
      this.row = row;
      for (int i = 0; i < data.length; i++) {
        this.data[i] = 0;
      }
      this.seq = -1;
      this.sum = 0;
    }
    
    void set(int index, int value) {
      sum -= data[index];
      sum += value;
      data[index] = value;
    }
    
    ConditionalMutation toMutation() {
      Condition cond = new Condition("meta", "seq");
      if (seq >= 0)
        cond.setValue(seq + "");
      
      ConditionalMutation cm = new ConditionalMutation(row, cond);
      
      cm.put("meta", "seq", (seq + 1) + "");
      cm.put("meta", "sum", (sum) + "");
      
      for (int i = 0; i < data.length; i++) {
        cm.put("data", i + "", data[i] + "");
      }
      
      return cm;
    }
    
    @Override
    public String toString() {
      return row + " " + seq + " " + sum;
    }
  }
  
  private static class MutatorTask implements Runnable {
    String table;
    ArrayList<ByteSequence> rows;
    ConditionalWriter cw;
    Connector conn;
    AtomicBoolean failed;
    
    public MutatorTask(String table, Connector conn, ArrayList<ByteSequence> rows, ConditionalWriter cw, AtomicBoolean failed) {
      this.table = table;
      this.rows = rows;
      this.conn = conn;
      this.cw = cw;
      this.failed = failed;
    }
    
    @Override
    public void run() {
      try {
        Random rand = new Random();
        
        Scanner scanner = new IsolatedScanner(conn.createScanner(table, Authorizations.EMPTY));
        
        for (int i = 0; i < 20; i++) {
          int numRows = rand.nextInt(10) + 1;
          
          ArrayList<ByteSequence> changes = new ArrayList<ByteSequence>(numRows);
          ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
          
          for (int j = 0; j < numRows; j++)
            changes.add(rows.get(rand.nextInt(rows.size())));
          
          for (ByteSequence row : changes) {
            scanner.setRange(new Range(row.toString()));
            Stats stats = new Stats(scanner.iterator());
            stats.set(rand.nextInt(10), Math.abs(rand.nextInt()));
            mutations.add(stats.toMutation());
          }
          
          ArrayList<ByteSequence> changed = new ArrayList<ByteSequence>(numRows);
          Iterator<Result> results = cw.write(mutations.iterator());
          while (results.hasNext()) {
            Result result = results.next();
            changed.add(new ArrayByteSequence(result.getMutation().getRow()));
          }
          
          Collections.sort(changes);
          Collections.sort(changed);
          
          Assert.assertEquals(changes, changed);
          
        }
        
      } catch (Exception e) {
        e.printStackTrace();
        failed.set(true);
      }
    }
  }
  
  @Test
  public void testThreads() throws Exception {
    // test multiple threads using a single conditional writer
    
    String table = "foo9";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    Random rand = new Random();
    
    switch (rand.nextInt(3)) {
      case 1:
        conn.tableOperations().addSplits(table, nss("4"));
        break;
      case 2:
        conn.tableOperations().addSplits(table, nss("3", "5"));
        break;
    }
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig());
    
    ArrayList<ByteSequence> rows = new ArrayList<ByteSequence>();
    
    for (int i = 0; i < 1000; i++) {
      rows.add(new ArrayByteSequence(FastFormat.toZeroPaddedString(Math.abs(rand.nextLong()), 16, 16, new byte[0])));
    }
    
    ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
    
    for (ByteSequence row : rows)
      mutations.add(new Stats(row).toMutation());
    
    ArrayList<ByteSequence> rows2 = new ArrayList<ByteSequence>();
    Iterator<Result> results = cw.write(mutations.iterator());
    while (results.hasNext()) {
      Result result = results.next();
      Assert.assertEquals(Status.ACCEPTED, result.getStatus());
      rows2.add(new ArrayByteSequence(result.getMutation().getRow()));
    }
    
    Collections.sort(rows);
    Collections.sort(rows2);
    
    Assert.assertEquals(rows, rows2);
    
    AtomicBoolean failed = new AtomicBoolean(false);
    
    ExecutorService tp = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 5; i++) {
      tp.submit(new MutatorTask(table, conn, rows, cw, failed));
    }
    
    tp.shutdown();
    
    while (!tp.isTerminated()) {
      tp.awaitTermination(1, TimeUnit.MINUTES);
    }
    
    Assert.assertFalse(failed.get());
    
    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
    
    RowIterator rowIter = new RowIterator(scanner);
    
    while (rowIter.hasNext()) {
      Iterator<Entry<Key,Value>> row = rowIter.next();
      new Stats(row);
    }
  }
  
  private SortedSet<Text> nss(String... splits) {
    TreeSet<Text> ret = new TreeSet<Text>();
    for (String split : splits)
      ret.add(new Text(split));
    
    return ret;
  }
  
  @Test
  public void testSecurity() throws Exception {
    // test against table user does not have read and/or write permissions for
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.securityOperations().createLocalUser("user1", new PasswordToken("u1p"));
    
    conn.tableOperations().create("sect1");
    conn.tableOperations().create("sect2");
    conn.tableOperations().create("sect3");
    
    conn.securityOperations().grantTablePermission("user1", "sect1", TablePermission.READ);
    conn.securityOperations().grantTablePermission("user1", "sect2", TablePermission.WRITE);
    conn.securityOperations().grantTablePermission("user1", "sect3", TablePermission.READ);
    conn.securityOperations().grantTablePermission("user1", "sect3", TablePermission.WRITE);
    
    Connector conn2 = zki.getConnector("user1", new PasswordToken("u1p"));
    
    ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
    cm1.put("tx", "seq", "1");
    cm1.put("data", "x", "a");
    
    ConditionalWriter cw1 = conn2.createConditionalWriter("sect1", new ConditionalWriterConfig());
    ConditionalWriter cw2 = conn2.createConditionalWriter("sect2", new ConditionalWriterConfig());
    ConditionalWriter cw3 = conn2.createConditionalWriter("sect3", new ConditionalWriterConfig());
    
    Assert.assertEquals(Status.ACCEPTED, cw3.write(cm1).getStatus());
    
    try {
      cw1.write(cm1).getStatus();
      Assert.assertFalse(true);
    } catch (AccumuloSecurityException ase) {
      
    }
    
    try {
      cw2.write(cm1).getStatus();
      Assert.assertFalse(true);
    } catch (AccumuloSecurityException ase) {
      
    }
    
  }
  
  @Test
  public void testTimeout() throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    String table = "fooT";
    
    conn.tableOperations().create(table);
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig().setTimeout(1, TimeUnit.SECONDS));
    
    ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
    cm1.put("tx", "seq", "1");
    cm1.put("data", "x", "a");
    
    Assert.assertEquals(cw.write(cm1).getStatus(), Status.ACCEPTED);
    
    IteratorSetting is = new IteratorSetting(5, SlowIterator.class);
    SlowIterator.setSeekSleepTime(is, 1500);
    
    ConditionalMutation cm2 = new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1").setIterators(is));
    cm2.put("tx", "seq", "2");
    cm2.put("data", "x", "b");
    
    Assert.assertEquals(cw.write(cm2).getStatus(), Status.UNKNOWN);
    
    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
    
    for (Entry<Key,Value> entry : scanner) {
      String cf = entry.getKey().getColumnFamilyData().toString();
      String cq = entry.getKey().getColumnQualifierData().toString();
      String val = entry.getValue().toString();
      
      if (cf.equals("tx") && cq.equals("seq"))
        Assert.assertEquals("1", val);
      else if (cf.equals("data") && cq.equals("x"))
        Assert.assertEquals("a", val);
      else
        Assert.fail();
    }
    
    ConditionalMutation cm3 = new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
    cm3.put("tx", "seq", "2");
    cm3.put("data", "x", "b");
    
    Assert.assertEquals(cw.write(cm3).getStatus(), Status.ACCEPTED);
    
    cw.close();
    
  }
  
  @Test
  public void testDeleteTable() throws Exception {
    String table = "foo12";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    try {
      conn.createConditionalWriter(table, new ConditionalWriterConfig());
      Assert.assertFalse(true);
    } catch (TableNotFoundException e) {}
    
    conn.tableOperations().create(table);
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig());
    
    conn.tableOperations().delete(table);
    
    ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
    cm1.put("tx", "seq", "1");
    cm1.put("data", "x", "a");
    
    Result result = cw.write(cm1);
    
    try {
      result.getStatus();
      Assert.assertFalse(true);
    } catch (AccumuloException ae) {
      Assert.assertEquals(TableDeletedException.class, ae.getCause().getClass());
    }
    
  }
  
  @Test
  public void testOffline() throws Exception {
    String table = "foo11";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig());
    
    conn.tableOperations().offline(table);
    
    waitForSingleTabletTableToGoOffline(table, zki);
    
    ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
    cm1.put("tx", "seq", "1");
    cm1.put("data", "x", "a");
    
    Result result = cw.write(cm1);
    
    try {
      result.getStatus();
      Assert.assertFalse(true);
    } catch (AccumuloException ae) {
      Assert.assertEquals(TableOfflineException.class, ae.getCause().getClass());
    }
    
    cw.close();
    
    try {
      conn.createConditionalWriter(table, new ConditionalWriterConfig());
      Assert.assertFalse(true);
    } catch (TableOfflineException e) {}
  }
  
  void waitForSingleTabletTableToGoOffline(String table, ZooKeeperInstance zki) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    TabletLocator locator = TabletLocator.getLocator(zki, new Text(Tables.getNameToIdMap(zki).get(table)));
    while (locator.locateTablet(new Credentials("root", new PasswordToken(secret)), new Text("a"), false, false) != null) {
      UtilWaitThread.sleep(50);
      locator.invalidateCache();
    }
  }
  
  @Test
  public void testError() throws Exception {
    String table = "foo10";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig());
    
    IteratorSetting iterSetting = new IteratorSetting(5, BadIterator.class);
    
    ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq").setIterators(iterSetting));
    cm1.put("tx", "seq", "1");
    cm1.put("data", "x", "a");
    
    Result result = cw.write(cm1);
    
    try {
      result.getStatus();
      Assert.assertFalse(true);
    } catch (AccumuloException ae) {
      
    }
    
    cw.close();
    
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }
}

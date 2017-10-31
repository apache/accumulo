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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
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
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.constraints.AlphaNumKeyConstraint;
import org.apache.accumulo.test.functional.BadIterator;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.tracer.TraceDump;
import org.apache.accumulo.tracer.TraceDump.Printer;
import org.apache.accumulo.tracer.TraceServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 *
 */
public class ConditionalWriterIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(ConditionalWriterIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configureMiniCluster(cfg, hadoopCoreSite);
    // Set the min span to 0 so we will definitely get all the traces back. See ACCUMULO-4365
    Map<String,String> siteConf = cfg.getSiteConfig();
    siteConf.put(Property.TRACE_SPAN_RECEIVER_PREFIX.getKey() + "tracer.span.min.ms", "0");
    cfg.setSiteConfig(siteConf);
  }

  public static long abs(long l) {
    l = Math.abs(l); // abs(Long.MIN_VALUE) == Long.MIN_VALUE...
    if (l < 0)
      return 0;
    return l;
  }

  @Before
  public void deleteUsers() throws Exception {
    Connector conn = getConnector();
    Set<String> users = conn.securityOperations().listLocalUsers();
    ClusterUser user = getUser(0);
    if (users.contains(user.getPrincipal())) {
      conn.securityOperations().dropLocalUser(user.getPrincipal());
    }
  }

  @Test
  public void testBasic() throws Exception {

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

      // mutation conditional on column tx:seq not existing
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
      Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
      scanner.fetchColumn(new Text("name"), new Text("last"));
      scanner.setRange(new Range("99006"));
      Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("Doe", entry.getValue().toString());

      // test w/ two conditions that are met
      ConditionalMutation cm6 = new ConditionalMutation("99006", new Condition("tx", "seq").setValue("2"), new Condition("name", "last").setValue("Doe"));
      cm6.put("name", "last", "DOE");
      cm6.put("tx", "seq", "3");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("DOE", entry.getValue().toString());

      // test a conditional mutation that deletes
      ConditionalMutation cm7 = new ConditionalMutation("99006", new Condition("tx", "seq").setValue("3"));
      cm7.putDelete("name", "last");
      cm7.putDelete("name", "first");
      cm7.putDelete("tx", "seq");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm7).getStatus());

      Assert.assertFalse("Did not expect to find any results", scanner.iterator().hasNext());

      // add the row back
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
      Assert.assertEquals(Status.REJECTED, cw.write(cm0).getStatus());

      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("doe", entry.getValue().toString());
    }
  }

  @Test
  public void testFields() throws Exception {

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    String user = null;
    ClientConfiguration clientConf = cluster.getClientConfig();
    final boolean saslEnabled = clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false);

    ClusterUser user1 = getUser(0);
    user = user1.getPrincipal();
    if (saslEnabled) {
      // The token is pointless for kerberos
      conn.securityOperations().createLocalUser(user, null);
    } else {
      conn.securityOperations().createLocalUser(user, new PasswordToken(user1.getPassword()));
    }

    Authorizations auths = new Authorizations("A", "B");

    conn.securityOperations().changeUserAuthorizations(user, auths);
    conn.securityOperations().grantSystemPermission(user, SystemPermission.CREATE_TABLE);

    conn = conn.getInstance().getConnector(user, user1.getToken());

    conn.tableOperations().create(tableName);

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig().setAuthorizations(auths))) {

      ColumnVisibility cva = new ColumnVisibility("A");
      ColumnVisibility cvb = new ColumnVisibility("B");

      ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cva));
      cm0.put("name", "last", cva, "doe");
      cm0.put("name", "first", cva, "john");
      cm0.put("tx", "seq", cva, "1");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());

      Scanner scanner = conn.createScanner(tableName, auths);
      scanner.setRange(new Range("99006"));
      // TODO verify all columns
      scanner.fetchColumn(new Text("tx"), new Text("seq"));
      Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
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
      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("1", entry.getValue().toString());

      // set all columns correctly
      ConditionalMutation cm6 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts).setValue("1"));
      cm6.put("name", "last", cva, "Doe");
      cm6.put("name", "first", cva, "John");
      cm6.put("tx", "seq", cva, "2");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("2", entry.getValue().toString());
    }
  }

  @Test
  public void testBadColVis() throws Exception {
    // test when a user sets a col vis in a condition that can never be seen

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    Authorizations auths = new Authorizations("A", "B");

    conn.securityOperations().changeUserAuthorizations(getAdminPrincipal(), auths);

    Authorizations filteredAuths = new Authorizations("A");

    ColumnVisibility cva = new ColumnVisibility("A");
    ColumnVisibility cvb = new ColumnVisibility("B");
    ColumnVisibility cvc = new ColumnVisibility("C");

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig().setAuthorizations(filteredAuths))) {

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

    }

    // test passing auths that exceed users configured auths

    Authorizations exceedingAuths = new Authorizations("A", "B", "D");
    try (ConditionalWriter cw2 = conn.createConditionalWriter(tableName, new ConditionalWriterConfig().setAuthorizations(exceedingAuths))) {

      ConditionalMutation cm8 = new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb), new Condition("tx", "seq").setVisibility(cva)
          .setValue("1"));
      cm8.put("name", "last", cva, "doe");
      cm8.put("name", "first", cva, "john");
      cm8.put("tx", "seq", cva, "1");

      try {
        Status status = cw2.write(cm8).getStatus();
        Assert.fail("Writing mutation with Authorizations the user doesn't have should fail. Got status: " + status);
      } catch (AccumuloSecurityException ase) {
        // expected, check specific failure?
      }
    }
  }

  @Test
  public void testConstraints() throws Exception {
    // ensure constraint violations are properly reported

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);
    conn.tableOperations().addConstraint(tableName, AlphaNumKeyConstraint.class.getName());
    conn.tableOperations().clone(tableName, tableName + "_clone", true, new HashMap<String,String>(), new HashSet<String>());

    Scanner scanner = conn.createScanner(tableName + "_clone", new Authorizations());

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName + "_clone", new ConditionalWriterConfig())) {

      ConditionalMutation cm0 = new ConditionalMutation("99006+", new Condition("tx", "seq"));
      cm0.put("tx", "seq", "1");

      Assert.assertEquals(Status.VIOLATED, cw.write(cm0).getStatus());
      Assert.assertFalse("Should find no results in the table is mutation result was violated", scanner.iterator().hasNext());

      ConditionalMutation cm1 = new ConditionalMutation("99006", new Condition("tx", "seq"));
      cm1.put("tx", "seq", "1");

      Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
      Assert.assertTrue("Accepted result should be returned when reading table", scanner.iterator().hasNext());
    }
  }

  @Test
  public void testIterators() throws Exception {

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName, new NewTableConfiguration().withoutDefaultIterators());

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

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

    Scanner scanner = conn.createScanner(tableName, new Authorizations());
    scanner.addScanIterator(iterConfig);
    scanner.setRange(new Range("ACCUMULO-1000"));
    scanner.fetchColumn(new Text("count"), new Text("comments"));

    Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
    Assert.assertEquals("3", entry.getValue().toString());

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

      ConditionalMutation cm0 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setValue("3"));
      cm0.put("count", "comments", "1");
      Assert.assertEquals(Status.REJECTED, cw.write(cm0).getStatus());
      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("3", entry.getValue().toString());

      ConditionalMutation cm1 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setIterators(iterConfig).setValue("3"));
      cm1.put("count", "comments", "1");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("4", entry.getValue().toString());

      ConditionalMutation cm2 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setValue("4"));
      cm2.put("count", "comments", "1");
      Assert.assertEquals(Status.REJECTED, cw.write(cm1).getStatus());
      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("4", entry.getValue().toString());

      // run test with multiple iterators passed in same batch and condition with two iterators

      ConditionalMutation cm3 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setIterators(iterConfig).setValue("4"));
      cm3.put("count", "comments", "1");

      ConditionalMutation cm4 = new ConditionalMutation("ACCUMULO-1001", new Condition("count2", "comments").setIterators(iterConfig2).setValue("2"));
      cm4.put("count2", "comments", "1");

      ConditionalMutation cm5 = new ConditionalMutation("ACCUMULO-1002", new Condition("count2", "comments").setIterators(iterConfig2, iterConfig3).setValue(
          "2"));
      cm5.put("count2", "comments", "1");

      Iterator<Result> results = cw.write(Arrays.asList(cm3, cm4, cm5).iterator());
      Map<String,Status> actual = new HashMap<>();

      while (results.hasNext()) {
        Result result = results.next();
        String k = new String(result.getMutation().getRow());
        Assert.assertFalse("Did not expect to see multiple resultus for the row: " + k, actual.containsKey(k));
        actual.put(k, result.getStatus());
      }

      Map<String,Status> expected = new HashMap<>();
      expected.put("ACCUMULO-1000", Status.ACCEPTED);
      expected.put("ACCUMULO-1001", Status.ACCEPTED);
      expected.put("ACCUMULO-1002", Status.REJECTED);

      Assert.assertEquals(expected, actual);
    }
  }

  public static class AddingIterator extends WrappingIterator {
    long amount = 0;

    @Override
    public Value getTopValue() {
      Value val = super.getTopValue();
      long l = Long.parseLong(val.toString());
      String newVal = (l + amount) + "";
      return new Value(newVal.getBytes(UTF_8));
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      this.setSource(source);
      amount = Long.parseLong(options.get("amount"));
    }
  }

  public static class MultiplyingIterator extends WrappingIterator {
    long amount = 0;

    @Override
    public Value getTopValue() {
      Value val = super.getTopValue();
      long l = Long.parseLong(val.toString());
      String newVal = l * amount + "";
      return new Value(newVal.getBytes(UTF_8));
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      this.setSource(source);
      amount = Long.parseLong(options.get("amount"));
    }
  }

  @Test
  public void testTableAndConditionIterators() throws Exception {

    // test w/ table that has iterators configured
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    IteratorSetting aiConfig1 = new IteratorSetting(30, "AI1", AddingIterator.class);
    aiConfig1.addOption("amount", "2");
    IteratorSetting aiConfig2 = new IteratorSetting(35, "MI1", MultiplyingIterator.class);
    aiConfig2.addOption("amount", "3");
    IteratorSetting aiConfig3 = new IteratorSetting(40, "AI2", AddingIterator.class);
    aiConfig3.addOption("amount", "5");

    conn.tableOperations().create(tableName);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    Mutation m = new Mutation("ACCUMULO-1000");
    m.put("count", "comments", "6");
    bw.addMutation(m);

    m = new Mutation("ACCUMULO-1001");
    m.put("count", "comments", "7");
    bw.addMutation(m);

    m = new Mutation("ACCUMULO-1002");
    m.put("count", "comments", "8");
    bw.addMutation(m);

    bw.close();

    conn.tableOperations().attachIterator(tableName, aiConfig1, EnumSet.of(IteratorScope.scan));
    conn.tableOperations().offline(tableName, true);
    conn.tableOperations().online(tableName, true);

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

      ConditionalMutation cm6 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setValue("8"));
      cm6.put("count", "comments", "7");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

      Scanner scanner = conn.createScanner(tableName, new Authorizations());
      scanner.setRange(new Range("ACCUMULO-1000"));
      scanner.fetchColumn(new Text("count"), new Text("comments"));

      Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("9", entry.getValue().toString());

      ConditionalMutation cm7 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setIterators(aiConfig2).setValue("27"));
      cm7.put("count", "comments", "8");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm7).getStatus());

      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("10", entry.getValue().toString());

      ConditionalMutation cm8 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setIterators(aiConfig2, aiConfig3).setValue("35"));
      cm8.put("count", "comments", "9");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm8).getStatus());

      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("11", entry.getValue().toString());

      ConditionalMutation cm3 = new ConditionalMutation("ACCUMULO-1000", new Condition("count", "comments").setIterators(aiConfig2).setValue("33"));
      cm3.put("count", "comments", "3");

      ConditionalMutation cm4 = new ConditionalMutation("ACCUMULO-1001", new Condition("count", "comments").setIterators(aiConfig3).setValue("14"));
      cm4.put("count", "comments", "3");

      ConditionalMutation cm5 = new ConditionalMutation("ACCUMULO-1002", new Condition("count", "comments").setIterators(aiConfig3).setValue("10"));
      cm5.put("count", "comments", "3");

      Iterator<Result> results = cw.write(Arrays.asList(cm3, cm4, cm5).iterator());
      Map<String,Status> actual = new HashMap<>();

      while (results.hasNext()) {
        Result result = results.next();
        String k = new String(result.getMutation().getRow());
        Assert.assertFalse("Did not expect to see multiple resultus for the row: " + k, actual.containsKey(k));
        actual.put(k, result.getStatus());
      }

      Map<String,Status> expected = new HashMap<>();
      expected.put("ACCUMULO-1000", Status.ACCEPTED);
      expected.put("ACCUMULO-1001", Status.ACCEPTED);
      expected.put("ACCUMULO-1002", Status.REJECTED);

      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testBatch() throws Exception {

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    conn.securityOperations().changeUserAuthorizations(getAdminPrincipal(), new Authorizations("A", "B"));

    ColumnVisibility cvab = new ColumnVisibility("A|B");

    ArrayList<ConditionalMutation> mutations = new ArrayList<>();

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

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig().setAuthorizations(new Authorizations("A")))) {
      Iterator<Result> results = cw.write(mutations.iterator());
      int count = 0;
      while (results.hasNext()) {
        Result result = results.next();
        Assert.assertEquals(Status.ACCEPTED, result.getStatus());
        count++;
      }

      Assert.assertEquals(3, count);

      Scanner scanner = conn.createScanner(tableName, new Authorizations("A"));
      scanner.fetchColumn(new Text("tx"), new Text("seq"));

      for (String row : new String[] {"99006", "59056", "19059"}) {
        scanner.setRange(new Range(row));
        Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
        Assert.assertEquals("1", entry.getValue().toString());
      }

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("7"));
      splits.add(new Text("3"));
      conn.tableOperations().addSplits(tableName, splits);

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

      Assert.assertEquals("Expected only one accepted conditional mutation", 1, accepted);
      Assert.assertEquals("Expected two rejected conditional mutations", 2, rejected);

      for (String row : new String[] {"59056", "19059"}) {
        scanner.setRange(new Range(row));
        Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
        Assert.assertEquals("1", entry.getValue().toString());
      }

      scanner.setRange(new Range("99006"));
      Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("2", entry.getValue().toString());

      scanner.clearColumns();
      scanner.fetchColumn(new Text("name"), new Text("last"));
      entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("Doe", entry.getValue().toString());
    }
  }

  @Test
  public void testBigBatch() throws Exception {

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);
    conn.tableOperations().addSplits(tableName, nss("2", "4", "6"));

    sleepUninterruptibly(2, TimeUnit.SECONDS);

    int num = 100;

    ArrayList<byte[]> rows = new ArrayList<>(num);
    ArrayList<ConditionalMutation> cml = new ArrayList<>(num);

    Random r = new Random();
    byte[] e = new byte[0];

    for (int i = 0; i < num; i++) {
      rows.add(FastFormat.toZeroPaddedString(abs(r.nextLong()), 16, 16, e));
    }

    for (int i = 0; i < num; i++) {
      ConditionalMutation cm = new ConditionalMutation(rows.get(i), new Condition("meta", "seq"));

      cm.put("meta", "seq", "1");
      cm.put("meta", "tx", UUID.randomUUID().toString());

      cml.add(cm);
    }

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

      Iterator<Result> results = cw.write(cml.iterator());

      int count = 0;

      // TODO check got each row back
      while (results.hasNext()) {
        Result result = results.next();
        Assert.assertEquals(Status.ACCEPTED, result.getStatus());
        count++;
      }

      Assert.assertEquals("Did not receive the expected number of results", num, count);

      ArrayList<ConditionalMutation> cml2 = new ArrayList<>(num);

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

      Assert.assertEquals("Did not receive the expected number of results", num, count);
    }
  }

  @Test
  public void testBatchErrors() throws Exception {

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);
    conn.tableOperations().addConstraint(tableName, AlphaNumKeyConstraint.class.getName());
    conn.tableOperations().clone(tableName, tableName + "_clone", true, new HashMap<String,String>(), new HashSet<String>());

    conn.securityOperations().changeUserAuthorizations(getAdminPrincipal(), new Authorizations("A", "B"));

    ColumnVisibility cvaob = new ColumnVisibility("A|B");
    ColumnVisibility cvaab = new ColumnVisibility("A&B");

    switch ((new Random()).nextInt(3)) {
      case 1:
        conn.tableOperations().addSplits(tableName, nss("6"));
        break;
      case 2:
        conn.tableOperations().addSplits(tableName, nss("2", "95"));
        break;
    }

    ArrayList<ConditionalMutation> mutations = new ArrayList<>();

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

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig().setAuthorizations(new Authorizations("A")))) {
      Iterator<Result> results = cw.write(mutations.iterator());
      HashSet<String> rows = new HashSet<>();
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

      Scanner scanner = conn.createScanner(tableName, new Authorizations("A"));
      scanner.fetchColumn(new Text("tx"), new Text("seq"));

      Entry<Key,Value> entry = Iterables.getOnlyElement(scanner);
      Assert.assertEquals("1", entry.getValue().toString());
    }
  }

  @Test
  public void testSameRow() throws Exception {
    // test multiple mutations for same row in same batch

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

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

      Assert.assertEquals("Expected one accepted result", 1, accepted);
      Assert.assertEquals("Expected two rejected results", 2, rejected);
      Assert.assertEquals("Expected three total results", 3, total);
    }
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
    String tableName;
    ArrayList<ByteSequence> rows;
    ConditionalWriter cw;
    Connector conn;
    AtomicBoolean failed;

    public MutatorTask(String tableName, Connector conn, ArrayList<ByteSequence> rows, ConditionalWriter cw, AtomicBoolean failed) {
      this.tableName = tableName;
      this.rows = rows;
      this.conn = conn;
      this.cw = cw;
      this.failed = failed;
    }

    @Override
    public void run() {
      try (Scanner scanner = new IsolatedScanner(conn.createScanner(tableName, Authorizations.EMPTY))) {
        Random rand = new Random();

        for (int i = 0; i < 20; i++) {
          int numRows = rand.nextInt(10) + 1;

          ArrayList<ByteSequence> changes = new ArrayList<>(numRows);
          ArrayList<ConditionalMutation> mutations = new ArrayList<>();

          for (int j = 0; j < numRows; j++)
            changes.add(rows.get(rand.nextInt(rows.size())));

          for (ByteSequence row : changes) {
            scanner.setRange(new Range(row.toString()));
            Stats stats = new Stats(scanner.iterator());
            stats.set(rand.nextInt(10), rand.nextInt(Integer.MAX_VALUE));
            mutations.add(stats.toMutation());
          }

          ArrayList<ByteSequence> changed = new ArrayList<>(numRows);
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
        log.error("{}", e.getMessage(), e);
        failed.set(true);
      }
    }
  }

  @Test
  public void testThreads() throws Exception {
    // test multiple threads using a single conditional writer

    String tableName = getUniqueNames(1)[0];
    Connector conn = getConnector();

    conn.tableOperations().create(tableName);

    Random rand = new Random();

    switch (rand.nextInt(3)) {
      case 1:
        conn.tableOperations().addSplits(tableName, nss("4"));
        break;
      case 2:
        conn.tableOperations().addSplits(tableName, nss("3", "5"));
        break;
    }

    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

      ArrayList<ByteSequence> rows = new ArrayList<>();

      for (int i = 0; i < 1000; i++) {
        rows.add(new ArrayByteSequence(FastFormat.toZeroPaddedString(abs(rand.nextLong()), 16, 16, new byte[0])));
      }

      ArrayList<ConditionalMutation> mutations = new ArrayList<>();

      for (ByteSequence row : rows)
        mutations.add(new Stats(row).toMutation());

      ArrayList<ByteSequence> rows2 = new ArrayList<>();
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
        tp.submit(new MutatorTask(tableName, conn, rows, cw, failed));
      }

      tp.shutdown();

      while (!tp.isTerminated()) {
        tp.awaitTermination(1, TimeUnit.MINUTES);
      }

      Assert.assertFalse("A MutatorTask failed with an exception", failed.get());
    }

    try (Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY)) {

      RowIterator rowIter = new RowIterator(scanner);

      while (rowIter.hasNext()) {
        Iterator<Entry<Key,Value>> row = rowIter.next();
        new Stats(row);
      }
    }
  }

  private SortedSet<Text> nss(String... splits) {
    TreeSet<Text> ret = new TreeSet<>();
    for (String split : splits)
      ret.add(new Text(split));

    return ret;
  }

  @Test
  public void testSecurity() throws Exception {
    // test against table user does not have read and/or write permissions for
    Connector conn = getConnector();
    String user = null;
    ClientConfiguration clientConf = cluster.getClientConfig();
    final boolean saslEnabled = clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false);

    // Create a new user
    ClusterUser user1 = getUser(0);
    user = user1.getPrincipal();
    if (saslEnabled) {
      conn.securityOperations().createLocalUser(user, null);
    } else {
      conn.securityOperations().createLocalUser(user, new PasswordToken(user1.getPassword()));
    }

    String[] tables = getUniqueNames(3);
    String table1 = tables[0], table2 = tables[1], table3 = tables[2];

    // Create three tables
    conn.tableOperations().create(table1);
    conn.tableOperations().create(table2);
    conn.tableOperations().create(table3);

    // Grant R on table1, W on table2, R/W on table3
    conn.securityOperations().grantTablePermission(user, table1, TablePermission.READ);
    conn.securityOperations().grantTablePermission(user, table2, TablePermission.WRITE);
    conn.securityOperations().grantTablePermission(user, table3, TablePermission.READ);
    conn.securityOperations().grantTablePermission(user, table3, TablePermission.WRITE);

    // Login as the user
    Connector conn2 = conn.getInstance().getConnector(user, user1.getToken());

    ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
    cm1.put("tx", "seq", "1");
    cm1.put("data", "x", "a");

    try (ConditionalWriter cw1 = conn2.createConditionalWriter(table1, new ConditionalWriterConfig());
        ConditionalWriter cw2 = conn2.createConditionalWriter(table2, new ConditionalWriterConfig());
        ConditionalWriter cw3 = conn2.createConditionalWriter(table3, new ConditionalWriterConfig())) {

      // Should be able to conditional-update a table we have R/W on
      Assert.assertEquals(Status.ACCEPTED, cw3.write(cm1).getStatus());

      // Conditional-update to a table we only have read on should fail
      try {
        Status status = cw1.write(cm1).getStatus();
        Assert.fail("Expected exception writing conditional mutation to table the user doesn't have write access to, Got status: " + status);
      } catch (AccumuloSecurityException ase) {

      }

      // Conditional-update to a table we only have writer on should fail
      try {
        Status status = cw2.write(cm1).getStatus();
        Assert.fail("Expected exception writing conditional mutation to table the user doesn't have read access to. Got status: " + status);
      } catch (AccumuloSecurityException ase) {

      }
    }
  }

  @Test
  public void testTimeout() throws Exception {
    Connector conn = getConnector();

    String table = getUniqueNames(1)[0];

    conn.tableOperations().create(table);

    try (ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig().setTimeout(3, TimeUnit.SECONDS))) {

      ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
      cm1.put("tx", "seq", "1");
      cm1.put("data", "x", "a");

      Assert.assertEquals(cw.write(cm1).getStatus(), Status.ACCEPTED);

      IteratorSetting is = new IteratorSetting(5, SlowIterator.class);
      SlowIterator.setSeekSleepTime(is, 5000);

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
          Assert.assertEquals("Unexpected value in tx:seq", "1", val);
        else if (cf.equals("data") && cq.equals("x"))
          Assert.assertEquals("Unexpected value in data:x", "a", val);
        else
          Assert.fail("Saw unexpected column family and qualifier: " + entry);
      }

      ConditionalMutation cm3 = new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
      cm3.put("tx", "seq", "2");
      cm3.put("data", "x", "b");

      Assert.assertEquals(cw.write(cm3).getStatus(), Status.ACCEPTED);
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    String table = getUniqueNames(1)[0];
    Connector conn = getConnector();

    try {
      conn.createConditionalWriter(table, new ConditionalWriterConfig());
      Assert.fail("Creating conditional writer for table that doesn't exist should fail");
    } catch (TableNotFoundException e) {}

    conn.tableOperations().create(table);

    try (ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig())) {

      conn.tableOperations().delete(table);

      ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
      cm1.put("tx", "seq", "1");
      cm1.put("data", "x", "a");

      Result result = cw.write(cm1);

      try {
        Status status = result.getStatus();
        Assert.fail("Expected exception writing conditional mutation to deleted table. Got status: " + status);
      } catch (AccumuloException ae) {
        Assert.assertEquals(TableDeletedException.class, ae.getCause().getClass());
      }
    }
  }

  @Test
  public void testOffline() throws Exception {
    String table = getUniqueNames(1)[0];
    Connector conn = getConnector();

    conn.tableOperations().create(table);

    try (ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig())) {

      conn.tableOperations().offline(table, true);

      ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
      cm1.put("tx", "seq", "1");
      cm1.put("data", "x", "a");

      Result result = cw.write(cm1);

      try {
        Status status = result.getStatus();
        Assert.fail("Expected exception writing conditional mutation to offline table. Got status: " + status);
      } catch (AccumuloException ae) {
        Assert.assertEquals(TableOfflineException.class, ae.getCause().getClass());
      }

      try {
        conn.createConditionalWriter(table, new ConditionalWriterConfig());
        Assert.fail("Expected exception creating conditional writer to offline table");
      } catch (TableOfflineException e) {}
    }
  }

  @Test
  public void testError() throws Exception {
    String table = getUniqueNames(1)[0];
    Connector conn = getConnector();

    conn.tableOperations().create(table);

    try (ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig())) {

      IteratorSetting iterSetting = new IteratorSetting(5, BadIterator.class);

      ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq").setIterators(iterSetting));
      cm1.put("tx", "seq", "1");
      cm1.put("data", "x", "a");

      Result result = cw.write(cm1);

      try {
        Status status = result.getStatus();
        Assert.fail("Expected exception using iterator which throws an error, Got status: " + status);
      } catch (AccumuloException ae) {

      }

    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoConditions() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    String table = getUniqueNames(1)[0];
    Connector conn = getConnector();

    conn.tableOperations().create(table);

    try (ConditionalWriter cw = conn.createConditionalWriter(table, new ConditionalWriterConfig())) {

      ConditionalMutation cm1 = new ConditionalMutation("r1");
      cm1.put("tx", "seq", "1");
      cm1.put("data", "x", "a");

      cw.write(cm1);
    }
  }

  @Test
  public void testTrace() throws Exception {
    // Need to add a getClientConfig() to AccumuloCluster
    Assume.assumeTrue(getClusterType() == ClusterType.MINI);
    Process tracer = null;
    Connector conn = getConnector();
    AccumuloCluster cluster = getCluster();
    MiniAccumuloClusterImpl mac = (MiniAccumuloClusterImpl) cluster;
    if (!conn.tableOperations().exists("trace")) {
      tracer = mac.exec(TraceServer.class);
      while (!conn.tableOperations().exists("trace")) {
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }

    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    DistributedTrace.enable("localhost", "testTrace", mac.getClientConfig());
    sleepUninterruptibly(1, TimeUnit.SECONDS);
    Span root = Trace.on("traceTest");
    try (ConditionalWriter cw = conn.createConditionalWriter(tableName, new ConditionalWriterConfig())) {

      // mutation conditional on column tx:seq not exiting
      ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq"));
      cm0.put("name", "last", "doe");
      cm0.put("name", "first", "john");
      cm0.put("tx", "seq", "1");
      Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
      root.stop();
    }

    final Scanner scanner = conn.createScanner("trace", Authorizations.EMPTY);
    scanner.setRange(new Range(new Text(Long.toHexString(root.traceId()))));
    loop: while (true) {
      final StringBuilder finalBuffer = new StringBuilder();
      int traceCount = TraceDump.printTrace(scanner, new Printer() {
        @Override
        public void print(final String line) {
          try {
            finalBuffer.append(line).append("\n");
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      });
      String traceOutput = finalBuffer.toString();
      log.info("Trace output:" + traceOutput);
      if (traceCount > 0) {
        int lastPos = 0;
        for (String part : "traceTest, startScan,startConditionalUpdate,conditionalUpdate,Check conditions,apply conditional mutations".split(",")) {
          log.info("Looking in trace output for '" + part + "'");
          int pos = traceOutput.indexOf(part);
          if (-1 == pos) {
            log.info("Trace output doesn't contain '" + part + "'");
            Thread.sleep(1000);
            break loop;
          }
          assertTrue("Did not find '" + part + "' in output", pos > 0);
          assertTrue("'" + part + "' occurred earlier than the previous element unexpectedly", pos > lastPos);
          lastPos = pos;
        }
        break;
      } else {
        log.info("Ignoring trace output as traceCount not greater than zero: " + traceCount);
        Thread.sleep(1000);
      }
    }
    if (tracer != null) {
      tracer.destroy();
    }
  }
}

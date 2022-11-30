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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
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
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.constraints.AlphaNumKeyConstraint;
import org.apache.accumulo.test.functional.BadIterator;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConditionalWriterIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(ConditionalWriterIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new Callback());
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static class Callback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Set the min span to 0 so we will definitely get all the traces back. See ACCUMULO-4365
      Map<String,String> siteConf = cfg.getSiteConfig();
      cfg.setSiteConfig(siteConf);
    }
  }

  public static long abs(long l) {
    l = Math.abs(l); // abs(Long.MIN_VALUE) == Long.MIN_VALUE...
    if (l < 0) {
      return 0;
    }
    return l;
  }

  @BeforeEach
  public void deleteUsers() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Set<String> users = client.securityOperations().listLocalUsers();
      ClusterUser user = getUser(0);
      if (users.contains(user.getPrincipal())) {
        client.securityOperations().dropLocalUser(user.getPrincipal());
      }
    }
  }

  @Test
  public void testBasic() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      try (ConditionalWriter cw = client.createConditionalWriter(tableName);
          Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {

        // mutation conditional on column tx:seq not existing
        ConditionalMutation cm0 = new ConditionalMutation("99006", new Condition("tx", "seq"));
        cm0.put("name", "last", "doe");
        cm0.put("name", "first", "john");
        cm0.put("tx", "seq", "1");
        assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
        assertEquals(Status.REJECTED, cw.write(cm0).getStatus());

        // mutation conditional on column tx:seq being 1
        ConditionalMutation cm1 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"));
        cm1.put("name", "last", "Doe");
        cm1.put("tx", "seq", "2");
        assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());

        // test condition where value differs
        ConditionalMutation cm2 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"));
        cm2.put("name", "last", "DOE");
        cm2.put("tx", "seq", "2");
        assertEquals(Status.REJECTED, cw.write(cm2).getStatus());

        // test condition where column does not exists
        ConditionalMutation cm3 =
            new ConditionalMutation("99006", new Condition("txtypo", "seq").setValue("1"));
        cm3.put("name", "last", "deo");
        cm3.put("tx", "seq", "2");
        assertEquals(Status.REJECTED, cw.write(cm3).getStatus());

        // test two conditions, where one should fail
        ConditionalMutation cm4 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("2"),
                new Condition("name", "last").setValue("doe"));
        cm4.put("name", "last", "deo");
        cm4.put("tx", "seq", "3");
        assertEquals(Status.REJECTED, cw.write(cm4).getStatus());

        // test two conditions, where one should fail
        ConditionalMutation cm5 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("1"),
                new Condition("name", "last").setValue("Doe"));
        cm5.put("name", "last", "deo");
        cm5.put("tx", "seq", "3");
        assertEquals(Status.REJECTED, cw.write(cm5).getStatus());

        // ensure rejected mutations did not write
        scanner.fetchColumn("name", "last");
        scanner.setRange(new Range("99006"));
        Entry<Key,Value> entry = getOnlyElement(scanner);
        assertEquals("Doe", entry.getValue().toString());

        // test w/ two conditions that are met
        ConditionalMutation cm6 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("2"),
                new Condition("name", "last").setValue("Doe"));
        cm6.put("name", "last", "DOE");
        cm6.put("tx", "seq", "3");
        assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

        entry = getOnlyElement(scanner);
        assertEquals("DOE", entry.getValue().toString());

        // test a conditional mutation that deletes
        ConditionalMutation cm7 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setValue("3"));
        cm7.putDelete("name", "last");
        cm7.putDelete("name", "first");
        cm7.putDelete("tx", "seq");
        assertEquals(Status.ACCEPTED, cw.write(cm7).getStatus());

        assertFalse(scanner.iterator().hasNext(), "Did not expect to find any results");

        // add the row back
        assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
        assertEquals(Status.REJECTED, cw.write(cm0).getStatus());

        entry = getOnlyElement(scanner);
        assertEquals("doe", entry.getValue().toString());
      }
    }
  }

  @Test
  public void testFields() throws Exception {

    try (AccumuloClient client1 = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      ClusterUser user1 = getUser(0);
      String user = user1.getPrincipal();
      if (saslEnabled()) {
        // The token is pointless for kerberos
        client1.securityOperations().createLocalUser(user, null);
      } else {
        client1.securityOperations().createLocalUser(user, new PasswordToken(user1.getPassword()));
      }

      Authorizations auths = new Authorizations("A", "B");

      client1.securityOperations().changeUserAuthorizations(user, auths);
      client1.securityOperations().grantSystemPermission(user, SystemPermission.CREATE_TABLE);

      try (AccumuloClient client2 =
          Accumulo.newClient().from(client1.properties()).as(user, user1.getToken()).build()) {
        client2.tableOperations().create(tableName);

        try (
            ConditionalWriter cw = client2.createConditionalWriter(tableName,
                new ConditionalWriterConfig().setAuthorizations(auths));
            Scanner scanner = client2.createScanner(tableName, auths)) {

          ColumnVisibility cva = new ColumnVisibility("A");
          ColumnVisibility cvb = new ColumnVisibility("B");

          ConditionalMutation cm0 =
              new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cva));
          cm0.put("name", "last", cva, "doe");
          cm0.put("name", "first", cva, "john");
          cm0.put("tx", "seq", cva, "1");
          assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());

          scanner.setRange(new Range("99006"));

          // verify we get the expected values that were written above
          Function<Scanner,Entry<Key,Value>> verifyValues = givenScanner -> {
            Entry<Key,Value> result;

            givenScanner.fetchColumn("name", "last");
            result = getOnlyElement(givenScanner);
            assertEquals("doe", result.getValue().toString());
            givenScanner.clearColumns();

            givenScanner.fetchColumn("name", "first");
            result = getOnlyElement(givenScanner);
            assertEquals("john", result.getValue().toString());
            givenScanner.clearColumns();

            givenScanner.fetchColumn("tx", "seq");
            result = getOnlyElement(givenScanner);
            assertEquals("1", result.getValue().toString());
            givenScanner.clearColumns();

            return result;
          };

          Entry<Key,Value> entry = verifyValues.apply(scanner);

          long ts = entry.getKey().getTimestamp();

          // test wrong colf
          ConditionalMutation cm1 = new ConditionalMutation("99006",
              new Condition("txA", "seq").setVisibility(cva).setValue("1"));
          cm1.put("name", "last", cva, "Doe");
          cm1.put("name", "first", cva, "John");
          cm1.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm1).getStatus());

          // test wrong colq
          ConditionalMutation cm2 = new ConditionalMutation("99006",
              new Condition("tx", "seqA").setVisibility(cva).setValue("1"));
          cm2.put("name", "last", cva, "Doe");
          cm2.put("name", "first", cva, "John");
          cm2.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm2).getStatus());

          // test wrong colv
          ConditionalMutation cm3 = new ConditionalMutation("99006",
              new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
          cm3.put("name", "last", cva, "Doe");
          cm3.put("name", "first", cva, "John");
          cm3.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm3).getStatus());

          // test wrong timestamp
          ConditionalMutation cm4 = new ConditionalMutation("99006",
              new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts + 1).setValue("1"));
          cm4.put("name", "last", cva, "Doe");
          cm4.put("name", "first", cva, "John");
          cm4.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm4).getStatus());

          // test wrong timestamp
          ConditionalMutation cm5 = new ConditionalMutation("99006",
              new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts - 1).setValue("1"));
          cm5.put("name", "last", cva, "Doe");
          cm5.put("name", "first", cva, "John");
          cm5.put("tx", "seq", cva, "2");
          assertEquals(Status.REJECTED, cw.write(cm5).getStatus());

          // ensure no updates were made
          var after = verifyValues.apply(scanner);
          assertEquals(ts, after.getKey().getTimestamp());

          // set all columns correctly
          ConditionalMutation cm6 = new ConditionalMutation("99006",
              new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts).setValue("1"));
          cm6.put("name", "last", cva, "Doe");
          cm6.put("name", "first", cva, "John");
          cm6.put("tx", "seq", cva, "2");
          assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

          scanner.fetchColumn("tx", "seq");
          entry = getOnlyElement(scanner);
          assertEquals("2", entry.getValue().toString());
        }
      }
    }
  }

  @Test
  public void testBadColVis() throws Exception {
    // test when a user sets a col vis in a condition that can never be seen

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      Authorizations auths = new Authorizations("A", "B");

      client.securityOperations().changeUserAuthorizations(getAdminPrincipal(), auths);

      Authorizations filteredAuths = new Authorizations("A");

      ColumnVisibility cva = new ColumnVisibility("A");
      ColumnVisibility cvb = new ColumnVisibility("B");
      ColumnVisibility cvc = new ColumnVisibility("C");

      try (ConditionalWriter cw = client.createConditionalWriter(tableName,
          new ConditionalWriterConfig().setAuthorizations(filteredAuths))) {

        // User has authorization, but didn't include it in the writer
        ConditionalMutation cm0 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb));
        cm0.put("name", "last", cva, "doe");
        cm0.put("name", "first", cva, "john");
        cm0.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm0).getStatus());

        ConditionalMutation cm1 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
        cm1.put("name", "last", cva, "doe");
        cm1.put("name", "first", cva, "john");
        cm1.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm1).getStatus());

        // User does not have the authorization
        ConditionalMutation cm2 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvc));
        cm2.put("name", "last", cva, "doe");
        cm2.put("name", "first", cva, "john");
        cm2.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm2).getStatus());

        ConditionalMutation cm3 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvc).setValue("1"));
        cm3.put("name", "last", cva, "doe");
        cm3.put("name", "first", cva, "john");
        cm3.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm3).getStatus());

        // if any visibility is bad, good visibilities don't override
        ConditionalMutation cm4 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb),
                new Condition("tx", "seq").setVisibility(cva));

        cm4.put("name", "last", cva, "doe");
        cm4.put("name", "first", cva, "john");
        cm4.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm4).getStatus());

        ConditionalMutation cm5 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvb).setValue("1"),
            new Condition("tx", "seq").setVisibility(cva).setValue("1"));
        cm5.put("name", "last", cva, "doe");
        cm5.put("name", "first", cva, "john");
        cm5.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm5).getStatus());

        ConditionalMutation cm6 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvb).setValue("1"),
            new Condition("tx", "seq").setVisibility(cva));
        cm6.put("name", "last", cva, "doe");
        cm6.put("name", "first", cva, "john");
        cm6.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm6).getStatus());

        ConditionalMutation cm7 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb),
                new Condition("tx", "seq").setVisibility(cva).setValue("1"));
        cm7.put("name", "last", cva, "doe");
        cm7.put("name", "first", cva, "john");
        cm7.put("tx", "seq", cva, "1");
        assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm7).getStatus());

      }

      // test passing auths that exceed users configured auths

      Authorizations exceedingAuths = new Authorizations("A", "B", "D");
      try (ConditionalWriter cw2 = client.createConditionalWriter(tableName,
          new ConditionalWriterConfig().setAuthorizations(exceedingAuths))) {

        ConditionalMutation cm8 =
            new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvb),
                new Condition("tx", "seq").setVisibility(cva).setValue("1"));
        cm8.put("name", "last", cva, "doe");
        cm8.put("name", "first", cva, "john");
        cm8.put("tx", "seq", cva, "1");

        assertThrows(AccumuloSecurityException.class, () -> {
          Status status = cw2.write(cm8).getStatus();
          log.error("Writing mutation with Authorizations the user doesn't have should fail. Got "
              + "status: {}", status);
        });
      }
    }
  }

  @Test
  public void testConstraints() throws Exception {
    // ensure constraint violations are properly reported

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      client.tableOperations().addConstraint(tableName, AlphaNumKeyConstraint.class.getName());
      client.tableOperations().clone(tableName, tableName + "_clone", true, new HashMap<>(),
          new HashSet<>());

      try (ConditionalWriter cw = client.createConditionalWriter(tableName + "_clone");
          Scanner scanner = client.createScanner(tableName + "_clone", new Authorizations())) {

        ConditionalMutation cm0 = new ConditionalMutation("99006+", new Condition("tx", "seq"));
        cm0.put("tx", "seq", "1");

        assertEquals(Status.VIOLATED, cw.write(cm0).getStatus());
        assertFalse(scanner.iterator().hasNext(),
            "Should find no results in the table is mutation result was violated");

        ConditionalMutation cm1 = new ConditionalMutation("99006", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");

        assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
        assertTrue(scanner.iterator().hasNext(),
            "Accepted result should be returned when reading table");
      }
    }
  }

  @Test
  public void testIterators() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName,
          new NewTableConfiguration().withoutDefaultIterators());

      try (BatchWriter bw = client.createBatchWriter(tableName)) {

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
      }

      IteratorSetting iterConfig = new IteratorSetting(10, SummingCombiner.class);
      SummingCombiner.setEncodingType(iterConfig, Type.STRING);
      SummingCombiner.setColumns(iterConfig,
          Collections.singletonList(new IteratorSetting.Column("count")));

      IteratorSetting iterConfig2 = new IteratorSetting(10, SummingCombiner.class);
      SummingCombiner.setEncodingType(iterConfig2, Type.STRING);
      SummingCombiner.setColumns(iterConfig2,
          Collections.singletonList(new IteratorSetting.Column("count2", "comments")));

      IteratorSetting iterConfig3 = new IteratorSetting(5, VersioningIterator.class);
      VersioningIterator.setMaxVersions(iterConfig3, 1);

      try (Scanner scanner = client.createScanner(tableName, new Authorizations())) {
        scanner.addScanIterator(iterConfig);
        scanner.setRange(new Range("ACCUMULO-1000"));
        scanner.fetchColumn("count", "comments");

        Entry<Key,Value> entry = getOnlyElement(scanner);
        assertEquals("3", entry.getValue().toString());

        try (ConditionalWriter cw = client.createConditionalWriter(tableName)) {

          ConditionalMutation cm0 = new ConditionalMutation("ACCUMULO-1000",
              new Condition("count", "comments").setValue("3"));
          cm0.put("count", "comments", "1");
          assertEquals(Status.REJECTED, cw.write(cm0).getStatus());
          entry = getOnlyElement(scanner);
          assertEquals("3", entry.getValue().toString());

          ConditionalMutation cm1 = new ConditionalMutation("ACCUMULO-1000",
              new Condition("count", "comments").setIterators(iterConfig).setValue("3"));
          cm1.put("count", "comments", "1");
          assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
          entry = getOnlyElement(scanner);
          assertEquals("4", entry.getValue().toString());

          ConditionalMutation cm2 = new ConditionalMutation("ACCUMULO-1000",
              new Condition("count", "comments").setValue("4"));
          cm2.put("count", "comments", "1");
          assertEquals(Status.REJECTED, cw.write(cm1).getStatus());
          entry = getOnlyElement(scanner);
          assertEquals("4", entry.getValue().toString());

          // run test with multiple iterators passed in same batch and condition with two iterators

          ConditionalMutation cm3 = new ConditionalMutation("ACCUMULO-1000",
              new Condition("count", "comments").setIterators(iterConfig).setValue("4"));
          cm3.put("count", "comments", "1");

          ConditionalMutation cm4 = new ConditionalMutation("ACCUMULO-1001",
              new Condition("count2", "comments").setIterators(iterConfig2).setValue("2"));
          cm4.put("count2", "comments", "1");

          ConditionalMutation cm5 =
              new ConditionalMutation("ACCUMULO-1002", new Condition("count2", "comments")
                  .setIterators(iterConfig2, iterConfig3).setValue("2"));
          cm5.put("count2", "comments", "1");

          Iterator<Result> results = cw.write(Arrays.asList(cm3, cm4, cm5).iterator());
          Map<String,Status> actual = new HashMap<>();

          while (results.hasNext()) {
            Result result = results.next();
            String k = new String(result.getMutation().getRow());
            assertFalse(actual.containsKey(k),
                "Did not expect to see multiple results for the row: " + k);
            actual.put(k, result.getStatus());
          }

          Map<String,Status> expected = new HashMap<>();
          expected.put("ACCUMULO-1000", Status.ACCEPTED);
          expected.put("ACCUMULO-1001", Status.ACCEPTED);
          expected.put("ACCUMULO-1002", Status.REJECTED);

          assertEquals(expected, actual);
        }
      }
    }
  }

  public static class AddingIterator extends WrappingIterator {
    long amount = 0;

    @Override
    public Value getTopValue() {
      Value val = super.getTopValue();
      long l = Long.parseLong(val.toString());
      String newVal = (l + amount) + "";
      return new Value(newVal);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) {
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
      return new Value(newVal);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) {
      this.setSource(source);
      amount = Long.parseLong(options.get("amount"));
    }
  }

  @Test
  public void testTableAndConditionIterators() throws Exception {

    // test w/ table that has iterators configured
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      IteratorSetting aiConfig1 = new IteratorSetting(30, "AI1", AddingIterator.class);
      aiConfig1.addOption("amount", "2");
      IteratorSetting aiConfig2 = new IteratorSetting(35, "MI1", MultiplyingIterator.class);
      aiConfig2.addOption("amount", "3");
      IteratorSetting aiConfig3 = new IteratorSetting(40, "AI2", AddingIterator.class);
      aiConfig3.addOption("amount", "5");

      client.tableOperations().create(tableName);

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        Mutation m = new Mutation("ACCUMULO-1000");
        m.put("count", "comments", "6");
        bw.addMutation(m);

        m = new Mutation("ACCUMULO-1001");
        m.put("count", "comments", "7");
        bw.addMutation(m);

        m = new Mutation("ACCUMULO-1002");
        m.put("count", "comments", "8");
        bw.addMutation(m);
      }

      client.tableOperations().attachIterator(tableName, aiConfig1, EnumSet.of(IteratorScope.scan));
      client.tableOperations().offline(tableName, true);
      client.tableOperations().online(tableName, true);

      try (ConditionalWriter cw = client.createConditionalWriter(tableName);
          Scanner scanner = client.createScanner(tableName, new Authorizations())) {

        ConditionalMutation cm6 = new ConditionalMutation("ACCUMULO-1000",
            new Condition("count", "comments").setValue("8"));
        cm6.put("count", "comments", "7");
        assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

        scanner.setRange(new Range("ACCUMULO-1000"));
        scanner.fetchColumn("count", "comments");

        Entry<Key,Value> entry = getOnlyElement(scanner);
        assertEquals("9", entry.getValue().toString());

        ConditionalMutation cm7 = new ConditionalMutation("ACCUMULO-1000",
            new Condition("count", "comments").setIterators(aiConfig2).setValue("27"));
        cm7.put("count", "comments", "8");
        assertEquals(Status.ACCEPTED, cw.write(cm7).getStatus());

        entry = getOnlyElement(scanner);
        assertEquals("10", entry.getValue().toString());

        ConditionalMutation cm8 = new ConditionalMutation("ACCUMULO-1000",
            new Condition("count", "comments").setIterators(aiConfig2, aiConfig3).setValue("35"));
        cm8.put("count", "comments", "9");
        assertEquals(Status.ACCEPTED, cw.write(cm8).getStatus());

        entry = getOnlyElement(scanner);
        assertEquals("11", entry.getValue().toString());

        ConditionalMutation cm3 = new ConditionalMutation("ACCUMULO-1000",
            new Condition("count", "comments").setIterators(aiConfig2).setValue("33"));
        cm3.put("count", "comments", "3");

        ConditionalMutation cm4 = new ConditionalMutation("ACCUMULO-1001",
            new Condition("count", "comments").setIterators(aiConfig3).setValue("14"));
        cm4.put("count", "comments", "3");

        ConditionalMutation cm5 = new ConditionalMutation("ACCUMULO-1002",
            new Condition("count", "comments").setIterators(aiConfig3).setValue("10"));
        cm5.put("count", "comments", "3");

        Iterator<Result> results = cw.write(Arrays.asList(cm3, cm4, cm5).iterator());
        Map<String,Status> actual = new HashMap<>();

        while (results.hasNext()) {
          Result result = results.next();
          String k = new String(result.getMutation().getRow());
          assertFalse(actual.containsKey(k),
              "Did not expect to see multiple results for the row: " + k);
          actual.put(k, result.getStatus());
        }

        Map<String,Status> expected = new HashMap<>();
        expected.put("ACCUMULO-1000", Status.ACCEPTED);
        expected.put("ACCUMULO-1001", Status.ACCEPTED);
        expected.put("ACCUMULO-1002", Status.REJECTED);
        assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void testBatch() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      client.securityOperations().changeUserAuthorizations(getAdminPrincipal(),
          new Authorizations("A", "B"));

      ColumnVisibility cvab = new ColumnVisibility("A|B");

      ArrayList<ConditionalMutation> mutations = new ArrayList<>();

      ConditionalMutation cm0 =
          new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvab));
      cm0.put("name", "last", cvab, "doe");
      cm0.put("name", "first", cvab, "john");
      cm0.put("tx", "seq", cvab, "1");
      mutations.add(cm0);

      ConditionalMutation cm1 =
          new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvab));
      cm1.put("name", "last", cvab, "doe");
      cm1.put("name", "first", cvab, "jane");
      cm1.put("tx", "seq", cvab, "1");
      mutations.add(cm1);

      ConditionalMutation cm2 =
          new ConditionalMutation("19059", new Condition("tx", "seq").setVisibility(cvab));
      cm2.put("name", "last", cvab, "doe");
      cm2.put("name", "first", cvab, "jack");
      cm2.put("tx", "seq", cvab, "1");
      mutations.add(cm2);

      try (
          ConditionalWriter cw = client.createConditionalWriter(tableName,
              new ConditionalWriterConfig().setAuthorizations(new Authorizations("A")));
          Scanner scanner = client.createScanner(tableName, new Authorizations("A"))) {
        Iterator<Result> results = cw.write(mutations.iterator());
        int count = 0;
        while (results.hasNext()) {
          Result result = results.next();
          assertEquals(Status.ACCEPTED, result.getStatus());
          count++;
        }

        assertEquals(3, count);

        scanner.fetchColumn("tx", "seq");

        for (String row : new String[] {"99006", "59056", "19059"}) {
          scanner.setRange(new Range(row));
          Entry<Key,Value> entry = getOnlyElement(scanner);
          assertEquals("1", entry.getValue().toString());
        }

        TreeSet<Text> splits = new TreeSet<>();
        splits.add(new Text("7"));
        splits.add(new Text("3"));
        client.tableOperations().addSplits(tableName, splits);

        mutations.clear();

        ConditionalMutation cm3 = new ConditionalMutation("99006",
            new Condition("tx", "seq").setVisibility(cvab).setValue("1"));
        cm3.put("name", "last", cvab, "Doe");
        cm3.put("tx", "seq", cvab, "2");
        mutations.add(cm3);

        ConditionalMutation cm4 =
            new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvab));
        cm4.put("name", "last", cvab, "Doe");
        cm4.put("tx", "seq", cvab, "1");
        mutations.add(cm4);

        ConditionalMutation cm5 = new ConditionalMutation("19059",
            new Condition("tx", "seq").setVisibility(cvab).setValue("2"));
        cm5.put("name", "last", cvab, "Doe");
        cm5.put("tx", "seq", cvab, "3");
        mutations.add(cm5);

        results = cw.write(mutations.iterator());
        int accepted = 0;
        int rejected = 0;
        while (results.hasNext()) {
          Result result = results.next();
          if (new String(result.getMutation().getRow()).equals("99006")) {
            assertEquals(Status.ACCEPTED, result.getStatus());
            accepted++;
          } else {
            assertEquals(Status.REJECTED, result.getStatus());
            rejected++;
          }
        }

        assertEquals(1, accepted, "Expected only one accepted conditional mutation");
        assertEquals(2, rejected, "Expected two rejected conditional mutations");

        for (String row : new String[] {"59056", "19059"}) {
          scanner.setRange(new Range(row));
          Entry<Key,Value> entry = getOnlyElement(scanner);
          assertEquals("1", entry.getValue().toString());
        }

        scanner.setRange(new Range("99006"));
        Entry<Key,Value> entry = getOnlyElement(scanner);
        assertEquals("2", entry.getValue().toString());

        scanner.clearColumns();
        scanner.fetchColumn("name", "last");
        entry = getOnlyElement(scanner);
        assertEquals("Doe", entry.getValue().toString());
      }
    }
  }

  @Test
  public void testBigBatch() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(nss("2", "4", "6"));
      client.tableOperations().create(tableName, ntc);

      sleepUninterruptibly(2, TimeUnit.SECONDS);

      int num = 100;

      ArrayList<byte[]> rows = new ArrayList<>(num);
      ArrayList<ConditionalMutation> cml = new ArrayList<>(num);

      byte[] e = new byte[0];

      for (int i = 0; i < num; i++) {
        rows.add(FastFormat.toZeroPaddedString(abs(random.nextLong()), 16, 16, e));
      }

      for (int i = 0; i < num; i++) {
        ConditionalMutation cm = new ConditionalMutation(rows.get(i), new Condition("meta", "seq"));

        cm.put("meta", "seq", "1");
        cm.put("meta", "tx", UUID.randomUUID().toString());

        cml.add(cm);
      }
      try (ConditionalWriter cw = client.createConditionalWriter(tableName)) {

        Iterator<Result> results = cw.write(cml.iterator());

        int count = 0;

        Set<String> rowsReceived = new HashSet<>();
        while (results.hasNext()) {
          Result result = results.next();
          rowsReceived.add(new String(result.getMutation().getRow(), UTF_8));
          assertEquals(Status.ACCEPTED, result.getStatus());
          count++;
        }

        assertEquals(num, count, "Did not receive the expected number of results");

        Set<String> rowsExpected =
            rows.stream().map(row -> new String(row, UTF_8)).collect(Collectors.toSet());
        assertEquals(rowsExpected, rowsReceived, "Did not receive all expected rows");

        ArrayList<ConditionalMutation> cml2 = new ArrayList<>(num);

        for (int i = 0; i < num; i++) {
          ConditionalMutation cm =
              new ConditionalMutation(rows.get(i), new Condition("meta", "seq").setValue("1"));

          cm.put("meta", "seq", "2");
          cm.put("meta", "tx", UUID.randomUUID().toString());

          cml2.add(cm);
        }

        count = 0;

        results = cw.write(cml2.iterator());

        while (results.hasNext()) {
          Result result = results.next();
          assertEquals(Status.ACCEPTED, result.getStatus());
          count++;
        }

        assertEquals(num, count, "Did not receive the expected number of results");
      }
    }
  }

  @Test
  public void testBatchErrors() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      client.tableOperations().addConstraint(tableName, AlphaNumKeyConstraint.class.getName());
      client.tableOperations().clone(tableName, tableName + "_clone", true, new HashMap<>(),
          new HashSet<>());

      client.securityOperations().changeUserAuthorizations(getAdminPrincipal(),
          new Authorizations("A", "B"));

      ColumnVisibility cvaob = new ColumnVisibility("A|B");
      ColumnVisibility cvaab = new ColumnVisibility("A&B");

      switch (random.nextInt(3)) {
        case 1:
          client.tableOperations().addSplits(tableName, nss("6"));
          break;
        case 2:
          client.tableOperations().addSplits(tableName, nss("2", "95"));
          break;
      }

      ArrayList<ConditionalMutation> mutations = new ArrayList<>();

      ConditionalMutation cm0 =
          new ConditionalMutation("99006", new Condition("tx", "seq").setVisibility(cvaob));
      cm0.put("name+", "last", cvaob, "doe");
      cm0.put("name", "first", cvaob, "john");
      cm0.put("tx", "seq", cvaob, "1");
      mutations.add(cm0);

      ConditionalMutation cm1 =
          new ConditionalMutation("59056", new Condition("tx", "seq").setVisibility(cvaab));
      cm1.put("name", "last", cvaab, "doe");
      cm1.put("name", "first", cvaab, "jane");
      cm1.put("tx", "seq", cvaab, "1");
      mutations.add(cm1);

      ConditionalMutation cm2 =
          new ConditionalMutation("19059", new Condition("tx", "seq").setVisibility(cvaob));
      cm2.put("name", "last", cvaob, "doe");
      cm2.put("name", "first", cvaob, "jack");
      cm2.put("tx", "seq", cvaob, "1");
      mutations.add(cm2);

      ConditionalMutation cm3 = new ConditionalMutation("90909",
          new Condition("tx", "seq").setVisibility(cvaob).setValue("1"));
      cm3.put("name", "last", cvaob, "doe");
      cm3.put("name", "first", cvaob, "john");
      cm3.put("tx", "seq", cvaob, "2");
      mutations.add(cm3);

      try (
          ConditionalWriter cw = client.createConditionalWriter(tableName,
              new ConditionalWriterConfig().setAuthorizations(new Authorizations("A")));
          Scanner scanner = client.createScanner(tableName, new Authorizations("A"))) {
        Iterator<Result> results = cw.write(mutations.iterator());
        HashSet<String> rows = new HashSet<>();
        while (results.hasNext()) {
          Result result = results.next();
          String row = new String(result.getMutation().getRow());
          switch (row) {
            case "19059":
              assertEquals(Status.ACCEPTED, result.getStatus());
              break;
            case "59056":
              assertEquals(Status.INVISIBLE_VISIBILITY, result.getStatus());
              break;
            case "99006":
              assertEquals(Status.VIOLATED, result.getStatus());
              break;
            case "90909":
              assertEquals(Status.REJECTED, result.getStatus());
              break;
          }
          rows.add(row);
        }

        assertEquals(4, rows.size());

        scanner.fetchColumn("tx", "seq");
        Entry<Key,Value> entry = getOnlyElement(scanner);
        assertEquals("1", entry.getValue().toString());
      }
    }
  }

  @Test
  public void testSameRow() throws Exception {
    // test multiple mutations for same row in same batch

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      try (ConditionalWriter cw = client.createConditionalWriter(tableName)) {

        ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());

        ConditionalMutation cm2 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
        cm2.put("tx", "seq", "2");
        cm2.put("data", "x", "b");

        ConditionalMutation cm3 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
        cm3.put("tx", "seq", "2");
        cm3.put("data", "x", "c");

        ConditionalMutation cm4 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
        cm4.put("tx", "seq", "2");
        cm4.put("data", "x", "d");

        Iterator<Result> results = cw.write(Arrays.asList(cm2, cm3, cm4).iterator());

        int accepted = 0;
        int rejected = 0;
        int total = 0;

        while (results.hasNext()) {
          Status status = results.next().getStatus();
          if (status == Status.ACCEPTED) {
            accepted++;
          }
          if (status == Status.REJECTED) {
            rejected++;
          }
          total++;
        }

        assertEquals(1, accepted, "Expected one accepted result");
        assertEquals(2, rejected, "Expected two rejected results");
        assertEquals(3, total, "Expected three total results");
      }
    }
  }

  private static class Stats {

    ByteSequence row = null;
    int seq;
    long sum;
    int[] data = new int[10];

    public Stats(Iterator<Entry<Key,Value>> iterator) {
      while (iterator.hasNext()) {
        Entry<Key,Value> entry = iterator.next();

        if (row == null) {
          row = entry.getKey().getRowData();
        }

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

      assertEquals(sum2, sum);
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
      if (seq >= 0) {
        cond.setValue(seq + "");
      }

      ConditionalMutation cm = new ConditionalMutation(row, cond);

      cm.put("meta", "seq", (seq + 1) + "");
      cm.put("meta", "sum", sum + "");

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
    AccumuloClient client;
    AtomicBoolean failed;

    public MutatorTask(String tableName, AccumuloClient client, ArrayList<ByteSequence> rows,
        ConditionalWriter cw, AtomicBoolean failed) {
      this.tableName = tableName;
      this.rows = rows;
      this.client = client;
      this.cw = cw;
      this.failed = failed;
    }

    @Override
    public void run() {
      try (Scanner scanner =
          new IsolatedScanner(client.createScanner(tableName, Authorizations.EMPTY))) {

        for (int i = 0; i < 20; i++) {
          int numRows = random.nextInt(10) + 1;

          ArrayList<ByteSequence> changes = new ArrayList<>(numRows);
          ArrayList<ConditionalMutation> mutations = new ArrayList<>();

          for (int j = 0; j < numRows; j++) {
            changes.add(rows.get(random.nextInt(rows.size())));
          }

          for (ByteSequence row : changes) {
            scanner.setRange(new Range(row.toString()));
            Stats stats = new Stats(scanner.iterator());
            stats.set(random.nextInt(10), random.nextInt(Integer.MAX_VALUE));
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

          assertEquals(changes, changed);
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
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      NewTableConfiguration ntc = new NewTableConfiguration();

      switch (random.nextInt(3)) {
        case 1:
          ntc = ntc.withSplits(nss("4"));
          break;
        case 2:
          ntc = ntc.withSplits(nss("3", "5"));
          break;
      }

      client.tableOperations().create(tableName, ntc);

      try (ConditionalWriter cw = client.createConditionalWriter(tableName)) {

        ArrayList<ByteSequence> rows = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
          rows.add(new ArrayByteSequence(
              FastFormat.toZeroPaddedString(abs(random.nextLong()), 16, 16, new byte[0])));
        }

        ArrayList<ConditionalMutation> mutations = new ArrayList<>();

        for (ByteSequence row : rows) {
          mutations.add(new Stats(row).toMutation());
        }

        ArrayList<ByteSequence> rows2 = new ArrayList<>();
        Iterator<Result> results = cw.write(mutations.iterator());
        while (results.hasNext()) {
          Result result = results.next();
          assertEquals(Status.ACCEPTED, result.getStatus());
          rows2.add(new ArrayByteSequence(result.getMutation().getRow()));
        }

        Collections.sort(rows);
        Collections.sort(rows2);

        assertEquals(rows, rows2);

        AtomicBoolean failed = new AtomicBoolean(false);

        ExecutorService tp = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
          tp.execute(new MutatorTask(tableName, client, rows, cw, failed));
        }

        tp.shutdown();

        while (!tp.isTerminated()) {
          tp.awaitTermination(1, TimeUnit.MINUTES);
        }

        assertFalse(failed.get(), "A MutatorTask failed with an exception");
      }

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        RowIterator rowIter = new RowIterator(scanner);

        while (rowIter.hasNext()) {
          Iterator<Entry<Key,Value>> row = rowIter.next();
          new Stats(row);
        }
      }
    }
  }

  private SortedSet<Text> nss(String... splits) {
    TreeSet<Text> ret = new TreeSet<>();
    for (String split : splits) {
      ret.add(new Text(split));
    }

    return ret;
  }

  @Test
  public void testSecurity() throws Exception {
    // test against table user does not have read and/or write permissions for
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      // Create a new user
      ClusterUser user1 = getUser(0);
      String user = user1.getPrincipal();
      if (saslEnabled()) {
        client.securityOperations().createLocalUser(user, null);
      } else {
        client.securityOperations().createLocalUser(user, new PasswordToken(user1.getPassword()));
      }

      String[] tables = getUniqueNames(3);
      String table1 = tables[0], table2 = tables[1], table3 = tables[2];

      // Create three tables
      client.tableOperations().create(table1);
      client.tableOperations().create(table2);
      client.tableOperations().create(table3);

      // Grant R on table1, W on table2, R/W on table3
      client.securityOperations().grantTablePermission(user, table1, TablePermission.READ);
      client.securityOperations().grantTablePermission(user, table2, TablePermission.WRITE);
      client.securityOperations().grantTablePermission(user, table3, TablePermission.READ);
      client.securityOperations().grantTablePermission(user, table3, TablePermission.WRITE);

      ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
      cm1.put("tx", "seq", "1");
      cm1.put("data", "x", "a");

      try (
          AccumuloClient client2 =
              Accumulo.newClient().from(client.properties()).as(user, user1.getToken()).build();
          ConditionalWriter cw1 = client2.createConditionalWriter(table1);
          ConditionalWriter cw2 = client2.createConditionalWriter(table2);
          ConditionalWriter cw3 = client2.createConditionalWriter(table3)) {

        // Should be able to conditional-update a table we have R/W on
        assertEquals(Status.ACCEPTED, cw3.write(cm1).getStatus());

        // Conditional-update to a table we only have read on should fail
        assertThrows(AccumuloSecurityException.class, () -> {
          Status status = cw1.write(cm1).getStatus();
          log.error("Expected exception writing conditional mutation to table the user doesn't "
              + "have write access to, Got status: {}", status);
        });

        // Conditional-update to a table we only have writer on should fail
        assertThrows(AccumuloSecurityException.class, () -> {
          Status status = cw2.write(cm1).getStatus();
          log.error(
              "Expected exception writing conditional mutation to table the user doesn't have read access to. Got status: {}",
              status);
        });
      }
    }
  }

  @Test
  public void testTimeout() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String table = getUniqueNames(1)[0];

      client.tableOperations().create(table);

      try (
          ConditionalWriter cw = client.createConditionalWriter(table,
              new ConditionalWriterConfig().setTimeout(3, TimeUnit.SECONDS));
          Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {

        ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        assertEquals(cw.write(cm1).getStatus(), Status.ACCEPTED);

        IteratorSetting is = new IteratorSetting(5, SlowIterator.class);
        SlowIterator.setSeekSleepTime(is, 5000);

        ConditionalMutation cm2 = new ConditionalMutation("r1",
            new Condition("tx", "seq").setValue("1").setIterators(is));
        cm2.put("tx", "seq", "2");
        cm2.put("data", "x", "b");

        assertEquals(cw.write(cm2).getStatus(), Status.UNKNOWN);

        for (Entry<Key,Value> entry : scanner) {
          String cf = entry.getKey().getColumnFamilyData().toString();
          String cq = entry.getKey().getColumnQualifierData().toString();
          String val = entry.getValue().toString();

          if (cf.equals("tx") && cq.equals("seq")) {
            assertEquals("1", val, "Unexpected value in tx:seq");
          } else if (cf.equals("data") && cq.equals("x")) {
            assertEquals("a", val, "Unexpected value in data:x");
          } else {
            fail("Saw unexpected column family and qualifier: " + entry);
          }
        }

        ConditionalMutation cm3 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setValue("1"));
        cm3.put("tx", "seq", "2");
        cm3.put("data", "x", "b");

        assertEquals(cw.write(cm3).getStatus(), Status.ACCEPTED);
      }
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      assertThrows(TableNotFoundException.class, () -> client.createConditionalWriter(table),
          "Creating conditional writer for table that doesn't exist should fail");

      client.tableOperations().create(table);

      try (ConditionalWriter cw = client.createConditionalWriter(table)) {

        client.tableOperations().delete(table);

        ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        Result result = cw.write(cm1);

        var ae = assertThrows(AccumuloException.class, () -> {
          Status status = result.getStatus();
          log.error(
              "Expected exception writing conditional mutation to deleted table. Got status: {}",
              status);
        });
        assertSame(TableDeletedException.class, ae.getCause().getClass());
      }
    }
  }

  @Test
  public void testOffline() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      try (ConditionalWriter cw = client.createConditionalWriter(table)) {

        client.tableOperations().offline(table, true);

        ConditionalMutation cm1 = new ConditionalMutation("r1", new Condition("tx", "seq"));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        Result result = cw.write(cm1);

        var ae = assertThrows(AccumuloException.class, () -> {
          Status status = result.getStatus();
          log.error("Expected exception writing conditional mutation to offline table. Got "
              + "status: {}", status);
        });
        assertSame(TableOfflineException.class, ae.getCause().getClass());

        assertThrows(TableOfflineException.class, () -> client.createConditionalWriter(table),
            "Expected exception creating conditional writer to offline table");
      }
    }
  }

  @Test
  public void testError() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      try (ConditionalWriter cw = client.createConditionalWriter(table)) {

        IteratorSetting iterSetting = new IteratorSetting(5, BadIterator.class);

        ConditionalMutation cm1 =
            new ConditionalMutation("r1", new Condition("tx", "seq").setIterators(iterSetting));
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        Result result = cw.write(cm1);

        assertThrows(AccumuloException.class, () -> {
          Status status = result.getStatus();
          log.error("Expected exception using iterator which throws an error, Got status: {}",
              status);
        });

      }
    }
  }

  @Test
  public void testNoConditions() throws AccumuloException, AccumuloSecurityException,
      TableExistsException, TableNotFoundException {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      try (ConditionalWriter cw = client.createConditionalWriter(table)) {

        ConditionalMutation cm1 = new ConditionalMutation("r1");
        cm1.put("tx", "seq", "1");
        cm1.put("data", "x", "a");

        assertThrows(IllegalArgumentException.class, () -> cw.write(cm1));
      }
    }
  }

}

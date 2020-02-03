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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint;
import org.apache.accumulo.examples.simple.constraints.NumericValueConstraint;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstraintIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(ConstraintIT.class);

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void run() throws Exception {
    String[] tableNames = getUniqueNames(3);
    Connector c = getConnector();
    for (String table : tableNames) {
      c.tableOperations().create(table);
      c.tableOperations().addConstraint(table, NumericValueConstraint.class.getName());
      c.tableOperations().addConstraint(table, AlphaNumKeyConstraint.class.getName());
    }

    // A static sleep to just let ZK do its thing
    Thread.sleep(10 * 1000);

    // Then check that the client has at least gotten the updates
    for (String table : tableNames) {
      log.debug("Checking constraints on {}", table);
      Map<String,Integer> constraints = c.tableOperations().listConstraints(table);
      while (!constraints.containsKey(NumericValueConstraint.class.getName())
          || !constraints.containsKey(AlphaNumKeyConstraint.class.getName())) {
        log.debug("Failed to verify constraints. Sleeping and retrying");
        Thread.sleep(2000);
        constraints = c.tableOperations().listConstraints(table);
      }
      log.debug("Verified all constraints on {}", table);
    }

    log.debug("Verified constraints on all tables. Running tests");

    test1(tableNames[0]);

    test2(tableNames[1], false);
    test2(tableNames[2], true);
  }

  private void test1(String tableName) throws Exception {
    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    Mutation mut1 = new Mutation(new Text("r1"));
    mut1.put(new Text("cf1"), new Text("cq1"), new Value("123".getBytes(UTF_8)));

    bw.addMutation(mut1);

    // should not throw any exceptions
    bw.close();

    bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    // create a mutation with a non numeric value
    Mutation mut2 = new Mutation(new Text("r1"));
    mut2.put(new Text("cf1"), new Text("cq1"), new Value("123a".getBytes(UTF_8)));

    bw.addMutation(mut2);

    boolean sawMRE = false;

    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, constraint did not catch bad mutation");
    } catch (MutationsRejectedException mre) {
      sawMRE = true;

      // verify constraint violation summary
      List<ConstraintViolationSummary> cvsl = mre.getConstraintViolationSummaries();

      if (cvsl.size() != 1) {
        throw new Exception("Unexpected constraints");
      }

      for (ConstraintViolationSummary cvs : cvsl) {
        if (!cvs.constrainClass.equals(NumericValueConstraint.class.getName())) {
          throw new Exception("Unexpected constraint class " + cvs.constrainClass);
        }

        if (cvs.numberOfViolatingMutations != 1) {
          throw new Exception("Unexpected # violating mutations " + cvs.numberOfViolatingMutations);
        }
      }
    }

    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }

    // verify mutation did not go through
    Scanner scanner = getConnector().createScanner(tableName, Authorizations.EMPTY);
    scanner.setRange(new Range(new Text("r1")));

    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    Entry<Key,Value> entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1"))
        || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1"))
        || !entry.getValue().equals(new Value("123".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception(
          "Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }

    // remove the numeric value constraint
    getConnector().tableOperations().removeConstraint(tableName, 2);
    sleepUninterruptibly(1, TimeUnit.SECONDS);

    // now should be able to add a non numeric value
    bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());
    bw.addMutation(mut2);
    bw.close();

    // verify mutation went through
    iter = scanner.iterator();
    entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1"))
        || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1"))
        || !entry.getValue().equals(new Value("123a".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception(
          "Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }

    // add a constraint that references a non-existant class
    getConnector().tableOperations().setProperty(tableName, Property.TABLE_CONSTRAINT_PREFIX + "1",
        "com.foobar.nonExistantClass");
    sleepUninterruptibly(1, TimeUnit.SECONDS);

    // add a mutation
    bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());

    Mutation mut3 = new Mutation(new Text("r1"));
    mut3.put(new Text("cf1"), new Text("cq1"), new Value("foo".getBytes(UTF_8)));

    bw.addMutation(mut3);

    sawMRE = false;

    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, mutation went through when table had bad constraints");
    } catch (MutationsRejectedException mre) {
      sawMRE = true;
    }

    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }

    // verify the mutation did not go through
    iter = scanner.iterator();
    entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1"))
        || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1"))
        || !entry.getValue().equals(new Value("123a".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception(
          "Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }

    // remove the bad constraint
    getConnector().tableOperations().removeConstraint(tableName, 1);
    sleepUninterruptibly(1, TimeUnit.SECONDS);

    // try the mutation again
    bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());
    bw.addMutation(mut3);
    bw.close();

    // verify it went through
    iter = scanner.iterator();
    entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1"))
        || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1"))
        || !entry.getValue().equals(new Value("foo".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception(
          "Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }
  }

  private Mutation newMut(String row, String cf, String cq, String val) {
    Mutation mut1 = new Mutation(new Text(row));
    mut1.put(new Text(cf), new Text(cq), new Value(val.getBytes(UTF_8)));
    return mut1;
  }

  private void test2(String table, boolean doFlush) throws Exception {
    // test sending multiple mutations with multiple constrain violations... all of the non
    // violating mutations
    // should go through
    int numericErrors = 2;

    BatchWriter bw = getConnector().createBatchWriter(table, new BatchWriterConfig());
    bw.addMutation(newMut("r1", "cf1", "cq1", "123"));
    bw.addMutation(newMut("r1", "cf1", "cq2", "I'm a bad value"));
    if (doFlush) {
      try {
        bw.flush();
        throw new Exception("Didn't find a bad mutation");
      } catch (MutationsRejectedException mre) {
        // ignored
        try {
          bw.close();
        } catch (MutationsRejectedException ex) {
          // ignored
        }
        bw = getConnector().createBatchWriter(table, new BatchWriterConfig());
        numericErrors = 1;
      }
    }
    bw.addMutation(newMut("r1", "cf1", "cq3", "I'm a naughty value"));
    bw.addMutation(newMut("@bad row@", "cf1", "cq2", "456"));
    bw.addMutation(newMut("r1", "cf1", "cq4", "789"));

    boolean sawMRE = false;

    try {
      bw.close();
      // should not get here
      throw new Exception("Test failed, constraint did not catch bad mutation");
    } catch (MutationsRejectedException mre) {
      System.out.println(mre);

      sawMRE = true;

      // verify constraint violation summary
      List<ConstraintViolationSummary> cvsl = mre.getConstraintViolationSummaries();

      if (cvsl.size() != 2) {
        throw new Exception("Unexpected constraints");
      }

      HashMap<String,Integer> expected = new HashMap<>();

      expected.put("org.apache.accumulo.examples.simple.constraints.NumericValueConstraint",
          numericErrors);
      expected.put("org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint", 1);

      for (ConstraintViolationSummary cvs : cvsl) {
        if (expected.get(cvs.constrainClass) != cvs.numberOfViolatingMutations) {
          throw new Exception(
              "Unexpected " + cvs.constrainClass + " " + cvs.numberOfViolatingMutations);
        }
      }
    }

    if (!sawMRE) {
      throw new Exception("Did not see MutationsRejectedException");
    }

    Scanner scanner = getConnector().createScanner(table, Authorizations.EMPTY);

    Iterator<Entry<Key,Value>> iter = scanner.iterator();

    Entry<Key,Value> entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1"))
        || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq1"))
        || !entry.getValue().equals(new Value("123".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    entry = iter.next();

    if (!entry.getKey().getRow().equals(new Text("r1"))
        || !entry.getKey().getColumnFamily().equals(new Text("cf1"))
        || !entry.getKey().getColumnQualifier().equals(new Text("cq4"))
        || !entry.getValue().equals(new Value("789".getBytes(UTF_8)))) {
      throw new Exception("Unexpected key or value " + entry.getKey() + " " + entry.getValue());
    }

    if (iter.hasNext()) {
      entry = iter.next();
      throw new Exception(
          "Unexpected extra key or value " + entry.getKey() + " " + entry.getValue());
    }

  }

}

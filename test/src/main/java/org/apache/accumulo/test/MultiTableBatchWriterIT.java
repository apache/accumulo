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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class MultiTableBatchWriterIT extends AccumuloClusterHarness {

  private AccumuloClient accumuloClient;
  private MultiTableBatchWriter mtbw;

  @Override
  public int defaultTimeoutSeconds() {
    return 5 * 60;
  }

  @Before
  public void setUpArgs() {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
    mtbw = getMultiTableBatchWriter();
  }

  @After
  public void closeClient() {
    accumuloClient.close();
  }

  public MultiTableBatchWriter getMultiTableBatchWriter() {
    return accumuloClient.createMultiTableBatchWriter();
  }

  @Test
  public void testTableRenameDataValidation() throws Exception {

    try {
      final String[] names = getUniqueNames(2);
      final String table1 = names[0], table2 = names[1];

      TableOperations tops = accumuloClient.tableOperations();
      tops.create(table1);

      BatchWriter bw1 = mtbw.getBatchWriter(table1);

      Mutation m1 = new Mutation("foo");
      m1.put("col1", "", "val1");

      bw1.addMutation(m1);

      tops.rename(table1, table2);
      tops.create(table1);

      BatchWriter bw2 = mtbw.getBatchWriter(table1);

      Mutation m2 = new Mutation("bar");
      m2.put("col1", "", "val1");

      bw1.addMutation(m2);
      bw2.addMutation(m2);

      mtbw.close();

      Map<Entry<String,String>,String> table1Expectations = new HashMap<>();
      table1Expectations.put(Maps.immutableEntry("bar", "col1"), "val1");

      Map<Entry<String,String>,String> table2Expectations = new HashMap<>();
      table2Expectations.put(Maps.immutableEntry("foo", "col1"), "val1");
      table2Expectations.put(Maps.immutableEntry("bar", "col1"), "val1");

      Map<Entry<String,String>,String> actual = new HashMap<>();

      try (Scanner s = accumuloClient.createScanner(table1, new Authorizations())) {
        s.setRange(new Range());
        for (Entry<Key,Value> entry : s) {
          actual.put(Maps.immutableEntry(entry.getKey().getRow().toString(),
              entry.getKey().getColumnFamily().toString()), entry.getValue().toString());
        }
        assertEquals("Differing results for " + table1, table1Expectations, actual);
      }

      try (Scanner s = accumuloClient.createScanner(table2, new Authorizations())) {
        s.setRange(new Range());
        actual = new HashMap<>();
        for (Entry<Key,Value> entry : s) {
          actual.put(Maps.immutableEntry(entry.getKey().getRow().toString(),
              entry.getKey().getColumnFamily().toString()), entry.getValue().toString());
        }
        assertEquals("Differing results for " + table2, table2Expectations, actual);
      }

    } finally {
      if (mtbw != null) {
        mtbw.close();
      }
    }
  }

  @Test
  public void testTableRenameSameWriters() throws Exception {

    try {
      final String[] names = getUniqueNames(4);
      final String table1 = names[0], table2 = names[1];
      final String newTable1 = names[2], newTable2 = names[3];

      TableOperations tops = accumuloClient.tableOperations();
      tops.create(table1);
      tops.create(table2);

      BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

      Mutation m1 = new Mutation("foo");
      m1.put("col1", "", "val1");
      m1.put("col2", "", "val2");

      bw1.addMutation(m1);
      bw2.addMutation(m1);

      tops.rename(table1, newTable1);
      tops.rename(table2, newTable2);

      Mutation m2 = new Mutation("bar");
      m2.put("col1", "", "val1");
      m2.put("col2", "", "val2");

      bw1.addMutation(m2);
      bw2.addMutation(m2);

      mtbw.close();

      Map<Entry<String,String>,String> expectations = new HashMap<>();
      expectations.put(Maps.immutableEntry("foo", "col1"), "val1");
      expectations.put(Maps.immutableEntry("foo", "col2"), "val2");
      expectations.put(Maps.immutableEntry("bar", "col1"), "val1");
      expectations.put(Maps.immutableEntry("bar", "col2"), "val2");

      for (String table : Arrays.asList(newTable1, newTable2)) {
        try (Scanner s = accumuloClient.createScanner(table, new Authorizations())) {
          s.setRange(new Range());
          Map<Entry<String,String>,String> actual = new HashMap<>();
          for (Entry<Key,Value> entry : s) {
            actual.put(Maps.immutableEntry(entry.getKey().getRow().toString(),
                entry.getKey().getColumnFamily().toString()), entry.getValue().toString());
          }
          assertEquals("Differing results for " + table, expectations, actual);
        }
      }
    } finally {
      if (mtbw != null) {
        mtbw.close();
      }
    }
  }

  @Test
  public void testTableRenameNewWriters() throws Exception {

    try {
      final String[] names = getUniqueNames(4);
      final String table1 = names[0], table2 = names[1];
      final String newTable1 = names[2], newTable2 = names[3];

      TableOperations tops = accumuloClient.tableOperations();
      tops.create(table1);
      tops.create(table2);

      BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

      Mutation m1 = new Mutation("foo");
      m1.put("col1", "", "val1");
      m1.put("col2", "", "val2");

      bw1.addMutation(m1);
      bw2.addMutation(m1);

      tops.rename(table1, newTable1);

      // MTBW is still caching this name to the correct table, but we should invalidate its cache
      // after seeing the rename
      try {
        bw1 = mtbw.getBatchWriter(table1);
        fail("Should not be able to find this table");
      } catch (TableNotFoundException e) {
        // pass
      }

      tops.rename(table2, newTable2);

      try {
        bw2 = mtbw.getBatchWriter(table2);
        fail("Should not be able to find this table");
      } catch (TableNotFoundException e) {
        // pass
      }

      bw1 = mtbw.getBatchWriter(newTable1);
      bw2 = mtbw.getBatchWriter(newTable2);

      Mutation m2 = new Mutation("bar");
      m2.put("col1", "", "val1");
      m2.put("col2", "", "val2");

      bw1.addMutation(m2);
      bw2.addMutation(m2);

      mtbw.close();

      Map<Entry<String,String>,String> expectations = new HashMap<>();
      expectations.put(Maps.immutableEntry("foo", "col1"), "val1");
      expectations.put(Maps.immutableEntry("foo", "col2"), "val2");
      expectations.put(Maps.immutableEntry("bar", "col1"), "val1");
      expectations.put(Maps.immutableEntry("bar", "col2"), "val2");

      for (String table : Arrays.asList(newTable1, newTable2)) {
        try (Scanner s = accumuloClient.createScanner(table, new Authorizations())) {
          s.setRange(new Range());
          Map<Entry<String,String>,String> actual = new HashMap<>();
          for (Entry<Key,Value> entry : s) {
            actual.put(Maps.immutableEntry(entry.getKey().getRow().toString(),
                entry.getKey().getColumnFamily().toString()), entry.getValue().toString());
          }
          assertEquals("Differing results for " + table, expectations, actual);
        }
      }
    } finally {
      if (mtbw != null) {
        mtbw.close();
      }
    }
  }

  @Test
  public void testTableRenameNewWritersNoCaching() throws Exception {
    mtbw = getMultiTableBatchWriter();

    try {
      final String[] names = getUniqueNames(4);
      final String table1 = names[0], table2 = names[1];
      final String newTable1 = names[2], newTable2 = names[3];

      TableOperations tops = accumuloClient.tableOperations();
      tops.create(table1);
      tops.create(table2);

      BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

      Mutation m1 = new Mutation("foo");
      m1.put("col1", "", "val1");
      m1.put("col2", "", "val2");

      bw1.addMutation(m1);
      bw2.addMutation(m1);

      tops.rename(table1, newTable1);
      tops.rename(table2, newTable2);

      try {
        bw1 = mtbw.getBatchWriter(table1);
        fail("Should not have gotten batchwriter for " + table1);
      } catch (TableNotFoundException e) {
        // Pass
      }

      try {
        bw2 = mtbw.getBatchWriter(table2);
      } catch (TableNotFoundException e) {
        // Pass
      }
    } finally {
      if (mtbw != null) {
        mtbw.close();
      }
    }
  }

  @Test
  public void testTableDelete() throws Exception {
    boolean mutationsRejected = false;

    try {
      final String[] names = getUniqueNames(2);
      final String table1 = names[0], table2 = names[1];

      TableOperations tops = accumuloClient.tableOperations();
      tops.create(table1);
      tops.create(table2);

      BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

      Mutation m1 = new Mutation("foo");
      m1.put("col1", "", "val1");
      m1.put("col2", "", "val2");

      bw1.addMutation(m1);
      bw2.addMutation(m1);

      tops.delete(table1);
      tops.delete(table2);

      Mutation m2 = new Mutation("bar");
      m2.put("col1", "", "val1");
      m2.put("col2", "", "val2");

      try {
        bw1.addMutation(m2);
        bw2.addMutation(m2);
      } catch (MutationsRejectedException e) {
        // Pass - Mutations might flush immediately
        mutationsRejected = true;
      }

    } finally {
      if (mtbw != null) {
        try {
          // Mutations might have flushed before the table offline occurred
          mtbw.close();
        } catch (MutationsRejectedException e) {
          // Pass
          mutationsRejected = true;
        }
      }
    }

    assertTrue("Expected mutations to be rejected.", mutationsRejected);
  }

  @Test
  public void testOfflineTable() throws Exception {
    boolean mutationsRejected = false;

    try {
      final String[] names = getUniqueNames(2);
      final String table1 = names[0], table2 = names[1];

      TableOperations tops = accumuloClient.tableOperations();
      tops.create(table1);
      tops.create(table2);

      BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

      Mutation m1 = new Mutation("foo");
      m1.put("col1", "", "val1");
      m1.put("col2", "", "val2");

      bw1.addMutation(m1);
      bw2.addMutation(m1);

      tops.offline(table1, true);
      tops.offline(table2, true);

      Mutation m2 = new Mutation("bar");
      m2.put("col1", "", "val1");
      m2.put("col2", "", "val2");

      try {
        bw1.addMutation(m2);
        bw2.addMutation(m2);
      } catch (MutationsRejectedException e) {
        // Pass -- Mutations might flush immediately and fail because of offline table
        mutationsRejected = true;
      }
    } finally {
      if (mtbw != null) {
        try {
          mtbw.close();
        } catch (MutationsRejectedException e) {
          // Pass
          mutationsRejected = true;
        }
      }
    }

    assertTrue("Expected mutations to be rejected.", mutationsRejected);
  }
}

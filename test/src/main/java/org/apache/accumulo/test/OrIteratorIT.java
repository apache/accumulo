/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OrIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class OrIteratorIT extends AccumuloClusterHarness {
  private static final String EMPTY = "";

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void testMultipleRowsInTablet() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    try (BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig())) {
      Mutation m = new Mutation("row1");
      m.put("bob", "2", EMPTY);
      m.put("frank", "3", EMPTY);
      m.put("steve", "1", EMPTY);
      bw.addMutation(m);

      m = new Mutation("row2");
      m.put("bob", "7", EMPTY);
      m.put("eddie", "4", EMPTY);
      m.put("mort", "6", EMPTY);
      m.put("zed", "5", EMPTY);
      bw.addMutation(m);
    }

    IteratorSetting is = new IteratorSetting(50, OrIterator.class);
    is.addOption(OrIterator.COLUMNS_KEY, "mort,frank");
    Map<String,String> expectedData = new HashMap<>();
    expectedData.put("frank", "3");
    expectedData.put("mort", "6");

    try (BatchScanner bs = conn.createBatchScanner(tableName, Authorizations.EMPTY, 1)) {
      Set<Range> ranges = new HashSet<>(Arrays.asList(Range.exact("row1"), Range.exact("row2")));
      bs.setRanges(ranges);
      bs.addScanIterator(is);
      for (Entry<Key,Value> entry : bs) {
        String term = entry.getKey().getColumnFamily().toString();
        String expectedDocId = expectedData.remove(term);
        assertNotNull("Found unexpected term: " + term, expectedDocId);
        assertEquals(expectedDocId, entry.getKey().getColumnQualifier().toString());
      }
      assertTrue("Expected no leftover entries but saw " + expectedData, expectedData.isEmpty());
    }
  }

  @Test
  public void testMultipleTablets() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    try (BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig())) {
      Mutation m = new Mutation("row1");
      m.put("bob", "2", EMPTY);
      m.put("frank", "3", EMPTY);
      m.put("steve", "1", EMPTY);
      bw.addMutation(m);

      m = new Mutation("row2");
      m.put("bob", "7", EMPTY);
      m.put("eddie", "4", EMPTY);
      m.put("mort", "6", EMPTY);
      m.put("zed", "5", EMPTY);
      bw.addMutation(m);

      m = new Mutation("row3");
      m.put("carl", "9", EMPTY);
      m.put("george", "8", EMPTY);
      m.put("nick", "3", EMPTY);
      m.put("zed", "1", EMPTY);
      bw.addMutation(m);
    }

    conn.tableOperations().addSplits(tableName, new TreeSet<>(Arrays.asList(new Text("row2"), new Text("row3"))));

    IteratorSetting is = new IteratorSetting(50, OrIterator.class);
    is.addOption(OrIterator.COLUMNS_KEY, "mort,frank,nick");
    Map<String,String> expectedData = new HashMap<>();
    expectedData.put("frank", "3");
    expectedData.put("mort", "6");
    expectedData.put("nick", "3");

    try (BatchScanner bs = conn.createBatchScanner(tableName, Authorizations.EMPTY, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      bs.addScanIterator(is);
      for (Entry<Key,Value> entry : bs) {
        String term = entry.getKey().getColumnFamily().toString();
        String expectedDocId = expectedData.remove(term);
        assertNotNull("Found unexpected term: " + term, expectedDocId);
        assertEquals(expectedDocId, entry.getKey().getColumnQualifier().toString());
      }
      assertTrue("Expected no leftover entries but saw " + expectedData, expectedData.isEmpty());
    }
  }

  @Test
  public void testSingleLargeRow() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);
    conn.tableOperations().setProperty(tableName, Property.TABLE_SCAN_MAXMEM.getKey(), "1");

    try (BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig())) {
      Mutation m = new Mutation("row1");
      m.put("bob", "02", EMPTY);
      m.put("carl", "07", EMPTY);
      m.put("eddie", "04", EMPTY);
      m.put("frank", "03", EMPTY);
      m.put("greg", "15", EMPTY);
      m.put("mort", "06", EMPTY);
      m.put("nick", "12", EMPTY);
      m.put("richard", "18", EMPTY);
      m.put("steve", "01", EMPTY);
      m.put("ted", "11", EMPTY);
      m.put("zed", "05", EMPTY);
      bw.addMutation(m);
    }

    IteratorSetting is = new IteratorSetting(50, OrIterator.class);
    is.addOption(OrIterator.COLUMNS_KEY, "richard,carl,frank,nick,eddie,zed");
    Map<String,String> expectedData = new HashMap<>();
    expectedData.put("frank", "03");
    expectedData.put("eddie", "04");
    expectedData.put("zed", "05");
    expectedData.put("carl", "07");
    expectedData.put("nick", "12");
    expectedData.put("richard", "18");

    try (BatchScanner bs = conn.createBatchScanner(tableName, Authorizations.EMPTY, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      bs.addScanIterator(is);
      for (Entry<Key,Value> entry : bs) {
        String term = entry.getKey().getColumnFamily().toString();
        String expectedDocId = expectedData.remove(term);
        assertNotNull("Found unexpected term: " + term + " or the docId was unexpectedly null", expectedDocId);
        assertEquals(expectedDocId, entry.getKey().getColumnQualifier().toString());
      }
      assertTrue("Expected no leftover entries but saw " + expectedData, expectedData.isEmpty());
    }
  }

  @Test
  public void testNoMatchesForTable() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    try (BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig())) {
      Mutation m = new Mutation("row1");
      m.put("bob", "02", EMPTY);
      m.put("carl", "07", EMPTY);
      m.put("eddie", "04", EMPTY);
      m.put("frank", "03", EMPTY);
      m.put("greg", "15", EMPTY);
      m.put("mort", "06", EMPTY);
      m.put("nick", "12", EMPTY);
      m.put("richard", "18", EMPTY);
      m.put("steve", "01", EMPTY);
      m.put("ted", "11", EMPTY);
      m.put("zed", "05", EMPTY);
      bw.addMutation(m);
    }

    IteratorSetting is = new IteratorSetting(50, OrIterator.class);
    is.addOption(OrIterator.COLUMNS_KEY, "theresa,sally");
    Map<String,String> expectedData = Collections.emptyMap();

    try (BatchScanner bs = conn.createBatchScanner(tableName, Authorizations.EMPTY, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      bs.addScanIterator(is);
      for (Entry<Key,Value> entry : bs) {
        String term = entry.getKey().getColumnFamily().toString();
        String expectedDocId = expectedData.remove(term);
        assertNotNull("Found unexpected term: " + term + " or the docId was unexpectedly null", expectedDocId);
        assertEquals(expectedDocId, entry.getKey().getColumnQualifier().toString());
      }
      assertTrue("Expected no leftover entries but saw " + expectedData, expectedData.isEmpty());
    }
  }

  @Test
  public void testNoMatchesInSingleTablet() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    try (BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig())) {
      Mutation m = new Mutation("row1");
      m.put("bob", "02", EMPTY);
      m.put("carl", "07", EMPTY);
      m.put("eddie", "04", EMPTY);
      bw.addMutation(m);

      m = new Mutation("row2");
      m.put("frank", "03", EMPTY);
      m.put("greg", "15", EMPTY);
      m.put("mort", "06", EMPTY);
      m.put("nick", "12", EMPTY);
      bw.addMutation(m);

      m = new Mutation("row3");
      m.put("richard", "18", EMPTY);
      m.put("steve", "01", EMPTY);
      m.put("ted", "11", EMPTY);
      m.put("zed", "05", EMPTY);
      bw.addMutation(m);
    }

    IteratorSetting is = new IteratorSetting(50, OrIterator.class);
    is.addOption(OrIterator.COLUMNS_KEY, "bob,eddie,steve,zed");
    Map<String,String> expectedData = new HashMap<>();
    expectedData.put("bob", "02");
    expectedData.put("eddie", "04");
    expectedData.put("zed", "05");
    expectedData.put("steve", "01");

    // Split each row into its own tablet
    conn.tableOperations().addSplits(tableName, new TreeSet<>(Arrays.asList(new Text("row2"), new Text("row3"))));

    try (BatchScanner bs = conn.createBatchScanner(tableName, Authorizations.EMPTY, 1)) {
      bs.setRanges(Collections.singleton(new Range()));
      bs.addScanIterator(is);
      for (Entry<Key,Value> entry : bs) {
        String term = entry.getKey().getColumnFamily().toString();
        String expectedDocId = expectedData.remove(term);
        assertNotNull("Found unexpected term: " + term + " or the docId was unexpectedly null", expectedDocId);
        assertEquals(expectedDocId, entry.getKey().getColumnQualifier().toString());
      }
      assertTrue("Expected no leftover entries but saw " + expectedData, expectedData.isEmpty());
    }
  }

  @Test
  public void testResultOrder() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    try (BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig())) {
      Mutation m = new Mutation("row1");
      m.put("bob", "2", EMPTY);
      m.put("frank", "3", EMPTY);
      m.put("steve", "1", EMPTY);
      bw.addMutation(m);
    }

    IteratorSetting is = new IteratorSetting(50, OrIterator.class);
    is.addOption(OrIterator.COLUMNS_KEY, "bob,steve");

    try (Scanner s = conn.createScanner(tableName, Authorizations.EMPTY)) {
      s.addScanIterator(is);
      Iterator<Entry<Key,Value>> iter = s.iterator();
      assertTrue(iter.hasNext());
      Key k = iter.next().getKey();
      assertEquals("Actual key was " + k, 0, k.compareTo(new Key("row1", "steve", "1"), PartialKey.ROW_COLFAM_COLQUAL));
      assertTrue(iter.hasNext());
      k = iter.next().getKey();
      assertEquals("Actual key was " + k, 0, k.compareTo(new Key("row1", "bob", "2"), PartialKey.ROW_COLFAM_COLQUAL));
      assertFalse(iter.hasNext());
    }
  }
}

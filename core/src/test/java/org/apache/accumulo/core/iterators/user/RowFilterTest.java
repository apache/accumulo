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
package org.apache.accumulo.core.iterators.user;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 *
 */

public class RowFilterTest {

  public static class SummingRowFilter extends RowFilter {

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) throws IOException {
      int sum = 0;
      int sum2 = 0;

      Key firstKey = null;

      if (rowIterator.hasTop()) {
        firstKey = new Key(rowIterator.getTopKey());
      }

      while (rowIterator.hasTop()) {
        sum += Integer.parseInt(rowIterator.getTopValue().toString());
        rowIterator.next();
      }

      // ensure that seeks are confined to the row
      rowIterator.seek(new Range(), new HashSet<ByteSequence>(), false);
      while (rowIterator.hasTop()) {
        sum2 += Integer.parseInt(rowIterator.getTopValue().toString());
        rowIterator.next();
      }

      rowIterator.seek(new Range(firstKey.getRow(), false, null, true), new HashSet<ByteSequence>(), false);
      while (rowIterator.hasTop()) {
        sum2 += Integer.parseInt(rowIterator.getTopValue().toString());
        rowIterator.next();
      }

      return sum == 2 && sum2 == 2;
    }

  }

  public static class RowZeroOrOneFilter extends RowFilter {
    private static final Set<String> passRows = new HashSet<String>(Arrays.asList("0", "1"));

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) throws IOException {
      return rowIterator.hasTop() && passRows.contains(rowIterator.getTopKey().getRow().toString());
    }
  }

  public static class RowOneOrTwoFilter extends RowFilter {
    private static final Set<String> passRows = new HashSet<String>(Arrays.asList("1", "2"));

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) throws IOException {
      return rowIterator.hasTop() && passRows.contains(rowIterator.getTopKey().getRow().toString());
    }
  }

  public static class TrueFilter extends RowFilter {
    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) throws IOException {
      return true;
    }
  }

  public List<Mutation> createMutations() {
    List<Mutation> mutations = new LinkedList<Mutation>();
    Mutation m = new Mutation("0");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "1");
    m.put("cf1", "cq3", "1");
    m.put("cf1", "cq4", "1");
    m.put("cf1", "cq5", "1");
    m.put("cf1", "cq6", "1");
    m.put("cf1", "cq7", "1");
    m.put("cf1", "cq8", "1");
    m.put("cf1", "cq9", "1");
    m.put("cf2", "cq1", "1");
    m.put("cf2", "cq2", "1");
    mutations.add(m);

    m = new Mutation("1");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "2");
    mutations.add(m);

    m = new Mutation("2");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "1");
    mutations.add(m);

    m = new Mutation("3");
    m.put("cf1", "cq1", "0");
    m.put("cf1", "cq2", "2");
    mutations.add(m);

    m = new Mutation("4");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "1");
    m.put("cf1", "cq3", "1");
    m.put("cf1", "cq4", "1");
    m.put("cf1", "cq5", "1");
    m.put("cf1", "cq6", "1");
    m.put("cf1", "cq7", "1");
    m.put("cf1", "cq8", "1");
    m.put("cf1", "cq9", "1");
    m.put("cf2", "cq1", "1");
    m.put("cf2", "cq2", "1");

    mutations.add(m);
    return mutations;
  }

  public TreeMap<Key,Value> createKeyValues() {
    List<Mutation> mutations = createMutations();
    TreeMap<Key,Value> keyValues = new TreeMap<Key,Value>();

    final Text cf = new Text(), cq = new Text();
    for (Mutation m : mutations) {
      final Text row = new Text(m.getRow());
      for (ColumnUpdate update : m.getUpdates()) {
        cf.set(update.getColumnFamily());
        cq.set(update.getColumnQualifier());

        Key k = new Key(row, cf, cq);
        Value v = new Value(update.getValue());

        keyValues.put(k, v);
      }
    }

    return keyValues;
  }

  @Test
  public void test1() throws Exception {
    MockInstance instance = new MockInstance("rft1");
    Connector conn = instance.getConnector("", new PasswordToken(""));

    conn.tableOperations().create("table1");
    BatchWriter bw = conn.createBatchWriter("table1", new BatchWriterConfig());

    for (Mutation m : createMutations()) {
      bw.addMutation(m);
    }
    IteratorSetting is = new IteratorSetting(40, SummingRowFilter.class);
    conn.tableOperations().attachIterator("table1", is);

    Scanner scanner = conn.createScanner("table1", Authorizations.EMPTY);
    assertEquals(new HashSet<String>(Arrays.asList("2", "3")), getRows(scanner));

    scanner.fetchColumn(new Text("cf1"), new Text("cq2"));
    assertEquals(new HashSet<String>(Arrays.asList("1", "3")), getRows(scanner));

    scanner.clearColumns();
    scanner.fetchColumn(new Text("cf1"), new Text("cq1"));
    assertEquals(new HashSet<String>(), getRows(scanner));

    scanner.setRange(new Range("0", "4"));
    scanner.clearColumns();
    assertEquals(new HashSet<String>(Arrays.asList("2", "3")), getRows(scanner));

    scanner.setRange(new Range("2"));
    scanner.clearColumns();
    assertEquals(new HashSet<String>(Arrays.asList("2")), getRows(scanner));

    scanner.setRange(new Range("4"));
    scanner.clearColumns();
    assertEquals(new HashSet<String>(), getRows(scanner));

    scanner.setRange(new Range("4"));
    scanner.clearColumns();
    scanner.fetchColumn(new Text("cf1"), new Text("cq2"));
    scanner.fetchColumn(new Text("cf1"), new Text("cq4"));
    assertEquals(new HashSet<String>(Arrays.asList("4")), getRows(scanner));

  }

  @Test
  public void testChainedRowFilters() throws Exception {
    MockInstance instance = new MockInstance("rft1");
    Connector conn = instance.getConnector("", new PasswordToken(""));

    conn.tableOperations().create("chained_row_filters");
    BatchWriter bw = conn.createBatchWriter("chained_row_filters", new BatchWriterConfig());
    for (Mutation m : createMutations()) {
      bw.addMutation(m);
    }
    conn.tableOperations().attachIterator("chained_row_filters", new IteratorSetting(40, "trueFilter1", TrueFilter.class));
    conn.tableOperations().attachIterator("chained_row_filters", new IteratorSetting(41, "trueFilter2", TrueFilter.class));
    Scanner scanner = conn.createScanner("chained_row_filters", Authorizations.EMPTY);
    assertEquals(new HashSet<String>(Arrays.asList("0", "1", "2", "3", "4")), getRows(scanner));
  }

  @Test
  public void testFilterConjunction() throws Exception {
    MockInstance instance = new MockInstance("rft1");
    Connector conn = instance.getConnector("", new PasswordToken(""));

    conn.tableOperations().create("filter_conjunction");
    BatchWriter bw = conn.createBatchWriter("filter_conjunction", new BatchWriterConfig());
    for (Mutation m : createMutations()) {
      bw.addMutation(m);
    }
    conn.tableOperations().attachIterator("filter_conjunction", new IteratorSetting(40, "rowZeroOrOne", RowZeroOrOneFilter.class));
    conn.tableOperations().attachIterator("filter_conjunction", new IteratorSetting(41, "rowOneOrTwo", RowOneOrTwoFilter.class));
    Scanner scanner = conn.createScanner("filter_conjunction", Authorizations.EMPTY);
    assertEquals(new HashSet<String>(Arrays.asList("1")), getRows(scanner));
  }

  @Test
  public void deepCopyCopiesTheSource() throws Exception {
    SortedMapIterator source = new SortedMapIterator(createKeyValues());

    RowFilter filter = new RowZeroOrOneFilter();
    filter.init(source, Collections.<String,String> emptyMap(), new DefaultIteratorEnvironment());

    filter.seek(new Range(), Collections.<ByteSequence> emptySet(), false);

    // Save off the first key and value
    Key firstKey = filter.getTopKey();
    Value firstValue = filter.getTopValue();

    // Assert that the row is valid given our filter
    assertEquals("0", firstKey.getRow().toString());

    // Read some extra data, just making sure it's all valid
    Key lastKeyRead = null;
    for (int i = 0; i < 5; i++) {
      filter.next();
      lastKeyRead = filter.getTopKey();
      assertEquals("0", lastKeyRead.getRow().toString());
    }

    // Make a copy of the original RowFilter
    RowFilter copy = (RowFilter) filter.deepCopy(new DefaultIteratorEnvironment());

    // Because it's a copy, we should be able to safely seek this one without affecting the original
    copy.seek(new Range(), Collections.<ByteSequence> emptySet(), false);

    assertTrue("deepCopy'ed RowFilter did not have a top key", copy.hasTop());

    Key firstKeyFromCopy = copy.getTopKey();
    Value firstValueFromCopy = copy.getTopValue();

    // Verify that we got the same first k-v pair we did earlier
    assertEquals(firstKey, firstKeyFromCopy);
    assertEquals(firstValue, firstValueFromCopy);

    filter.next();
    Key finalKeyRead = filter.getTopKey();

    // Make sure we got a Key that was greater than the last Key we read from the original RowFilter
    assertTrue("Expected next key read to be greater than the previous after deepCopy", lastKeyRead.compareTo(finalKeyRead) < 0);
  }

  private HashSet<String> getRows(Scanner scanner) {
    HashSet<String> rows = new HashSet<String>();
    for (Entry<Key,Value> entry : scanner) {
      rows.add(entry.getKey().getRow().toString());
    }
    return rows;
  }
}

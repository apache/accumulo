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
package org.apache.accumulo.core.iterators.user;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

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
      rowIterator.seek(new Range(null, false, firstKey == null ? null : firstKey.getRow(), false),
          Set.of(), false);
      while (rowIterator.hasTop()) {
        sum2 += Integer.parseInt(rowIterator.getTopValue().toString());
        rowIterator.next();
      }

      rowIterator.seek(new Range(firstKey == null ? null : firstKey.getRow(), false, null, true),
          Set.of(), false);
      while (rowIterator.hasTop()) {
        sum2 += Integer.parseInt(rowIterator.getTopValue().toString());
        rowIterator.next();
      }

      return sum == 2 && sum2 == 0;
    }

  }

  public static class RowZeroOrOneFilter extends RowFilter {
    private static final Set<String> passRows = Set.of("0", "1");

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) {
      return rowIterator.hasTop() && passRows.contains(rowIterator.getTopKey().getRow().toString());
    }
  }

  public static class RowOneOrTwoFilter extends RowFilter {
    private static final Set<String> passRows = Set.of("1", "2");

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) {
      return rowIterator.hasTop() && passRows.contains(rowIterator.getTopKey().getRow().toString());
    }
  }

  public static class TrueFilter extends RowFilter {
    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) {
      return true;
    }
  }

  public static class ColFamFilter extends RowFilter {
    private Set<ByteSequence> families;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);
      this.families = Set.of(new ArrayByteSequence(options.get("family")));
    }

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key,Value> rowIterator) throws IOException {
      rowIterator.seek(new Range(), families, true);
      return rowIterator.hasTop();
    }

  }

  public List<Mutation> createMutations() {
    List<Mutation> mutations = new LinkedList<>();
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
    m.put("cf2", "cq2", "2");
    mutations.add(m);

    m = new Mutation("2");
    m.put("cf1", "cq1", "1");
    m.put("cf1", "cq2", "1");
    mutations.add(m);

    m = new Mutation("3");
    m.put("cf1", "cq1", "0");
    m.put("cf2", "cq2", "2");
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
    TreeMap<Key,Value> keyValues = new TreeMap<>();

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
    ColumnFamilySkippingIterator source =
        new ColumnFamilySkippingIterator(new SortedMapIterator(createKeyValues()));

    RowFilter filter = new SummingRowFilter();
    filter.init(source, Collections.emptyMap(), new DefaultIteratorEnvironment());

    filter.seek(new Range(), Collections.emptySet(), false);

    assertEquals(Set.of("2", "3"), getRows(filter));

    ByteSequence cf = new ArrayByteSequence("cf2");

    filter.seek(new Range(), Set.of(cf), true);
    assertEquals(Set.of("1", "3", "0", "4"), getRows(filter));

    filter.seek(new Range("0", "4"), Collections.emptySet(), false);
    assertEquals(Set.of("2", "3"), getRows(filter));

    filter.seek(new Range("2"), Collections.emptySet(), false);
    assertEquals(Set.of("2"), getRows(filter));

    filter.seek(new Range("4"), Collections.emptySet(), false);
    assertEquals(Set.of(), getRows(filter));

    filter.seek(new Range("4"), Set.of(cf), true);
    assertEquals(Set.of("4"), getRows(filter));

  }

  @Test
  public void testChainedRowFilters() throws Exception {
    SortedMapIterator source = new SortedMapIterator(createKeyValues());

    RowFilter filter0 = new TrueFilter();
    filter0.init(source, Collections.emptyMap(), new DefaultIteratorEnvironment());

    RowFilter filter = new TrueFilter();
    filter.init(filter0, Collections.emptyMap(), new DefaultIteratorEnvironment());

    filter.seek(new Range(), Collections.emptySet(), false);

    assertEquals(Set.of("0", "1", "2", "3", "4"), getRows(filter));
  }

  @Test
  public void testFilterConjunction() throws Exception {

    SortedMapIterator source = new SortedMapIterator(createKeyValues());

    RowFilter filter0 = new RowZeroOrOneFilter();
    filter0.init(source, Collections.emptyMap(), new DefaultIteratorEnvironment());

    RowFilter filter = new RowOneOrTwoFilter();
    filter.init(filter0, Collections.emptyMap(), new DefaultIteratorEnvironment());

    filter.seek(new Range(), Collections.emptySet(), false);

    assertEquals(Set.of("1"), getRows(filter));
  }

  @Test
  public void deepCopyCopiesTheSource() throws Exception {
    SortedMapIterator source = new SortedMapIterator(createKeyValues());

    RowFilter filter = new RowZeroOrOneFilter();
    filter.init(source, Collections.emptyMap(), new DefaultIteratorEnvironment());

    filter.seek(new Range(), Collections.emptySet(), false);

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
    copy.seek(new Range(), Collections.emptySet(), false);

    assertTrue(copy.hasTop(), "deepCopy'ed RowFilter did not have a top key");

    Key firstKeyFromCopy = copy.getTopKey();
    Value firstValueFromCopy = copy.getTopValue();

    // Verify that we got the same first k-v pair we did earlier
    assertEquals(firstKey, firstKeyFromCopy);
    assertEquals(firstValue, firstValueFromCopy);

    filter.next();
    Key finalKeyRead = filter.getTopKey();

    // Make sure we got a Key that was greater than the last Key we read from the original RowFilter
    assertTrue(lastKeyRead.compareTo(finalKeyRead) < 0,
        "Expected next key read to be greater than the previous after deepCopy");
  }

  // This tests a chaining RowFitler that have an init method and deep copying the chain. There was
  // a bug where if the row filter called init it was not called on deep copy.
  @Test
  public void testDeepCopyChainedIteratorWithInit() throws Exception {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("1", "f1", "q1"), new Value("1"));
    data.put(new Key("1", "f2", "q1"), new Value("2"));
    data.put(new Key("2", "f1", "q1"), new Value("3"));
    data.put(new Key("2", "f2", "q1"), new Value("4"));
    data.put(new Key("2", "f3", "q1"), new Value("5"));
    data.put(new Key("3", "f1", "q1"), new Value("6"));
    data.put(new Key("3", "f2", "q1"), new Value("7"));
    data.put(new Key("3", "f3", "q1"), new Value("8"));
    data.put(new Key("3", "f4", "q1"), new Value("9"));
    data.put(new Key("4", "f3", "q1"), new Value("0"));
    data.put(new Key("5", "f1", "q1"), new Value("a"));
    data.put(new Key("5", "f3", "q1"), new Value("b"));

    var source = new ColumnFamilySkippingIterator(new SortedMapIterator(data));

    RowFilter filter1 = new ColFamFilter();
    filter1.init(source, Map.of("family", "f1"), null);

    // The chain of filter1 and filter2 should only return rows that have families 'f1' and 'f3'.
    RowFilter filter2 = new ColFamFilter();
    filter2.init(filter1, Map.of("family", "f3"), null);

    // Deep copy of filter2 should be able to independently iterator over the same data.
    var filter3 = (RowFilter) filter2.deepCopy(null);

    for (int i = 0; i < 3; i++) {
      filter2.seek(new Range(), Set.of(), false);
      filter3.seek(new Range(), Set.of(), false);

      Iterator<Key> expectedKeys2 = data.keySet().stream().filter(k -> {
        var row = k.getRowData().toString();
        return row.equals("2") || row.equals("3") || row.equals("5");
      }).iterator();

      Iterator<Key> expectedKeys3 = data.keySet().stream().filter(k -> {
        var row = k.getRowData().toString();
        return row.equals("2") || row.equals("3") || row.equals("5");
      }).iterator();

      // advance one iterator and not the other so they are on different keys in the loop
      assertEquals(expectedKeys2.next(), filter2.getTopKey());
      filter2.next();

      while (expectedKeys2.hasNext() || filter2.hasTop() || expectedKeys3.hasNext()
          || filter3.hasTop()) {
        if (expectedKeys2.hasNext()) {
          assertEquals(expectedKeys2.next(), filter2.getTopKey());
          filter2.next();
        }
        if (expectedKeys3.hasNext()) {
          assertEquals(expectedKeys3.next(), filter3.getTopKey());
          filter3.next();
        }
      }

      assertFalse(expectedKeys2.hasNext());
      assertFalse(filter2.hasTop());
      assertFalse(expectedKeys3.hasNext());
      assertFalse(filter3.hasTop());
    }
  }

  private HashSet<String> getRows(RowFilter filter) throws IOException {
    HashSet<String> rows = new HashSet<>();
    while (filter.hasTop()) {
      rows.add(filter.getTopKey().getRowData().toString());
      filter.next();
    }
    return rows;
  }
}

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
package org.apache.accumulo.core.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class AggregatingIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  /**
   * @deprecated since 1.4; visible only for testing
   */
  @Deprecated
  public static class SummationAggregator implements org.apache.accumulo.core.iterators.aggregation.Aggregator {

    int sum;

    @Override
    public Value aggregate() {
      return new Value((sum + "").getBytes());
    }

    @Override
    public void collect(Value value) {
      int val = Integer.parseInt(value.toString());

      sum += val;
    }

    @Override
    public void reset() {
      sum = 0;

    }

  }

  static Key newKey(int row, int colf, int colq, long ts, boolean deleted) {
    Key k = newKey(row, colf, colq, ts);
    k.setDeleted(true);
    return k;
  }

  static Key newKey(int row, int colf, int colq, long ts) {
    return new Key(newRow(row), new Text(String.format("cf%03d", colf)), new Text(String.format("cq%03d", colq)), ts);
  }

  static Range newRow(int row, int colf, int colq, long ts, boolean inclusive) {
    return new Range(newKey(row, colf, colq, ts), inclusive, null, true);
  }

  static Range newRow(int row, int colf, int colq, long ts) {
    return newRow(row, colf, colq, ts, true);
  }

  static void newKeyValue(TreeMap<Key,Value> tm, int row, int colf, int colq, long ts, boolean deleted, String val) {
    Key k = newKey(row, colf, colq, ts);
    k.setDeleted(deleted);
    tm.put(k, new Value(val.getBytes()));
  }

  static Text newRow(int row) {
    return new Text(String.format("r%03d", row));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void test1() throws IOException {

    TreeMap<Key,Value> tm1 = new TreeMap<>();

    // keys that do not aggregate
    newKeyValue(tm1, 1, 1, 1, 1, false, "2");
    newKeyValue(tm1, 1, 1, 1, 2, false, "3");
    newKeyValue(tm1, 1, 1, 1, 3, false, "4");

    AggregatingIterator ai = new AggregatingIterator();

    Map<String,String> emptyMap = Collections.emptyMap();
    ai.init(new SortedMapIterator(tm1), emptyMap, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 3), ai.getTopKey());
    assertEquals("4", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 2), ai.getTopKey());
    assertEquals("3", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 1), ai.getTopKey());
    assertEquals("2", ai.getTopValue().toString());

    ai.next();

    assertFalse(ai.hasTop());

    // try seeking

    ai.seek(newRow(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 2), ai.getTopKey());
    assertEquals("3", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 1), ai.getTopKey());
    assertEquals("2", ai.getTopValue().toString());

    ai.next();

    assertFalse(ai.hasTop());

    // seek after everything
    ai.seek(newRow(1, 1, 1, 0), EMPTY_COL_FAMS, false);

    assertFalse(ai.hasTop());

  }

  @SuppressWarnings("deprecation")
  @Test
  public void test2() throws IOException {
    TreeMap<Key,Value> tm1 = new TreeMap<>();

    // keys that aggregate
    newKeyValue(tm1, 1, 1, 1, 1, false, "2");
    newKeyValue(tm1, 1, 1, 1, 2, false, "3");
    newKeyValue(tm1, 1, 1, 1, 3, false, "4");

    AggregatingIterator ai = new AggregatingIterator();

    Map<String,String> opts = new HashMap<>();

    opts.put("cf001", SummationAggregator.class.getName());

    ai.init(new SortedMapIterator(tm1), opts, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", ai.getTopValue().toString());

    ai.next();

    assertFalse(ai.hasTop());

    // try seeking to the beginning of a key that aggregates

    ai.seek(newRow(1, 1, 1, 3), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", ai.getTopValue().toString());

    ai.next();

    assertFalse(ai.hasTop());

    // try seeking the middle of a key the aggregates
    ai.seek(newRow(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertFalse(ai.hasTop());

    // try seeking to the end of a key the aggregates
    ai.seek(newRow(1, 1, 1, 1), EMPTY_COL_FAMS, false);

    assertFalse(ai.hasTop());

    // try seeking before a key the aggregates
    ai.seek(newRow(1, 1, 1, 4), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", ai.getTopValue().toString());

    ai.next();

    assertFalse(ai.hasTop());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void test3() throws IOException {

    TreeMap<Key,Value> tm1 = new TreeMap<>();

    // keys that aggregate
    newKeyValue(tm1, 1, 1, 1, 1, false, "2");
    newKeyValue(tm1, 1, 1, 1, 2, false, "3");
    newKeyValue(tm1, 1, 1, 1, 3, false, "4");

    // keys that do not aggregate
    newKeyValue(tm1, 2, 2, 1, 1, false, "2");
    newKeyValue(tm1, 2, 2, 1, 2, false, "3");

    AggregatingIterator ai = new AggregatingIterator();

    Map<String,String> opts = new HashMap<>();

    opts.put("cf001", SummationAggregator.class.getName());

    ai.init(new SortedMapIterator(tm1), opts, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(2, 2, 1, 1), ai.getTopKey());
    assertEquals("2", ai.getTopValue().toString());

    ai.next();

    assertFalse(ai.hasTop());

    // seek after key that aggregates
    ai.seek(newRow(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", ai.getTopValue().toString());

    // seek before key that aggregates
    ai.seek(newRow(1, 1, 1, 4), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", ai.getTopValue().toString());

  }

  @SuppressWarnings("deprecation")
  @Test
  public void test4() throws IOException {

    TreeMap<Key,Value> tm1 = new TreeMap<>();

    // keys that do not aggregate
    newKeyValue(tm1, 0, 0, 1, 1, false, "7");

    // keys that aggregate
    newKeyValue(tm1, 1, 1, 1, 1, false, "2");
    newKeyValue(tm1, 1, 1, 1, 2, false, "3");
    newKeyValue(tm1, 1, 1, 1, 3, false, "4");

    // keys that do not aggregate
    newKeyValue(tm1, 2, 2, 1, 1, false, "2");
    newKeyValue(tm1, 2, 2, 1, 2, false, "3");

    AggregatingIterator ai = new AggregatingIterator();

    Map<String,String> opts = new HashMap<>();

    opts.put("cf001", SummationAggregator.class.getName());

    ai.init(new SortedMapIterator(tm1), opts, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(0, 0, 1, 1), ai.getTopKey());
    assertEquals("7", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(2, 2, 1, 1), ai.getTopKey());
    assertEquals("2", ai.getTopValue().toString());

    ai.next();

    assertFalse(ai.hasTop());

    // seek test
    ai.seek(newRow(0, 0, 1, 0), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", ai.getTopValue().toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(newKey(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", ai.getTopValue().toString());

    // seek after key that aggregates
    ai.seek(newRow(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", ai.getTopValue().toString());

  }

  @SuppressWarnings("deprecation")
  @Test
  public void test5() throws IOException {
    // try aggregating across multiple data sets that contain
    // the exact same keys w/ different values

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    newKeyValue(tm1, 1, 1, 1, 1, false, "2");

    TreeMap<Key,Value> tm2 = new TreeMap<>();
    newKeyValue(tm2, 1, 1, 1, 1, false, "3");

    TreeMap<Key,Value> tm3 = new TreeMap<>();
    newKeyValue(tm3, 1, 1, 1, 1, false, "4");

    AggregatingIterator ai = new AggregatingIterator();
    Map<String,String> opts = new HashMap<>();
    opts.put("cf001", SummationAggregator.class.getName());

    List<SortedKeyValueIterator<Key,Value>> sources = new ArrayList<>(3);
    sources.add(new SortedMapIterator(tm1));
    sources.add(new SortedMapIterator(tm2));
    sources.add(new SortedMapIterator(tm3));

    MultiIterator mi = new MultiIterator(sources, true);
    ai.init(mi, opts, null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 1), ai.getTopKey());
    assertEquals("9", ai.getTopValue().toString());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void test6() throws IOException {
    TreeMap<Key,Value> tm1 = new TreeMap<>();

    // keys that aggregate
    newKeyValue(tm1, 1, 1, 1, 1, false, "2");
    newKeyValue(tm1, 1, 1, 1, 2, false, "3");
    newKeyValue(tm1, 1, 1, 1, 3, false, "4");

    AggregatingIterator ai = new AggregatingIterator();

    Map<String,String> opts = new HashMap<>();

    opts.put("cf001", SummationAggregator.class.getName());

    ai.init(new SortedMapIterator(tm1), opts, new DefaultIteratorEnvironment());

    // try seeking to the beginning of a key that aggregates

    ai.seek(newRow(1, 1, 1, 3, false), EMPTY_COL_FAMS, false);

    assertFalse(ai.hasTop());

  }

  @SuppressWarnings("deprecation")
  @Test
  public void test7() throws IOException {
    // test that delete is not aggregated

    TreeMap<Key,Value> tm1 = new TreeMap<>();

    newKeyValue(tm1, 1, 1, 1, 2, true, "");
    newKeyValue(tm1, 1, 1, 1, 3, false, "4");
    newKeyValue(tm1, 1, 1, 1, 4, false, "3");

    AggregatingIterator ai = new AggregatingIterator();

    Map<String,String> opts = new HashMap<>();

    opts.put("cf001", SummationAggregator.class.getName());

    ai.init(new SortedMapIterator(tm1), opts, new DefaultIteratorEnvironment());

    ai.seek(newRow(1, 1, 1, 4, true), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 4), ai.getTopKey());
    assertEquals("7", ai.getTopValue().toString());

    ai.next();
    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 2, true), ai.getTopKey());
    assertEquals("", ai.getTopValue().toString());

    ai.next();
    assertFalse(ai.hasTop());

    tm1 = new TreeMap<>();
    newKeyValue(tm1, 1, 1, 1, 2, true, "");
    ai = new AggregatingIterator();
    ai.init(new SortedMapIterator(tm1), opts, new DefaultIteratorEnvironment());

    ai.seek(newRow(1, 1, 1, 4, true), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(newKey(1, 1, 1, 2, true), ai.getTopKey());
    assertEquals("", ai.getTopValue().toString());

    ai.next();
    assertFalse(ai.hasTop());

  }
}

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.Combiner.ValueIterator;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.LongCombiner.FixedLenEncoder;
import org.apache.accumulo.core.iterators.LongCombiner.StringEncoder;
import org.apache.accumulo.core.iterators.LongCombiner.VarLenEncoder;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class CombinerTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

  static Key nk(int row, int colf, int colq, long ts, boolean deleted) {
    Key k = nk(row, colf, colq, ts);
    k.setDeleted(deleted);
    return k;
  }

  static Key nk(int row, int colf, int colq, long ts) {
    return new Key(nr(row), new Text(String.format("cf%03d", colf)), new Text(String.format("cq%03d", colq)), ts);
  }

  static Range nr(int row, int colf, int colq, long ts, boolean inclusive) {
    return new Range(nk(row, colf, colq, ts), inclusive, null, true);
  }

  static Range nr(int row, int colf, int colq, long ts) {
    return nr(row, colf, colq, ts, true);
  }

  static <V> void nkv(TreeMap<Key,Value> tm, int row, int colf, int colq, long ts, boolean deleted, V val, Encoder<V> encoder) {
    Key k = nk(row, colf, colq, ts);
    k.setDeleted(deleted);
    tm.put(k, new Value(encoder.encode(val)));
  }

  static Text nr(int row) {
    return new Text(String.format("r%03d", row));
  }

  @Test
  public void test1() throws IOException {
    Encoder<Long> encoder = LongCombiner.VAR_LEN_ENCODER;

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that do not aggregate
    nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
    nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
    nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);

    Combiner ai = new SummingCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, SummingCombiner.Type.VARLEN);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("2")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("4", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 1), ai.getTopKey());
    assertEquals("2", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());

    // try seeking

    ai.seek(nr(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 1), ai.getTopKey());
    assertEquals("2", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());

    // seek after everything
    ai.seek(nr(1, 1, 1, 0), EMPTY_COL_FAMS, false);

    assertFalse(ai.hasTop());

  }

  @Test
  public void test2() throws IOException {
    Encoder<Long> encoder = LongCombiner.VAR_LEN_ENCODER;

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that aggregate
    nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
    nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
    nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);

    Combiner ai = new SummingCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, VarLenEncoder.class);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());

    // try seeking to the beginning of a key that aggregates

    ai.seek(nr(1, 1, 1, 3), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());

    // try seeking the middle of a key the aggregates
    ai.seek(nr(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertFalse(ai.hasTop());

    // try seeking to the end of a key the aggregates
    ai.seek(nr(1, 1, 1, 1), EMPTY_COL_FAMS, false);

    assertFalse(ai.hasTop());

    // try seeking before a key the aggregates
    ai.seek(nr(1, 1, 1, 4), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());
  }

  @Test
  public void test3() throws IOException {
    Encoder<Long> encoder = LongCombiner.FIXED_LEN_ENCODER;

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that aggregate
    nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
    nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
    nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);

    // keys that do not aggregate
    nkv(tm1, 2, 2, 1, 1, false, 2l, encoder);
    nkv(tm1, 2, 2, 1, 2, false, 3l, encoder);

    Combiner ai = new SummingCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, FixedLenEncoder.class.getName());
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 1), ai.getTopKey());
    assertEquals("2", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());

    // seek after key that aggregates
    ai.seek(nr(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

    // seek before key that aggregates
    ai.seek(nr(1, 1, 1, 4), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

  }

  @Test
  public void testDeepCopy() throws IOException {
    Encoder<Long> encoder = LongCombiner.FIXED_LEN_ENCODER;

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that aggregate
    nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
    nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
    nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);

    // keys that do not aggregate
    nkv(tm1, 2, 2, 1, 1, false, 2l, encoder);
    nkv(tm1, 2, 2, 1, 2, false, 3l, encoder);

    Combiner ai = new SummingCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, FixedLenEncoder.class.getName());
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);

    SortedKeyValueIterator<Key,Value> ai2 = ai.deepCopy(null);
    SortedKeyValueIterator<Key,Value> ai3 = ai.deepCopy(null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 1), ai.getTopKey());
    assertEquals("2", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());

    // seek after key that aggregates
    ai2.seek(nr(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertTrue(ai2.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai2.getTopKey());
    assertEquals("3", encoder.decode(ai2.getTopValue().get()).toString());

    // seek before key that aggregates
    ai3.seek(nr(1, 1, 1, 4), EMPTY_COL_FAMS, false);

    assertTrue(ai3.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai3.getTopKey());
    assertEquals("9", encoder.decode(ai3.getTopValue().get()).toString());

    ai3.next();

    assertTrue(ai3.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai3.getTopKey());
    assertEquals("3", encoder.decode(ai3.getTopValue().get()).toString());
  }

  @Test
  public void test4() throws IOException {
    Encoder<Long> encoder = LongCombiner.STRING_ENCODER;

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that do not aggregate
    nkv(tm1, 0, 0, 1, 1, false, 7l, encoder);

    // keys that aggregate
    nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
    nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
    nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);

    // keys that do not aggregate
    nkv(tm1, 2, 2, 1, 1, false, 2l, encoder);
    nkv(tm1, 2, 2, 1, 2, false, 3l, encoder);

    Combiner ai = new SummingCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(0, 0, 1, 1), ai.getTopKey());
    assertEquals("7", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 1), ai.getTopKey());
    assertEquals("2", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());

    // seek test
    ai.seek(nr(0, 0, 1, 0), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

    // seek after key that aggregates
    ai.seek(nr(1, 1, 1, 2), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai.getTopKey());
    assertEquals("3", encoder.decode(ai.getTopValue().get()).toString());

    // combine all columns

    is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    Combiner.setCombineAllColumns(is, true);

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(0, 0, 1, 1), ai.getTopKey());
    assertEquals("7", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertTrue(ai.hasTop());
    assertEquals(nk(2, 2, 1, 2), ai.getTopKey());
    assertEquals("5", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());
  }

  @Test
  public void test5() throws IOException {
    Encoder<Long> encoder = LongCombiner.STRING_ENCODER;
    // try aggregating across multiple data sets that contain
    // the exact same keys w/ different values

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);

    TreeMap<Key,Value> tm2 = new TreeMap<Key,Value>();
    nkv(tm2, 1, 1, 1, 1, false, 3l, encoder);

    TreeMap<Key,Value> tm3 = new TreeMap<Key,Value>();
    nkv(tm3, 1, 1, 1, 1, false, 4l, encoder);

    Combiner ai = new SummingCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, StringEncoder.class);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    List<SortedKeyValueIterator<Key,Value>> sources = new ArrayList<SortedKeyValueIterator<Key,Value>>(3);
    sources.add(new SortedMapIterator(tm1));
    sources.add(new SortedMapIterator(tm2));
    sources.add(new SortedMapIterator(tm3));

    MultiIterator mi = new MultiIterator(sources, true);
    ai.init(mi, is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 1), ai.getTopKey());
    assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());
  }

  @Test
  public void test6() throws IOException {
    Encoder<Long> encoder = LongCombiner.VAR_LEN_ENCODER;
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that aggregate
    nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
    nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
    nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);

    Combiner ai = new SummingCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, VarLenEncoder.class.getName());
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), new DefaultIteratorEnvironment());

    // try seeking to the beginning of a key that aggregates

    ai.seek(nr(1, 1, 1, 3, false), EMPTY_COL_FAMS, false);

    assertFalse(ai.hasTop());

  }

  @Test
  public void test7() throws IOException {
    Encoder<Long> encoder = LongCombiner.FIXED_LEN_ENCODER;

    // test that delete is not aggregated

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    nkv(tm1, 1, 1, 1, 2, true, 0l, encoder);
    nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);
    nkv(tm1, 1, 1, 1, 4, false, 3l, encoder);

    Combiner ai = new SummingCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, SummingCombiner.Type.FIXEDLEN);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), new DefaultIteratorEnvironment());

    ai.seek(nr(1, 1, 1, 4, true), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 4), ai.getTopKey());
    assertEquals("7", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();
    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 2, true), ai.getTopKey());
    assertEquals("0", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();
    assertFalse(ai.hasTop());

    tm1 = new TreeMap<Key,Value>();
    nkv(tm1, 1, 1, 1, 2, true, 0l, encoder);
    ai = new SummingCombiner();
    ai.init(new SortedMapIterator(tm1), is.getOptions(), new DefaultIteratorEnvironment());

    ai.seek(nr(1, 1, 1, 4, true), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 2, true), ai.getTopKey());
    assertEquals("0", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();
    assertFalse(ai.hasTop());
  }

  @Test
  public void valueIteratorTest() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    tm.put(new Key("r", "f", "q", 1), new Value("1".getBytes()));
    tm.put(new Key("r", "f", "q", 2), new Value("2".getBytes()));
    SortedMapIterator smi = new SortedMapIterator(tm);
    smi.seek(new Range(), EMPTY_COL_FAMS, false);
    ValueIterator iter = new ValueIterator(smi);
    assertEquals(iter.next().toString(), "2");
    assertEquals(iter.next().toString(), "1");
    assertFalse(iter.hasNext());
  }

  @Test
  public void sumAllColumns() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    tm.put(new Key("r", "count", "a", 1), new Value("1".getBytes()));
    tm.put(new Key("r", "count", "a", 2), new Value("1".getBytes()));
    tm.put(new Key("r", "count", "b", 3), new Value("1".getBytes()));
    tm.put(new Key("r", "count", "b", 4), new Value("1".getBytes()));
    tm.put(new Key("r", "count", "b", 5), new Value("1".getBytes()));
    tm.put(new Key("r", "count", "c", 6), new Value("1".getBytes()));
    SortedMapIterator smi = new SortedMapIterator(tm);
    Combiner iter = new SummingCombiner();
    IteratorSetting s = new IteratorSetting(10, "s", SummingCombiner.class);
    SummingCombiner.setColumns(s, Collections.singletonList(new IteratorSetting.Column("count")));
    SummingCombiner.setEncodingType(s, LongCombiner.StringEncoder.class);
    iter.init(smi, s.getOptions(), new DefaultIteratorEnvironment());
    Combiner iter2 = new SummingCombiner();
    IteratorSetting s2 = new IteratorSetting(10, "s2", SummingCombiner.class);
    SummingCombiner.setColumns(s2, Collections.singletonList(new IteratorSetting.Column("count", "a")));
    SummingCombiner.setEncodingType(s2, LongCombiner.StringEncoder.class);
    iter2.init(iter, s.getOptions(), new DefaultIteratorEnvironment());
    iter2.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter2.hasTop());
    assertEquals("2", iter2.getTopValue().toString());
    iter2.next();
    assertTrue(iter2.hasTop());
    assertEquals("3", iter2.getTopValue().toString());
    iter2.next();
    assertTrue(iter2.hasTop());
    assertEquals("1", iter2.getTopValue().toString());
    iter2.next();
    assertFalse(iter2.hasTop());
  }

  @Test
  public void maxMinTest() throws IOException {
    Encoder<Long> encoder = LongCombiner.VAR_LEN_ENCODER;

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that aggregate
    nkv(tm1, 1, 1, 1, 1, false, 4l, encoder);
    nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
    nkv(tm1, 1, 1, 1, 3, false, 2l, encoder);

    Combiner ai = new MaxCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingCombiner.class);
    LongCombiner.setEncodingType(is, SummingCombiner.Type.VARLEN);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("4", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());

    ai = new MinCombiner();

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals("2", encoder.decode(ai.getTopValue().get()).toString());

    ai.next();

    assertFalse(ai.hasTop());
  }

  public static List<Long> nal(Long... longs) {
    List<Long> al = new ArrayList<Long>(longs.length);
    for (Long l : longs) {
      al.add(l);
    }
    return al;
  }

  public static void assertBytesEqual(byte[] a, byte[] b) {
    assertEquals(a.length, b.length);
    for (int i = 0; i < a.length; i++)
      assertEquals(a[i], b[i]);
  }

  public static void sumArray(Class<? extends Encoder<List<Long>>> encoderClass, SummingArrayCombiner.Type type) throws IOException, InstantiationException,
      IllegalAccessException {
    Encoder<List<Long>> encoder = encoderClass.newInstance();

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    // keys that aggregate
    nkv(tm1, 1, 1, 1, 1, false, nal(1l, 2l), encoder);
    nkv(tm1, 1, 1, 1, 2, false, nal(3l, 4l, 5l), encoder);
    nkv(tm1, 1, 1, 1, 3, false, nal(), encoder);

    Combiner ai = new SummingArrayCombiner();

    IteratorSetting is = new IteratorSetting(1, SummingArrayCombiner.class);
    SummingArrayCombiner.setEncodingType(is, type);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertBytesEqual(encoder.encode(nal(4l, 6l, 5l)), ai.getTopValue().get());

    ai.next();

    assertFalse(ai.hasTop());

    is.clearOptions();
    SummingArrayCombiner.setEncodingType(is, encoderClass);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertBytesEqual(encoder.encode(nal(4l, 6l, 5l)), ai.getTopValue().get());

    ai.next();

    assertFalse(ai.hasTop());

    is.clearOptions();
    SummingArrayCombiner.setEncodingType(is, encoderClass.getName());
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
    assertBytesEqual(encoder.encode(nal(4l, 6l, 5l)), ai.getTopValue().get());

    ai.next();

    assertFalse(ai.hasTop());

    is.clearOptions();
    SummingArrayCombiner.setEncodingType(is, SummingCombiner.VAR_LEN_ENCODER.getClass().getName());
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    try {
      ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
      Assert.fail();
    } catch (IllegalArgumentException e) {}

    is.clearOptions();
    SummingArrayCombiner.setEncodingType(is, BadEncoder.class.getName());
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("cf001")));

    try {
      ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
      Assert.fail();
    } catch (IllegalArgumentException e) {}
  }

  public static class BadEncoder implements Encoder<List<Long>> {
    @Override
    public byte[] encode(List<Long> v) {
      return new byte[0];
    }

    @Override
    public List<Long> decode(byte[] b) {
      return new ArrayList<Long>();
    }

  }

  @Test
  public void sumArrayTest() throws IOException, InstantiationException, IllegalAccessException {
    sumArray(SummingArrayCombiner.VarLongArrayEncoder.class, SummingArrayCombiner.Type.VARLEN);
    sumArray(SummingArrayCombiner.FixedLongArrayEncoder.class, SummingArrayCombiner.Type.FIXEDLEN);
    sumArray(SummingArrayCombiner.StringArrayEncoder.class, SummingArrayCombiner.Type.STRING);
  }

  @Test
  public void testEncoders() {
    TypedValueCombiner.testEncoder(SummingCombiner.FIXED_LEN_ENCODER, Long.MAX_VALUE);
    TypedValueCombiner.testEncoder(SummingCombiner.FIXED_LEN_ENCODER, Long.MIN_VALUE);
    TypedValueCombiner.testEncoder(SummingCombiner.FIXED_LEN_ENCODER, 42l);
    TypedValueCombiner.testEncoder(SummingCombiner.FIXED_LEN_ENCODER, -42l);
    TypedValueCombiner.testEncoder(SummingCombiner.FIXED_LEN_ENCODER, 0l);
    TypedValueCombiner.testEncoder(SummingCombiner.VAR_LEN_ENCODER, Long.MAX_VALUE);
    TypedValueCombiner.testEncoder(SummingCombiner.VAR_LEN_ENCODER, Long.MIN_VALUE);
    TypedValueCombiner.testEncoder(SummingCombiner.VAR_LEN_ENCODER, 42l);
    TypedValueCombiner.testEncoder(SummingCombiner.VAR_LEN_ENCODER, -42l);
    TypedValueCombiner.testEncoder(SummingCombiner.VAR_LEN_ENCODER, 0l);
    TypedValueCombiner.testEncoder(SummingCombiner.STRING_ENCODER, Long.MAX_VALUE);
    TypedValueCombiner.testEncoder(SummingCombiner.STRING_ENCODER, Long.MIN_VALUE);
    TypedValueCombiner.testEncoder(SummingCombiner.STRING_ENCODER, 42l);
    TypedValueCombiner.testEncoder(SummingCombiner.STRING_ENCODER, -42l);
    TypedValueCombiner.testEncoder(SummingCombiner.STRING_ENCODER, 0l);

    TypedValueCombiner.testEncoder(SummingArrayCombiner.FIXED_LONG_ARRAY_ENCODER, Arrays.asList(0l, -1l, 10l, Long.MAX_VALUE, Long.MIN_VALUE));
    TypedValueCombiner.testEncoder(SummingArrayCombiner.VAR_LONG_ARRAY_ENCODER, Arrays.asList(0l, -1l, 10l, Long.MAX_VALUE, Long.MIN_VALUE));
    TypedValueCombiner.testEncoder(SummingArrayCombiner.STRING_ARRAY_ENCODER, Arrays.asList(0l, -1l, 10l, Long.MAX_VALUE, Long.MIN_VALUE));
  }

  @Test
  public void testAdds() {
    assertEquals(LongCombiner.safeAdd(Long.MIN_VALUE + 5, -10), Long.MIN_VALUE);
    assertEquals(LongCombiner.safeAdd(Long.MAX_VALUE - 5, 10), Long.MAX_VALUE);
    assertEquals(LongCombiner.safeAdd(Long.MIN_VALUE + 5, -5), Long.MIN_VALUE);
    assertEquals(LongCombiner.safeAdd(Long.MAX_VALUE - 5, 5), Long.MAX_VALUE);
  }

}

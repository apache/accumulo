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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.system.MultiIteratorTest;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.junit.Assert;
import org.junit.Test;

public class IteratorUtilTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  static class WrappedIter implements SortedKeyValueIterator<Key,Value> {

    protected SortedKeyValueIterator<Key,Value> source;

    @Override
    public WrappedIter deepCopy(IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Key getTopKey() {
      return source.getTopKey();
    }

    @Override
    public Value getTopValue() {
      return source.getTopValue();
    }

    @Override
    public boolean hasTop() {
      return source.hasTop();
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      this.source = source;
    }

    @Override
    public void next() throws IOException {
      source.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      source.seek(range, columnFamilies, inclusive);
    }
  }

  static class AddingIter extends WrappedIter {

    int amount = 1;

    @Override
    public Value getTopValue() {
      Value val = super.getTopValue();

      int orig = Integer.parseInt(val.toString());

      return new Value(((orig + amount) + "").getBytes());
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      String amount = options.get("amount");

      if (amount != null) {
        this.amount = Integer.parseInt(amount);
      }
    }
  }

  static class SquaringIter extends WrappedIter {
    @Override
    public Value getTopValue() {
      Value val = super.getTopValue();

      int orig = Integer.parseInt(val.toString());

      return new Value(((orig * orig) + "").getBytes());
    }
  }

  @Test
  public void test1() throws IOException {
    ConfigurationCopy conf = new ConfigurationCopy();

    // create an iterator that adds 1 and then squares
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".addIter", "1," + AddingIter.class.getName());
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".sqIter", "2," + SquaringIter.class.getName());

    TreeMap<Key,Value> tm = new TreeMap<>();

    MultiIteratorTest.newKeyValue(tm, 1, 0, false, "1");
    MultiIteratorTest.newKeyValue(tm, 2, 0, false, "2");

    SortedMapIterator source = new SortedMapIterator(tm);

    SortedKeyValueIterator<Key,Value> iter = IteratorUtil.loadIterators(IteratorScope.minc, source, new KeyExtent(Table.ID.of("tab"), null, null), conf,
        new DefaultIteratorEnvironment(conf));
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(1, 0)));
    assertTrue(iter.getTopValue().toString().equals("4"));

    iter.next();

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(2, 0)));
    assertTrue(iter.getTopValue().toString().equals("9"));

    iter.next();

    assertFalse(iter.hasTop());
  }

  @Test
  public void test4() throws IOException {

    // try loading for a different scope
    AccumuloConfiguration conf = new ConfigurationCopy();

    TreeMap<Key,Value> tm = new TreeMap<>();

    MultiIteratorTest.newKeyValue(tm, 1, 0, false, "1");
    MultiIteratorTest.newKeyValue(tm, 2, 0, false, "2");

    SortedMapIterator source = new SortedMapIterator(tm);

    SortedKeyValueIterator<Key,Value> iter = IteratorUtil.loadIterators(IteratorScope.majc, source, new KeyExtent(Table.ID.of("tab"), null, null), conf,
        new DefaultIteratorEnvironment(conf));
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(1, 0)));
    assertTrue(iter.getTopValue().toString().equals("1"));

    iter.next();

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(2, 0)));
    assertTrue(iter.getTopValue().toString().equals("2"));

    iter.next();

    assertFalse(iter.hasTop());

  }

  @Test
  public void test3() throws IOException {
    // change the load order, so it squares and then adds

    ConfigurationCopy conf = new ConfigurationCopy();

    TreeMap<Key,Value> tm = new TreeMap<>();

    MultiIteratorTest.newKeyValue(tm, 1, 0, false, "1");
    MultiIteratorTest.newKeyValue(tm, 2, 0, false, "2");

    SortedMapIterator source = new SortedMapIterator(tm);

    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".addIter", "2," + AddingIter.class.getName());
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".sqIter", "1," + SquaringIter.class.getName());

    SortedKeyValueIterator<Key,Value> iter = IteratorUtil.loadIterators(IteratorScope.minc, source, new KeyExtent(Table.ID.of("tab"), null, null), conf,
        new DefaultIteratorEnvironment(conf));
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(1, 0)));
    assertTrue(iter.getTopValue().toString().equals("2"));

    iter.next();

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(2, 0)));
    assertTrue(iter.getTopValue().toString().equals("5"));

    iter.next();

    assertFalse(iter.hasTop());
  }

  @Test
  public void test2() throws IOException {

    ConfigurationCopy conf = new ConfigurationCopy();

    // create an iterator that adds 1 and then squares
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".addIter", "1," + AddingIter.class.getName());
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".addIter.opt.amount", "7");
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".sqIter", "2," + SquaringIter.class.getName());

    TreeMap<Key,Value> tm = new TreeMap<>();

    MultiIteratorTest.newKeyValue(tm, 1, 0, false, "1");
    MultiIteratorTest.newKeyValue(tm, 2, 0, false, "2");

    SortedMapIterator source = new SortedMapIterator(tm);

    SortedKeyValueIterator<Key,Value> iter = IteratorUtil.loadIterators(IteratorScope.minc, source, new KeyExtent(Table.ID.of("tab"), null, null), conf,
        new DefaultIteratorEnvironment(conf));
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(1, 0)));
    assertTrue(iter.getTopValue().toString().equals("64"));

    iter.next();

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(2, 0)));
    assertTrue(iter.getTopValue().toString().equals("81"));

    iter.next();

    assertFalse(iter.hasTop());

  }

  @Test
  public void test5() throws IOException {
    ConfigurationCopy conf = new ConfigurationCopy();

    // create an iterator that ages off
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".filter", "1," + AgeOffFilter.class.getName());
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".filter.opt.ttl", "100");
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".filter.opt.currentTime", "1000");

    TreeMap<Key,Value> tm = new TreeMap<>();

    MultiIteratorTest.newKeyValue(tm, 1, 850, false, "1");
    MultiIteratorTest.newKeyValue(tm, 2, 950, false, "2");

    SortedMapIterator source = new SortedMapIterator(tm);

    SortedKeyValueIterator<Key,Value> iter = IteratorUtil.loadIterators(IteratorScope.minc, source, new KeyExtent(Table.ID.of("tab"), null, null), conf,
        new DefaultIteratorEnvironment(conf));
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertTrue(iter.getTopKey().equals(MultiIteratorTest.newKey(2, 950)));
    iter.next();

    assertFalse(iter.hasTop());

  }

  @Test
  public void onlyReadsRelevantIteratorScopeConfigurations() throws Exception {
    Map<String,String> data = new HashMap<>();

    // Make some configuration items, one with a bogus scope
    data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foo", "50," + SummingCombiner.class.getName());
    data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foo.opt." + SummingCombiner.ALL_OPTION, "true");
    data.put(Property.TABLE_ITERATOR_PREFIX + ".fakescope.bar", "50," + SummingCombiner.class.getName());
    data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foo.opt.fakeopt", "fakevalue");

    AccumuloConfiguration conf = new ConfigurationCopy(data);

    List<IterInfo> iterators = new ArrayList<>();
    Map<String,Map<String,String>> options = new HashMap<>();

    IteratorUtil.parseIterConf(IteratorScope.scan, iterators, options, conf);

    Assert.assertEquals(1, iterators.size());
    IterInfo ii = iterators.get(0);
    Assert.assertEquals(new IterInfo(50, SummingCombiner.class.getName(), "foo"), ii);
  }
}

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
package org.apache.accumulo.core.iteratorsImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIteratorTest;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IteratorConfigUtilTest {

  private static final Logger log = LoggerFactory.getLogger(IteratorConfigUtilTest.class);
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final List<IterInfo> EMPTY_ITERS = Collections.emptyList();

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
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      this.source = source;
    }

    @Override
    public void next() throws IOException {
      source.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {
      source.seek(range, columnFamilies, inclusive);
    }
  }

  static class AddingIter extends WrappedIter {

    int amount = 1;

    @Override
    public Value getTopValue() {
      Value val = super.getTopValue();

      int orig = Integer.parseInt(val.toString());

      return new Value((orig + amount) + "");
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
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

      return new Value((orig * orig) + "");
    }
  }

  private SortedKeyValueIterator<Key,Value> createIter(IteratorScope scope,
      SortedMapIterator source, AccumuloConfiguration conf) throws IOException {
    var ibEnv = IteratorConfigUtil.loadIterConf(scope, EMPTY_ITERS, new HashMap<>(), conf);
    var iteratorBuilder =
        ibEnv.env(new DefaultIteratorEnvironment(conf)).useClassLoader(null).build();
    return IteratorConfigUtil.loadIterators(source, iteratorBuilder);
  }

  @Test
  public void test1() throws IOException {
    ConfigurationCopy conf = new ConfigurationCopy();

    // create an iterator that adds 1 and then squares
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".addIter",
        "1," + AddingIter.class.getName());
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".sqIter",
        "2," + SquaringIter.class.getName());

    TreeMap<Key,Value> tm = new TreeMap<>();

    MultiIteratorTest.newKeyValue(tm, 1, 0, false, "1");
    MultiIteratorTest.newKeyValue(tm, 2, 0, false, "2");

    SortedMapIterator source = new SortedMapIterator(tm);
    SortedKeyValueIterator<Key,Value> iter = createIter(IteratorScope.minc, source, conf);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(1, 0));
    assertEquals("4", iter.getTopValue().toString());

    iter.next();

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(2, 0));
    assertEquals("9", iter.getTopValue().toString());

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

    SortedKeyValueIterator<Key,Value> iter = createIter(IteratorScope.majc, source, conf);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(1, 0));
    assertEquals("1", iter.getTopValue().toString());

    iter.next();

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(2, 0));
    assertEquals("2", iter.getTopValue().toString());

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

    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".addIter",
        "2," + AddingIter.class.getName());
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".sqIter",
        "1," + SquaringIter.class.getName());

    SortedKeyValueIterator<Key,Value> iter = createIter(IteratorScope.minc, source, conf);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(1, 0));
    assertEquals("2", iter.getTopValue().toString());

    iter.next();

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(2, 0));
    assertEquals("5", iter.getTopValue().toString());

    iter.next();

    assertFalse(iter.hasTop());
  }

  @Test
  public void test2() throws IOException {

    ConfigurationCopy conf = new ConfigurationCopy();

    // create an iterator that adds 1 and then squares
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".addIter",
        "1," + AddingIter.class.getName());
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".addIter.opt.amount",
        "7");
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".sqIter",
        "2," + SquaringIter.class.getName());

    TreeMap<Key,Value> tm = new TreeMap<>();

    MultiIteratorTest.newKeyValue(tm, 1, 0, false, "1");
    MultiIteratorTest.newKeyValue(tm, 2, 0, false, "2");

    SortedMapIterator source = new SortedMapIterator(tm);

    SortedKeyValueIterator<Key,Value> iter = createIter(IteratorScope.minc, source, conf);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(1, 0));
    assertEquals("64", iter.getTopValue().toString());

    iter.next();

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(2, 0));
    assertEquals("81", iter.getTopValue().toString());

    iter.next();

    assertFalse(iter.hasTop());

  }

  @Test
  public void test5() throws IOException {
    ConfigurationCopy conf = new ConfigurationCopy();

    // create an iterator that ages off
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".filter",
        "1," + AgeOffFilter.class.getName());
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".filter.opt.ttl", "100");
    conf.set(Property.TABLE_ITERATOR_PREFIX + IteratorScope.minc.name() + ".filter.opt.currentTime",
        "1000");

    TreeMap<Key,Value> tm = new TreeMap<>();

    MultiIteratorTest.newKeyValue(tm, 1, 850, false, "1");
    MultiIteratorTest.newKeyValue(tm, 2, 950, false, "2");

    SortedMapIterator source = new SortedMapIterator(tm);

    SortedKeyValueIterator<Key,Value> iter = createIter(IteratorScope.minc, source, conf);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(iter.hasTop());
    assertEquals(iter.getTopKey(), MultiIteratorTest.newKey(2, 950));
    iter.next();

    assertFalse(iter.hasTop());

  }

  @Test
  public void onlyReadsRelevantIteratorScopeConfigurations() {
    Map<String,String> data = new HashMap<>();

    // Make some configuration items, one with a bogus scope
    data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foo", "50," + SummingCombiner.class.getName());
    data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foo.opt.all", "true");
    data.put(Property.TABLE_ITERATOR_PREFIX + ".fakescope.bar",
        "50," + SummingCombiner.class.getName());
    data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foo.opt.fakeopt", "fakevalue");

    AccumuloConfiguration conf = new ConfigurationCopy(data);

    List<IterInfo> iterators =
        IteratorConfigUtil.parseIterConf(IteratorScope.scan, EMPTY_ITERS, new HashMap<>(), conf);

    assertEquals(1, iterators.size());
    IterInfo ii = iterators.get(0);
    assertEquals(new IterInfo(50, SummingCombiner.class.getName(), "foo"), ii);
  }

  /**
   * Iterators should not contain dots in the name. Also, if the split size on "." is greater than
   * one, it should be 3, i.e., itername.opt.optname
   */
  @Test
  public void testInvalidIteratorFormats() {

    Map<String,String> data = new HashMap<>();
    Map<String,Map<String,String>> options = new HashMap<>();
    AccumuloConfiguration conf;

    // create iterator with 'dot' in name
    List<IterInfo> iterators = new ArrayList<>();
    try {
      data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foo.bar",
          "50," + SummingCombiner.class.getName());
      conf = new ConfigurationCopy(data);
      iterators = IteratorConfigUtil.parseIterConf(IteratorScope.scan, iterators, options, conf);
    } catch (IllegalArgumentException ex) {
      log.debug("caught expected exception: " + ex.getMessage());
    }
    data.clear();
    iterators.clear();
    options.clear();

    // create iterator with 'dot' in name and with split size of 3. If split size of three, then
    // second part must be 'opt'.
    try {
      data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foo.bar.baz",
          "49," + SummingCombiner.class.getName());
      conf = new ConfigurationCopy(data);
      iterators = IteratorConfigUtil.parseIterConf(IteratorScope.scan, iterators, options, conf);
    } catch (IllegalArgumentException ex) {
      log.debug("caught expected exception: " + ex.getMessage());
    }
    data.clear();
    iterators.clear();
    options.clear();

    // create iterator with invalid option format
    try {
      data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foobar",
          "48," + SummingCombiner.class.getName());
      data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foobar.opt", "fakevalue");
      conf = new ConfigurationCopy(data);
      iterators = IteratorConfigUtil.parseIterConf(IteratorScope.scan, iterators, options, conf);
      assertEquals(1, iterators.size());
      IterInfo ii = iterators.get(0);
      assertEquals(new IterInfo(48, SummingCombiner.class.getName(), "foobar"), ii);
    } catch (IllegalArgumentException ex) {
      log.debug("caught expected exception: " + ex.getMessage());
    }
    data.clear();
    iterators.clear();
    options.clear();

    // create iterator with 'opt' in incorrect position
    try {
      data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foobaz",
          "47," + SummingCombiner.class.getName());
      data.put(Property.TABLE_ITERATOR_SCAN_PREFIX + "foobaz.fake.opt", "fakevalue");
      conf = new ConfigurationCopy(data);
      iterators = IteratorConfigUtil.parseIterConf(IteratorScope.scan, iterators, options, conf);
      assertEquals(1, iterators.size());
      IterInfo ii = iterators.get(0);
      assertEquals(new IterInfo(47, SummingCombiner.class.getName(), "foobaz"), ii);
    } catch (IllegalArgumentException ex) {
      log.debug("caught expected exception: " + ex.getMessage());
    }
  }

}

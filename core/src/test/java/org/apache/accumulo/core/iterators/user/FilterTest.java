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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class FilterTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  private static final Map<String,String> EMPTY_OPTS = new HashMap<String,String>();

  public static class SimpleFilter extends Filter {
    public boolean accept(Key k, Value v) {
      // System.out.println(k.getRow());
      if (k.getRow().toString().endsWith("0"))
        return true;
      return false;
    }
  }

  public static class SimpleFilter2 extends Filter {
    public boolean accept(Key k, Value v) {
      if (k.getColumnFamily().toString().equals("a"))
        return false;
      return true;
    }
  }

  private static int size(SortedKeyValueIterator<Key,Value> iterator) throws IOException {
    int size = 0;
    while (iterator.hasTop()) {
      // System.out.println(iterator.getTopKey());
      size++;
      iterator.next();
    }
    return size;
  }

  @Test
  public void test1() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);

    Filter filter1 = new SimpleFilter();
    filter1.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    filter1.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter1);
    assertTrue("size = " + size, size == 100);

    Filter fi = new SimpleFilter();
    fi.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    Key k = new Key(new Text("500"));
    fi.seek(new Range(k, null), EMPTY_COL_FAMS, false);
    size = size(fi);
    assertTrue("size = " + size, size == 50);

    filter1 = new SimpleFilter();
    filter1.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    Filter filter2 = new SimpleFilter2();
    filter2.init(filter1, EMPTY_OPTS, null);
    filter2.seek(new Range(), EMPTY_COL_FAMS, false);
    size = size(filter2);
    assertTrue("size = " + size, size == 0);
  }

  @Test
  public void test1neg() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);

    Filter filter = new SimpleFilter();

    IteratorSetting is = new IteratorSetting(1, SimpleFilter.class);
    Filter.setNegate(is, true);

    filter.init(new SortedMapIterator(tm), is.getOptions(), null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter);
    assertTrue("size = " + size, size == 900);

    filter.init(new SortedMapIterator(tm), is.getOptions(), null);
    Key k = new Key(new Text("500"));
    filter.seek(new Range(k, null), EMPTY_COL_FAMS, false);
    size = size(filter);
    assertTrue("size = " + size, size == 450);

    filter.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    Filter filter2 = new SimpleFilter2();
    filter2.init(filter, is.getOptions(), null);
    filter2.seek(new Range(), EMPTY_COL_FAMS, false);
    size = size(filter2);
    assertTrue("size = " + size, size == 100);
  }

  @Test
  public void testDeepCopy() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);

    SimpleFilter filter = new SimpleFilter();

    IteratorSetting is = new IteratorSetting(1, SimpleFilter.class);
    Filter.setNegate(is, true);

    filter.init(new SortedMapIterator(tm), is.getOptions(), null);
    SortedKeyValueIterator<Key,Value> copy = filter.deepCopy(null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter);
    assertTrue("size = " + size, size == 900);
    copy.seek(new Range(), EMPTY_COL_FAMS, false);
    size = size(copy);
    assertTrue("size = " + size, size == 900);
  }

  @Test
  public void test2() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      k.setTimestamp(i);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);

    SortedKeyValueIterator<Key,Value> a = new AgeOffFilter();
    IteratorSetting is = new IteratorSetting(1, AgeOffFilter.class);
    AgeOffFilter.setTTL(is, 101l);
    AgeOffFilter.setCurrentTime(is, 1001l);
    AgeOffFilter.setNegate(is, true);
    assertTrue(((AgeOffFilter) a).validateOptions(is.getOptions()));
    try {
      ((AgeOffFilter) a).validateOptions(EMPTY_OPTS);
      assertTrue(false);
    } catch (IllegalArgumentException e) {}
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a = a.deepCopy(null);
    SortedKeyValueIterator<Key,Value> copy = a.deepCopy(null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 900);
    copy.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(copy), 900);
  }

  @Test
  public void test2a() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    IteratorSetting is = new IteratorSetting(1, ColumnAgeOffFilter.class);
    ColumnAgeOffFilter.addTTL(is, new IteratorSetting.Column("a"), 901l);
    long ts = System.currentTimeMillis();

    for (long i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq, ts - i);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);

    ColumnAgeOffFilter a = new ColumnAgeOffFilter();
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 902);

    ColumnAgeOffFilter.addTTL(is, new IteratorSetting.Column("a", "b"), 101l);
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 102);

    ColumnAgeOffFilter.removeTTL(is, new IteratorSetting.Column("a", "b"));
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a = (ColumnAgeOffFilter) a.deepCopy(null);
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 902);
  }

  @Test
  public void test3() throws IOException {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    HashSet<Column> hsc = new HashSet<Column>();
    hsc.add(new Column("c".getBytes(), null, null));

    Text colf1 = new Text("a");
    Text colq1 = new Text("b");
    Text colf2 = new Text("c");
    Text colq2 = new Text("d");
    Text colf;
    Text colq;
    for (int i = 0; i < 1000; i++) {
      if (Math.abs(Math.ceil(i / 2.0) - i / 2.0) < .001) {
        colf = colf1;
        colq = colq1;
      } else {
        colf = colf2;
        colq = colq2;
      }
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      k.setTimestamp(157l);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);

    ColumnQualifierFilter a = new ColumnQualifierFilter(new SortedMapIterator(tm), hsc);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 1000);

    hsc = new HashSet<Column>();
    hsc.add(new Column("a".getBytes(), "b".getBytes(), null));
    a = new ColumnQualifierFilter(new SortedMapIterator(tm), hsc);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(a);
    assertTrue("size was " + size, size == 500);

    hsc = new HashSet<Column>();
    a = new ColumnQualifierFilter(new SortedMapIterator(tm), hsc);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    size = size(a);
    assertTrue("size was " + size, size == 1000);
  }

  @Test
  public void test4() throws IOException {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    ColumnVisibility le1 = new ColumnVisibility("L1");
    ColumnVisibility le2 = new ColumnVisibility("L0&OFFICIAL");
    ColumnVisibility le3 = new ColumnVisibility("L1&L2");
    ColumnVisibility le4 = new ColumnVisibility("L1&L2&G1");
    ColumnVisibility[] lea = {le1, le2, le3, le4};
    Authorizations auths = new Authorizations("L1", "L2", "L0", "OFFICIAL");

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), new Text("a"), new Text("b"), new Text(lea[i % 4].getExpression()));
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);

    VisibilityFilter a = new VisibilityFilter(new SortedMapIterator(tm), auths, le2.getExpression());
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(a);
    assertTrue("size was " + size, size == 750);
  }

  private ColumnQualifierFilter ncqf(TreeMap<Key,Value> tm, Column... columns) throws IOException {
    HashSet<Column> hsc = new HashSet<Column>();

    for (Column column : columns) {
      hsc.add(column);
    }

    ColumnQualifierFilter a = new ColumnQualifierFilter(new SortedMapIterator(tm), hsc);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    return a;
  }

  @Test
  public void test5() throws IOException {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    tm.put(new Key(new Text(String.format("%03d", 1)), new Text("a"), new Text("x")), dv);
    tm.put(new Key(new Text(String.format("%03d", 2)), new Text("a"), new Text("y")), dv);
    tm.put(new Key(new Text(String.format("%03d", 3)), new Text("a"), new Text("z")), dv);
    tm.put(new Key(new Text(String.format("%03d", 4)), new Text("b"), new Text("x")), dv);
    tm.put(new Key(new Text(String.format("%03d", 5)), new Text("b"), new Text("y")), dv);

    assertTrue(tm.size() == 5);

    int size = size(ncqf(tm, new Column("c".getBytes(), null, null)));
    assertTrue(size == 5);

    size = size(ncqf(tm, new Column("a".getBytes(), null, null)));
    assertTrue(size == 5);

    size = size(ncqf(tm, new Column("a".getBytes(), "x".getBytes(), null)));
    assertTrue(size == 1);

    size = size(ncqf(tm, new Column("a".getBytes(), "x".getBytes(), null), new Column("b".getBytes(), "x".getBytes(), null)));
    assertTrue(size == 2);

    size = size(ncqf(tm, new Column("a".getBytes(), "x".getBytes(), null), new Column("b".getBytes(), "y".getBytes(), null)));
    assertTrue(size == 2);

    size = size(ncqf(tm, new Column("a".getBytes(), "x".getBytes(), null), new Column("b".getBytes(), null, null)));
    assertTrue(size == 3);
  }

  @Test
  public void testNoVisFilter() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    Value v = new Value();
    for (int i = 0; i < 1000; i++) {
      Key k = new Key(String.format("%03d", i), "a", "b", i % 10 == 0 ? "vis" : "");
      tm.put(k, v);
    }
    assertTrue(tm.size() == 1000);

    Filter filter = new ReqVisFilter();
    filter.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter);
    assertTrue("size = " + size, size == 100);
  }

  @Test
  public void testTimestampFilter() throws IOException, ParseException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    for (int i = 0; i < 100; i++) {
      Key k = new Key(new Text(String.format("%02d", i)), colf, colq);
      k.setTimestamp(i);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 100);

    SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMddHHmmssz");
    long baseTime = dateParser.parse("19990101000000GMT").getTime();
    tm.clear();
    for (int i = 0; i < 100; i++) {
      Key k = new Key(new Text(String.format("%02d", i)), colf, colq);
      k.setTimestamp(baseTime + (i * 1000));
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 100);
    TimestampFilter a = new TimestampFilter();
    IteratorSetting is = new IteratorSetting(1, TimestampFilter.class);
    TimestampFilter.setRange(is, "19990101010011GMT+01:00", "19990101010031GMT+01:00");
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a = (TimestampFilter) a.deepCopy(null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 21);
    TimestampFilter.setRange(is, baseTime + 11000, baseTime + 31000);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 21);

    TimestampFilter.setEnd(is, "19990101000031GMT", false);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 20);

    TimestampFilter.setStart(is, "19990101000011GMT", false);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 19);

    TimestampFilter.setEnd(is, "19990101000031GMT", true);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 20);

    is.clearOptions();
    TimestampFilter.setStart(is, "19990101000011GMT", true);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 89);

    TimestampFilter.setStart(is, "19990101000011GMT", false);
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 88);

    is.clearOptions();
    TimestampFilter.setEnd(is, "19990101000031GMT", true);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 32);

    TimestampFilter.setEnd(is, "19990101000031GMT", false);
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 31);

    TimestampFilter.setEnd(is, 253402300800001l, true);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);

    is.clearOptions();
    is.addOption(TimestampFilter.START, "19990101000011GMT");
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 89);

    is.clearOptions();
    is.addOption(TimestampFilter.END, "19990101000031GMT");
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(size(a), 32);

    try {
      a.validateOptions(EMPTY_OPTS);
      assertTrue(false);
    } catch (IllegalArgumentException e) {}
  }

  @Test
  public void testDeletes() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    Key k = new Key(new Text("0"), colf, colq);
    tm.put(k, dv);
    k = new Key(new Text("1"), colf, colq, 10);
    k.setDeleted(true);
    tm.put(k, dv);
    k = new Key(new Text("1"), colf, colq, 5);
    tm.put(k, dv);
    k = new Key(new Text("10"), colf, colq);
    tm.put(k, dv);

    assertTrue(tm.size() == 4);

    Filter filter = new SimpleFilter();
    filter.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter);
    assertTrue("size = " + size, size == 3);

  }
}

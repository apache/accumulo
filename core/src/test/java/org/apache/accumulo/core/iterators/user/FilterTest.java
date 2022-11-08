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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.accumulo.core.iteratorsImpl.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.iteratorsImpl.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class FilterTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final Map<String,String> EMPTY_OPTS = new HashMap<>();

  public static class SimpleFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      // System.out.println(k.getRow());
      return k.getRow().toString().endsWith("0");
    }
  }

  public static class SimpleFilter2 extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      return !k.getColumnFamily().toString().equals("a");
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
    TreeMap<Key,Value> tm = new TreeMap<>();

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    Filter filter1 = new SimpleFilter();
    filter1.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    filter1.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter1);
    assertEquals(100, size);

    Filter fi = new SimpleFilter();
    fi.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    Key k = new Key(new Text("500"));
    fi.seek(new Range(k, null), EMPTY_COL_FAMS, false);
    size = size(fi);
    assertEquals(50, size);

    filter1 = new SimpleFilter();
    filter1.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    Filter filter2 = new SimpleFilter2();
    filter2.init(filter1, EMPTY_OPTS, null);
    filter2.seek(new Range(), EMPTY_COL_FAMS, false);
    size = size(filter2);
    assertEquals(0, size);
  }

  @Test
  public void test1neg() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    Filter filter = new SimpleFilter();

    IteratorSetting is = new IteratorSetting(1, SimpleFilter.class);
    Filter.setNegate(is, true);

    filter.init(new SortedMapIterator(tm), is.getOptions(), null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter);
    assertEquals(900, size);

    filter.init(new SortedMapIterator(tm), is.getOptions(), null);
    Key k = new Key(new Text("500"));
    filter.seek(new Range(k, null), EMPTY_COL_FAMS, false);
    size = size(filter);
    assertEquals(450, size);

    filter.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    Filter filter2 = new SimpleFilter2();
    filter2.init(filter, is.getOptions(), null);
    filter2.seek(new Range(), EMPTY_COL_FAMS, false);
    size = size(filter2);
    assertEquals(100, size);
  }

  @Test
  public void testDeepCopy() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    SimpleFilter filter = new SimpleFilter();

    IteratorSetting is = new IteratorSetting(1, SimpleFilter.class);
    Filter.setNegate(is, true);

    filter.init(new SortedMapIterator(tm), is.getOptions(), null);
    SortedKeyValueIterator<Key,Value> copy = filter.deepCopy(null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter);
    assertEquals(900, size);
    copy.seek(new Range(), EMPTY_COL_FAMS, false);
    size = size(copy);
    assertEquals(900, size);
  }

  @Test
  public void test2() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      k.setTimestamp(i);
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    SortedKeyValueIterator<Key,Value> a = new AgeOffFilter();
    IteratorSetting is = new IteratorSetting(1, AgeOffFilter.class);
    AgeOffFilter.setTTL(is, 101L);
    AgeOffFilter.setCurrentTime(is, 1001L);
    AgeOffFilter.setNegate(is, true);
    final AgeOffFilter finalA = (AgeOffFilter) a;
    assertTrue((finalA.validateOptions(is.getOptions())));
    assertThrows(IllegalArgumentException.class, () -> finalA.validateOptions(EMPTY_OPTS));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a = a.deepCopy(null);
    SortedKeyValueIterator<Key,Value> copy = a.deepCopy(null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(900, size(a));
    copy.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(900, size(copy));
  }

  @Test
  public void test2a() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();
    IteratorSetting is = new IteratorSetting(1, ColumnAgeOffFilter.class);
    ColumnAgeOffFilter.addTTL(is, new IteratorSetting.Column("a"), 901L);
    long ts = System.currentTimeMillis();

    for (long i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq, ts - i);
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    ColumnAgeOffFilter a = new ColumnAgeOffFilter();
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(902, size(a));

    ColumnAgeOffFilter.addTTL(is, new IteratorSetting.Column("a", "b"), 101L);
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(102, size(a));

    ColumnAgeOffFilter.removeTTL(is, new IteratorSetting.Column("a", "b"));
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a = (ColumnAgeOffFilter) a.deepCopy(null);
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(902, size(a));
  }

  /**
   * Test for fix to ACCUMULO-1604: ColumnAgeOffFilter was throwing an error when using negate
   */
  @Test
  public void test2aNegate() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();
    IteratorSetting is = new IteratorSetting(1, ColumnAgeOffFilter.class);
    ColumnAgeOffFilter.addTTL(is, new IteratorSetting.Column("a"), 901L);
    ColumnAgeOffFilter.setNegate(is, true);
    long ts = System.currentTimeMillis();

    for (long i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq, ts - i);
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    ColumnAgeOffFilter a = new ColumnAgeOffFilter();
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(98, size(a));

    ColumnAgeOffFilter.addTTL(is, new IteratorSetting.Column("a", "b"), 101L);
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(898, size(a));

    ColumnAgeOffFilter.removeTTL(is, new IteratorSetting.Column("a", "b"));
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a = (ColumnAgeOffFilter) a.deepCopy(null);
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(98, size(a));
  }

  /**
   * Test for fix to ACCUMULO-1604: ColumnAgeOffFilter was throwing an error when using negate Test
   * case for when "negate" is an actual column name
   */
  @Test
  public void test2b() throws IOException {
    Text colf = new Text("negate");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();
    IteratorSetting is = new IteratorSetting(1, ColumnAgeOffFilter.class);
    ColumnAgeOffFilter.addTTL(is, new IteratorSetting.Column("negate"), 901L);
    long ts = System.currentTimeMillis();

    for (long i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq, ts - i);
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    ColumnAgeOffFilter a = new ColumnAgeOffFilter();
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(902, size(a));

    ColumnAgeOffFilter.addTTL(is, new IteratorSetting.Column("negate", "b"), 101L);
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(102, size(a));

    ColumnAgeOffFilter.removeTTL(is, new IteratorSetting.Column("negate", "b"));
    a.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
    a = (ColumnAgeOffFilter) a.deepCopy(null);
    a.overrideCurrentTime(ts);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(902, size(a));
  }

  @Test
  public void test3() throws IOException {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();
    HashSet<Column> hsc = new HashSet<>();
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
      k.setTimestamp(157L);
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    SortedKeyValueIterator<Key,Value> a =
        ColumnQualifierFilter.wrap(new SortedMapIterator(tm), hsc);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(1000, size(a));

    hsc = new HashSet<>();
    hsc.add(new Column("a".getBytes(), "b".getBytes(), null));
    a = ColumnQualifierFilter.wrap(new SortedMapIterator(tm), hsc);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(a);
    assertEquals(500, size);

    hsc = new HashSet<>();
    a = ColumnQualifierFilter.wrap(new SortedMapIterator(tm), hsc);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    size = size(a);
    assertEquals(1000, size);
  }

  @Test
  public void test4() throws IOException {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();

    ColumnVisibility le1 = new ColumnVisibility("L1");
    ColumnVisibility le2 = new ColumnVisibility("L0&OFFICIAL");
    ColumnVisibility le3 = new ColumnVisibility("L1&L2");
    ColumnVisibility le4 = new ColumnVisibility("L1&L2&G1");
    ColumnVisibility[] lea = {le1, le2, le3, le4};
    Authorizations auths = new Authorizations("L1", "L2", "L0", "OFFICIAL");

    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), new Text("a"), new Text("b"),
          new Text(lea[i % 4].getExpression()));
      tm.put(k, dv);
    }
    assertEquals(1000, tm.size());

    SortedKeyValueIterator<Key,Value> a =
        VisibilityFilter.wrap(new SortedMapIterator(tm), auths, le2.getExpression());
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(a);
    assertEquals(750, size);
  }

  private SortedKeyValueIterator<Key,Value> ncqf(TreeMap<Key,Value> tm, Column... columns)
      throws IOException {
    HashSet<Column> hsc = new HashSet<>();

    Collections.addAll(hsc, columns);

    SortedKeyValueIterator<Key,Value> a =
        ColumnQualifierFilter.wrap(new SortedMapIterator(tm), hsc);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    return a;
  }

  @Test
  public void test5() throws IOException {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();

    tm.put(new Key(new Text(String.format("%03d", 1)), new Text("a"), new Text("x")), dv);
    tm.put(new Key(new Text(String.format("%03d", 2)), new Text("a"), new Text("y")), dv);
    tm.put(new Key(new Text(String.format("%03d", 3)), new Text("a"), new Text("z")), dv);
    tm.put(new Key(new Text(String.format("%03d", 4)), new Text("b"), new Text("x")), dv);
    tm.put(new Key(new Text(String.format("%03d", 5)), new Text("b"), new Text("y")), dv);

    assertEquals(5, tm.size());

    int size = size(ncqf(tm, new Column("c".getBytes(), null, null)));
    assertEquals(5, size);

    size = size(ncqf(tm, new Column("a".getBytes(), null, null)));
    assertEquals(5, size);

    size = size(ncqf(tm, new Column("a".getBytes(), "x".getBytes(), null)));
    assertEquals(1, size);

    size = size(ncqf(tm, new Column("a".getBytes(), "x".getBytes(), null),
        new Column("b".getBytes(), "x".getBytes(), null)));
    assertEquals(2, size);

    size = size(ncqf(tm, new Column("a".getBytes(), "x".getBytes(), null),
        new Column("b".getBytes(), "y".getBytes(), null)));
    assertEquals(2, size);

    size = size(ncqf(tm, new Column("a".getBytes(), "x".getBytes(), null),
        new Column("b".getBytes(), null, null)));
    assertEquals(3, size);
  }

  @Test
  public void testNoVisFilter() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();
    Value v = new Value();
    for (int i = 0; i < 1000; i++) {
      Key k = new Key(String.format("%03d", i), "a", "b", i % 10 == 0 ? "vis" : "");
      tm.put(k, v);
    }
    assertEquals(1000, tm.size());

    Filter filter = new ReqVisFilter();
    filter.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter);
    assertEquals(100, size);
  }

  @Test
  public void testTimestampFilter() throws IOException, ParseException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();

    for (int i = 0; i < 100; i++) {
      Key k = new Key(new Text(String.format("%02d", i)), colf, colq);
      k.setTimestamp(i);
      tm.put(k, dv);
    }
    assertEquals(100, tm.size());

    SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMddHHmmssz");
    long baseTime = dateParser.parse("19990101000000GMT").getTime();
    tm.clear();
    for (int i = 0; i < 100; i++) {
      Key k = new Key(new Text(String.format("%02d", i)), colf, colq);
      k.setTimestamp(baseTime + (i * 1000));
      tm.put(k, dv);
    }
    assertEquals(100, tm.size());
    TimestampFilter a = new TimestampFilter();
    IteratorSetting is = new IteratorSetting(1, TimestampFilter.class);
    TimestampFilter.setRange(is, "19990101010011GMT+01:00", "19990101010031GMT+01:00");
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a = (TimestampFilter) a.deepCopy(null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(21, size(a));
    TimestampFilter.setRange(is, baseTime + 11000, baseTime + 31000);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(21, size(a));

    TimestampFilter.setEnd(is, "19990101000031GMT", false);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(20, size(a));

    TimestampFilter.setStart(is, "19990101000011GMT", false);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(19, size(a));

    TimestampFilter.setEnd(is, "19990101000031GMT", true);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(20, size(a));

    is.clearOptions();
    TimestampFilter.setStart(is, "19990101000011GMT", true);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(89, size(a));

    TimestampFilter.setStart(is, "19990101000011GMT", false);
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(88, size(a));

    is.clearOptions();
    TimestampFilter.setEnd(is, "19990101000031GMT", true);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(32, size(a));

    TimestampFilter.setEnd(is, "19990101000031GMT", false);
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(31, size(a));

    TimestampFilter.setEnd(is, 253402300800001L, true);
    a.init(new SortedMapIterator(tm), is.getOptions(), null);

    is.clearOptions();
    is.addOption(TimestampFilter.START, "19990101000011GMT");
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(89, size(a));

    is.clearOptions();
    is.addOption(TimestampFilter.END, "19990101000031GMT");
    assertTrue(a.validateOptions(is.getOptions()));
    a.init(new SortedMapIterator(tm), is.getOptions(), null);
    a.seek(new Range(), EMPTY_COL_FAMS, false);
    assertEquals(32, size(a));

    final TimestampFilter finalA = a;
    assertThrows(IllegalArgumentException.class, () -> finalA.validateOptions(EMPTY_OPTS));
  }

  @Test
  public void testDeletes() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<>();

    Key k = new Key(new Text("0"), colf, colq);
    tm.put(k, dv);
    k = new Key(new Text("1"), colf, colq, 10);
    k.setDeleted(true);
    tm.put(k, dv);
    k = new Key(new Text("1"), colf, colq, 5);
    tm.put(k, dv);
    k = new Key(new Text("10"), colf, colq);
    tm.put(k, dv);

    assertEquals(4, tm.size());

    Filter filter = new SimpleFilter();
    filter.init(new SortedMapIterator(tm), EMPTY_OPTS, null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);
    int size = size(filter);
    assertEquals(3, size);

  }
}

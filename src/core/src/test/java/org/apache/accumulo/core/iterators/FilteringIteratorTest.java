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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.filter.AgeOffFilter;
import org.apache.accumulo.core.iterators.filter.ColumnAgeOffFilter;
import org.apache.accumulo.core.iterators.filter.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.filter.Filter;
import org.apache.accumulo.core.iterators.filter.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/**
 * @deprecated since 1.4
 */
public class FilteringIteratorTest extends TestCase {
  
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  
  public class SimpleFilter implements Filter {
    public boolean accept(Key k, Value v) {
      if (k.getRow().toString().endsWith("0"))
        return true;
      return false;
    }
    
    @Override
    public void init(Map<String,String> options) {}
  }
  
  public class SimpleFilter2 implements Filter {
    public boolean accept(Key k, Value v) {
      if (k.getColumnFamily().toString().equals("a"))
        return false;
      return true;
    }
    
    @Override
    public void init(Map<String,String> options) {}
  }
  
  public void test1() {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    ArrayList<Filter> f = new ArrayList<Filter>();
    f.add(new SimpleFilter());
    
    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);
    
    int size = filter(tm, f);
    assertTrue("size = " + size, size == 100);
    
    try {
      FilteringIterator fi = new FilteringIterator(new SortedMapIterator(tm), f);
      Key k = new Key(new Text("500"));
      fi.seek(new Range(k, null), EMPTY_COL_FAMS, false);
      TreeMap<Key,Value> tmOut = new TreeMap<Key,Value>();
      while (fi.hasTop()) {
        tmOut.put(fi.getTopKey(), fi.getTopValue());
        fi.next();
      }
      assertTrue(tmOut.size() == 50);
    } catch (IOException e) {
      assertFalse(true);
    }
    
    f.add(new SimpleFilter2());
    size = filter(tm, f);
    assertTrue("tmOut wasn't empty " + size, size == 0);
  }
  
  public void test2() {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    ArrayList<Filter> f = new ArrayList<Filter>();
    AgeOffFilter a = new AgeOffFilter();
    HashMap<String,String> options = new HashMap<String,String>();
    options.put("ttl", "101");
    options.put("currentTime", "1001");
    a.init(options);
    f.add(a);
    
    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq);
      k.setTimestamp(i);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);
    
    int size = filter(tm, f);
    assertTrue(size == 100);
  }
  
  public void test2a() {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    ArrayList<Filter> f = new ArrayList<Filter>();
    ColumnAgeOffFilter a = new ColumnAgeOffFilter();
    HashMap<String,String> options = new HashMap<String,String>();
    options.put("a", "901");
    long ts = System.currentTimeMillis();
    a.init(options);
    a.overrideCurrentTime(ts);
    f.add(a);
    
    for (long i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), colf, colq, ts - i);
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);
    
    int size = filter(tm, f);
    assertTrue(size == 902);
    
    options.put("a:b", "101");
    a.init(options);
    a.overrideCurrentTime(ts);
    f.clear();
    f.add(a);
    size = filter(tm, f);
    assertTrue(size == 102);
  }
  
  public void test3() {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    ArrayList<Filter> f = new ArrayList<Filter>();
    HashSet<Column> hsc = new HashSet<Column>();
    hsc.add(new Column("c".getBytes(), null, null));
    ColumnQualifierFilter a = new ColumnQualifierFilter(hsc);
    f.add(a);
    
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
    
    int size = filter(tm, f);
    assertTrue(size == 1000);
    
    f = new ArrayList<Filter>();
    hsc = new HashSet<Column>();
    hsc.add(new Column("a".getBytes(), "b".getBytes(), null));
    a = new ColumnQualifierFilter(hsc);
    f.add(a);
    size = filter(tm, f);
    assertTrue("size was " + size, size == 500);
    
    f = new ArrayList<Filter>();
    hsc = new HashSet<Column>();
    a = new ColumnQualifierFilter(hsc);
    f.add(a);
    
    size = filter(tm, f);
    assertTrue("size was " + size, size == 1000);
  }
  
  private int filter(TreeMap<Key,Value> tm, ArrayList<Filter> f) {
    try {
      FilteringIterator fi = new FilteringIterator(new SortedMapIterator(tm), f);
      fi.seek(new Range(), EMPTY_COL_FAMS, false);
      TreeMap<Key,Value> tmOut = new TreeMap<Key,Value>();
      while (fi.hasTop()) {
        tmOut.put(fi.getTopKey(), fi.getTopValue());
        fi.next();
      }
      return tmOut.size();
    } catch (IOException ex) {
      fail(ex.getMessage());
      return -1;
    }
  }
  
  private int filter(TreeMap<Key,Value> tm, Filter f) {
    ArrayList<Filter> fl = new ArrayList<Filter>();
    fl.add(f);
    return filter(tm, fl);
  }
  
  public void test4() {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    ArrayList<Filter> f = new ArrayList<Filter>();
    
    ColumnVisibility le1 = new ColumnVisibility("L1");
    ColumnVisibility le2 = new ColumnVisibility("L0&OFFICIAL");
    ColumnVisibility le3 = new ColumnVisibility("L1&L2");
    ColumnVisibility le4 = new ColumnVisibility("L1&L2&G1");
    ColumnVisibility[] lea = {le1, le2, le3, le4};
    Authorizations auths = new Authorizations("L1", "L2", "L0", "OFFICIAL");
    VisibilityFilter a = new VisibilityFilter(auths, le2.getExpression());
    f.add(a);
    
    for (int i = 0; i < 1000; i++) {
      Key k = new Key(new Text(String.format("%03d", i)), new Text("a"), new Text("b"), new Text(lea[i % 4].getExpression()));
      tm.put(k, dv);
    }
    assertTrue(tm.size() == 1000);
    
    int size = filter(tm, f);
    assertTrue("size was " + size, size == 750);
  }
  
  private ColumnQualifierFilter ncqf(Column... columns) {
    HashSet<Column> hsc = new HashSet<Column>();
    
    for (Column column : columns) {
      hsc.add(column);
    }
    
    return new ColumnQualifierFilter(hsc);
  }
  
  public void test5() {
    Value dv = new Value();
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    
    tm.put(new Key(new Text(String.format("%03d", 1)), new Text("a"), new Text("x")), dv);
    tm.put(new Key(new Text(String.format("%03d", 2)), new Text("a"), new Text("y")), dv);
    tm.put(new Key(new Text(String.format("%03d", 3)), new Text("a"), new Text("z")), dv);
    tm.put(new Key(new Text(String.format("%03d", 4)), new Text("b"), new Text("x")), dv);
    tm.put(new Key(new Text(String.format("%03d", 5)), new Text("b"), new Text("y")), dv);
    
    assertTrue(tm.size() == 5);
    
    int size = filter(tm, ncqf(new Column("c".getBytes(), null, null)));
    assertTrue(size == 5);
    
    size = filter(tm, ncqf(new Column("a".getBytes(), null, null)));
    assertTrue(size == 5);
    
    size = filter(tm, ncqf(new Column("a".getBytes(), "x".getBytes(), null)));
    assertTrue(size == 1);
    
    size = filter(tm, ncqf(new Column("a".getBytes(), "x".getBytes(), null), new Column("b".getBytes(), "x".getBytes(), null)));
    assertTrue(size == 2);
    
    size = filter(tm, ncqf(new Column("a".getBytes(), "x".getBytes(), null), new Column("b".getBytes(), "y".getBytes(), null)));
    assertTrue(size == 2);
    
    size = filter(tm, ncqf(new Column("a".getBytes(), "x".getBytes(), null), new Column("b".getBytes(), null, null)));
    assertTrue(size == 3);
    
  }
  
}

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.ColumnAgeOffFilter;
import org.apache.accumulo.core.iterators.user.NoVisFilter;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class FilterTest extends TestCase {
    
    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
    
    public class SimpleFilter extends Filter {
        public SimpleFilter(SortedKeyValueIterator<Key,Value> iterator) {
            super(iterator);
        }
        
        public boolean accept(Key k, Value v) {
            // System.out.println(k.getRow());
            if (k.getRow().toString().endsWith("0")) return true;
            return false;
        }
    }
    
    public class SimpleFilter2 extends Filter {
        public SimpleFilter2(SortedKeyValueIterator<Key,Value> iterator) {
            super(iterator);
        }
        
        public boolean accept(Key k, Value v) {
            if (k.getColumnFamily().toString().equals("a")) return false;
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
        
        Filter filter1 = new SimpleFilter(new SortedMapIterator(tm));
        filter1.seek(new Range(), EMPTY_COL_FAMS, false);
        int size = size(filter1);
        assertTrue("size = " + size, size == 100);
        
        try {
            Filter fi = new SimpleFilter(new SortedMapIterator(tm));
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
        
        Filter filter2 = new SimpleFilter2(new SimpleFilter(new SortedMapIterator(tm)));
        filter2.seek(new Range(), EMPTY_COL_FAMS, false);
        size = size(filter2);
        assertTrue("tmOut wasn't empty " + size, size == 0);
    }
    
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
        
        AgeOffFilter a = new AgeOffFilter(new SortedMapIterator(tm), 101, 1001);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 100);
    }
    
    public void test2a() throws IOException {
        Text colf = new Text("a");
        Text colq = new Text("b");
        Value dv = new Value();
        TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
        HashMap<String,String> options = new HashMap<String,String>();
        options.put("a", "901");
        long ts = System.currentTimeMillis();
        
        for (long i = 0; i < 1000; i++) {
            Key k = new Key(new Text(String.format("%03d", i)), colf, colq, ts - i);
            tm.put(k, dv);
        }
        assertTrue(tm.size() == 1000);
        
        ColumnAgeOffFilter a = new ColumnAgeOffFilter(new SortedMapIterator(tm), null, 0l);
        a.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
        a.overrideCurrentTime(ts);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 902);
        
        options.put("a:b", "101");
        a.init(new SortedMapIterator(tm), options, new DefaultIteratorEnvironment());
        a.overrideCurrentTime(ts);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 102);
    }
    
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
    
    public void testNoVisFilter() throws IOException {
        TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
        Value v = new Value();
        for (int i = 0; i < 1000; i++) {
            Key k = new Key(String.format("%03d", i), "a", "b", i % 10 == 0 ? "vis" : "");
            tm.put(k, v);
        }
        assertTrue(tm.size() == 1000);
        
        Filter filter = new NoVisFilter(new SortedMapIterator(tm));
        filter.seek(new Range(), EMPTY_COL_FAMS, false);
        int size = size(filter);
        assertTrue("size = " + size, size == 100);
    }
    
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
        
        TimestampFilter a = new TimestampFilter(new SortedMapIterator(tm), 11, true, 31, false);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 20);
        a = new TimestampFilter(new SortedMapIterator(tm), 11, true, 31, true);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 21);
        a = new TimestampFilter(new SortedMapIterator(tm), 11, false, 31, false);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 19);
        a = new TimestampFilter(new SortedMapIterator(tm), 11, false, 31, true);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 20);
        
        SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMddHHmmssz");
        long baseTime = dateParser.parse("19990101000000GMT").getTime();
        tm.clear();
        for (int i = 0; i < 100; i++) {
            Key k = new Key(new Text(String.format("%02d", i)), colf, colq);
            k.setTimestamp(baseTime + (i * 1000));
            tm.put(k, dv);
        }
        assertTrue(tm.size() == 100);
        a = new TimestampFilter();
        TreeMap<String,String> opts = new TreeMap<String,String>();
        opts.put(TimestampFilter.START, "19990101000011GMT");
        opts.put(TimestampFilter.END, "19990101000031GMT");
        a.init(new SortedMapIterator(tm), opts, null);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 21);
        
        opts.put(TimestampFilter.END_INCL, "false");
        a.init(new SortedMapIterator(tm), opts, null);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 20);
        
        opts.put(TimestampFilter.START_INCL, "false");
        a.init(new SortedMapIterator(tm), opts, null);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 19);
        
        opts.put(TimestampFilter.END_INCL, "true");
        a.init(new SortedMapIterator(tm), opts, null);
        a.seek(new Range(), EMPTY_COL_FAMS, false);
        assertEquals(size(a), 20);
    }
}

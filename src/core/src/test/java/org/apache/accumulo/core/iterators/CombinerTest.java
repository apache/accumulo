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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.Combiner.ValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.user.MaxCombiner;
import org.apache.accumulo.core.iterators.user.MinCombiner;
import org.apache.accumulo.core.iterators.user.SummingArrayCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class CombinerTest {
    
    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
    
    static Key nk(int row, int colf, int colq, long ts, boolean deleted) {
        Key k = nk(row, colf, colq, ts);
        k.setDeleted(true);
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
    
    Encoder<Long> varNumEncoder = new LongCombiner.VarNumEncoder();
    Encoder<Long> longEncoder = new LongCombiner.LongEncoder();
    Encoder<Long> stringEncoder = new LongCombiner.StringEncoder();
    
    @Test
    public void test1() throws IOException {
        Encoder<Long> encoder = varNumEncoder;
        
        TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
        
        // keys that do not aggregate
        nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
        nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
        nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);
        
        Combiner ai = new SummingCombiner();
        
        Map<String,String> opts = new HashMap<String,String>();
        
        opts.put(Combiner.COLUMN_PREFIX + "cf002", null);
        opts.put(SummingCombiner.TYPE, SummingCombiner.Type.VARNUM.name());
        
        ai.init(new SortedMapIterator(tm1), opts, null);
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
        Encoder<Long> encoder = varNumEncoder;
        
        TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
        
        // keys that aggregate
        nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
        nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
        nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);
        
        Combiner ai = new SummingCombiner();
        
        Map<String,String> opts = new HashMap<String,String>();
        
        opts.put(Combiner.COLUMN_PREFIX + "cf001", null);
        opts.put(SummingCombiner.TYPE, SummingCombiner.Type.VARNUM.name());
        
        ai.init(new SortedMapIterator(tm1), opts, null);
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
        Encoder<Long> encoder = longEncoder;
        
        TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
        
        // keys that aggregate
        nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
        nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
        nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);
        
        // keys that do not aggregate
        nkv(tm1, 2, 2, 1, 1, false, 2l, encoder);
        nkv(tm1, 2, 2, 1, 2, false, 3l, encoder);
        
        Combiner ai = new SummingCombiner();
        
        Map<String,String> opts = new HashMap<String,String>();
        
        opts.put(Combiner.COLUMN_PREFIX + "cf001", "null");
        opts.put(SummingCombiner.TYPE, SummingCombiner.Type.LONG.name());
        
        ai.init(new SortedMapIterator(tm1), opts, null);
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
    public void test4() throws IOException {
        Encoder<Long> encoder = stringEncoder;
        
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
        
        Map<String,String> opts = new HashMap<String,String>();
        
        opts.put(Combiner.COLUMN_PREFIX + "cf001", null);
        opts.put(SummingCombiner.TYPE, SummingCombiner.Type.STRING.name());
        
        ai.init(new SortedMapIterator(tm1), opts, null);
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
        
    }
    
    @Test
    public void test5() throws IOException {
        Encoder<Long> encoder = stringEncoder;
        // try aggregating across multiple data sets that contain
        // the exact same keys w/ different values
        
        TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
        nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
        
        TreeMap<Key,Value> tm2 = new TreeMap<Key,Value>();
        nkv(tm2, 1, 1, 1, 1, false, 3l, encoder);
        
        TreeMap<Key,Value> tm3 = new TreeMap<Key,Value>();
        nkv(tm3, 1, 1, 1, 1, false, 4l, encoder);
        
        Combiner ai = new SummingCombiner();
        Map<String,String> opts = new HashMap<String,String>();
        opts.put(Combiner.COLUMN_PREFIX + "cf001", null);
        opts.put(SummingCombiner.TYPE, SummingCombiner.Type.STRING.name());
        
        List<SortedKeyValueIterator<Key,Value>> sources = new ArrayList<SortedKeyValueIterator<Key,Value>>(3);
        sources.add(new SortedMapIterator(tm1));
        sources.add(new SortedMapIterator(tm2));
        sources.add(new SortedMapIterator(tm3));
        
        MultiIterator mi = new MultiIterator(sources, true);
        ai.init(mi, opts, null);
        ai.seek(new Range(), EMPTY_COL_FAMS, false);
        
        assertTrue(ai.hasTop());
        assertEquals(nk(1, 1, 1, 1), ai.getTopKey());
        assertEquals("9", encoder.decode(ai.getTopValue().get()).toString());
    }
    
    @Test
    public void test6() throws IOException {
        Encoder<Long> encoder = varNumEncoder;
        TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
        
        // keys that aggregate
        nkv(tm1, 1, 1, 1, 1, false, 2l, encoder);
        nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
        nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);
        
        Combiner ai = new SummingCombiner();
        
        Map<String,String> opts = new HashMap<String,String>();
        
        opts.put(Combiner.COLUMN_PREFIX + "cf001", null);
        opts.put(SummingCombiner.TYPE, SummingCombiner.Type.VARNUM.name());
        
        ai.init(new SortedMapIterator(tm1), opts, new DefaultIteratorEnvironment());
        
        // try seeking to the beginning of a key that aggregates
        
        ai.seek(nr(1, 1, 1, 3, false), EMPTY_COL_FAMS, false);
        
        assertFalse(ai.hasTop());
        
    }
    
    @Test
    public void test7() throws IOException {
        Encoder<Long> encoder = longEncoder;
        
        // test that delete is not aggregated
        
        TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
        
        nkv(tm1, 1, 1, 1, 2, true, 0l, encoder);
        nkv(tm1, 1, 1, 1, 3, false, 4l, encoder);
        nkv(tm1, 1, 1, 1, 4, false, 3l, encoder);
        
        Combiner ai = new SummingCombiner();
        
        Map<String,String> opts = new HashMap<String,String>();
        
        opts.put(Combiner.COLUMN_PREFIX + "cf001", "null");
        opts.put(SummingCombiner.TYPE, SummingCombiner.Type.LONG.name());
        
        ai.init(new SortedMapIterator(tm1), opts, new DefaultIteratorEnvironment());
        
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
        ai.init(new SortedMapIterator(tm1), opts, new DefaultIteratorEnvironment());
        
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
    public void maxMinTest() throws IOException {
        Encoder<Long> encoder = varNumEncoder;
        
        TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
        
        // keys that aggregate
        nkv(tm1, 1, 1, 1, 1, false, 4l, encoder);
        nkv(tm1, 1, 1, 1, 2, false, 3l, encoder);
        nkv(tm1, 1, 1, 1, 3, false, 2l, encoder);
        
        Combiner ai = new MaxCombiner();
        
        Map<String,String> opts = new HashMap<String,String>();
        
        opts.put(Combiner.COLUMN_PREFIX + "cf001", null);
        opts.put(SummingCombiner.TYPE, SummingCombiner.Type.VARNUM.name());
        
        ai.init(new SortedMapIterator(tm1), opts, null);
        ai.seek(new Range(), EMPTY_COL_FAMS, false);
        
        assertTrue(ai.hasTop());
        assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
        assertEquals("4", encoder.decode(ai.getTopValue().get()).toString());
        
        ai.next();
        
        assertFalse(ai.hasTop());
        
        ai = new MinCombiner();
        
        ai.init(new SortedMapIterator(tm1), opts, null);
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
    
    public static void sumArray(Encoder<List<Long>> encoder, String type) throws IOException {
        TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
        
        // keys that aggregate
        nkv(tm1, 1, 1, 1, 1, false, nal(1l, 2l), encoder);
        nkv(tm1, 1, 1, 1, 2, false, nal(3l, 4l, 5l), encoder);
        nkv(tm1, 1, 1, 1, 3, false, nal(), encoder);
        
        Combiner ai = new SummingArrayCombiner();
        
        Map<String,String> opts = new HashMap<String,String>();
        
        opts.put(Combiner.COLUMN_PREFIX + "cf001", null);
        opts.put(SummingCombiner.TYPE, type);
        
        ai.init(new SortedMapIterator(tm1), opts, null);
        ai.seek(new Range(), EMPTY_COL_FAMS, false);
        
        assertTrue(ai.hasTop());
        assertEquals(nk(1, 1, 1, 3), ai.getTopKey());
        assertBytesEqual(encoder.encode(nal(4l, 6l, 5l)), ai.getTopValue().get());
        
        ai.next();
        
        assertFalse(ai.hasTop());
    }
    
    @Test
    public void sumArrayTest() throws IOException {
        Encoder<List<Long>> encoder = new SummingArrayCombiner.VarNumArrayEncoder();
        sumArray(encoder, "VARNUM");
        encoder = new SummingArrayCombiner.LongArrayEncoder();
        sumArray(encoder, "LONG");
        encoder = new SummingArrayCombiner.StringArrayEncoder();
        sumArray(encoder, "STRING");
    }
}

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
package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;

public class DeletingIteratorTest extends TestCase {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

  public void test1() {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dvOld = new Value("old".getBytes());
    Value dvDel = new Value("old".getBytes());
    Value dvNew = new Value("new".getBytes());

    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    Key k;

    for (int i = 0; i < 2; i++) {
      for (long j = 0; j < 5; j++) {
        k = new Key(new Text(String.format("%03d", i)), colf, colq, j);
        tm.put(k, dvOld);
      }
    }
    k = new Key(new Text(String.format("%03d", 0)), colf, colq, 5);
    k.setDeleted(true);
    tm.put(k, dvDel);
    for (int i = 0; i < 2; i++) {
      for (long j = 6; j < 11; j++) {
        k = new Key(new Text(String.format("%03d", i)), colf, colq, j);
        tm.put(k, dvNew);
      }
    }
    assertTrue("Initial size was " + tm.size(), tm.size() == 21);

    Text checkRow = new Text("000");
    try {
      DeletingIterator it = new DeletingIterator(new SortedMapIterator(tm), false);
      it.seek(new Range(), EMPTY_COL_FAMS, false);

      TreeMap<Key,Value> tmOut = new TreeMap<Key,Value>();
      while (it.hasTop()) {
        tmOut.put(it.getTopKey(), it.getTopValue());
        it.next();
      }
      assertTrue("size after no propagation was " + tmOut.size(), tmOut.size() == 15);
      for (Entry<Key,Value> e : tmOut.entrySet()) {
        if (e.getKey().getRow().equals(checkRow)) {
          byte[] b = e.getValue().get();
          assertTrue(b[0] == 'n');
          assertTrue(b[1] == 'e');
          assertTrue(b[2] == 'w');
        }
      }
    } catch (IOException e) {
      assertFalse(true);
    }

    try {
      DeletingIterator it = new DeletingIterator(new SortedMapIterator(tm), true);
      it.seek(new Range(), EMPTY_COL_FAMS, false);
      TreeMap<Key,Value> tmOut = new TreeMap<Key,Value>();
      while (it.hasTop()) {
        tmOut.put(it.getTopKey(), it.getTopValue());
        it.next();
      }
      assertTrue("size after propagation was " + tmOut.size(), tmOut.size() == 16);
      for (Entry<Key,Value> e : tmOut.entrySet()) {
        if (e.getKey().getRow().equals(checkRow)) {
          byte[] b = e.getValue().get();
          if (e.getKey().isDeleted()) {
            assertTrue(b[0] == 'o');
            assertTrue(b[1] == 'l');
            assertTrue(b[2] == 'd');
          } else {
            assertTrue(b[0] == 'n');
            assertTrue(b[1] == 'e');
            assertTrue(b[2] == 'w');
          }
        }
      }
    } catch (IOException e) {
      assertFalse(true);
    }
  }

  // seek test
  public void test2() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    nkv(tm, "r000", 4, false, "v4");
    nkv(tm, "r000", 3, false, "v3");
    nkv(tm, "r000", 2, true, "v2");
    nkv(tm, "r000", 1, false, "v1");

    DeletingIterator it = new DeletingIterator(new SortedMapIterator(tm), false);

    // SEEK two keys before delete
    it.seek(nr("r000", 4), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertEquals(nk("r000", 4), it.getTopKey());
    assertEquals("v4", it.getTopValue().toString());

    it.next();

    assertTrue(it.hasTop());
    assertEquals(nk("r000", 3), it.getTopKey());
    assertEquals("v3", it.getTopValue().toString());

    it.next();

    assertFalse(it.hasTop());

    // SEEK passed delete
    it.seek(nr("r000", 1), EMPTY_COL_FAMS, false);

    assertFalse(it.hasTop());

    // SEEK to delete
    it.seek(nr("r000", 2), EMPTY_COL_FAMS, false);

    assertFalse(it.hasTop());

    // SEEK right before delete
    it.seek(nr("r000", 3), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertEquals(nk("r000", 3), it.getTopKey());
    assertEquals("v3", it.getTopValue().toString());

    it.next();

    assertFalse(it.hasTop());
  }

  // test delete with same timestamp as existing key
  public void test3() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    nkv(tm, "r000", 3, false, "v3");
    nkv(tm, "r000", 2, false, "v2");
    nkv(tm, "r000", 2, true, "");
    nkv(tm, "r000", 1, false, "v1");

    DeletingIterator it = new DeletingIterator(new SortedMapIterator(tm), false);
    it.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertEquals(nk("r000", 3), it.getTopKey());
    assertEquals("v3", it.getTopValue().toString());

    it.next();

    assertFalse(it.hasTop());

    it.seek(nr("r000", 2), EMPTY_COL_FAMS, false);

    assertFalse(it.hasTop());
  }

  // test range inclusiveness
  public void test4() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    nkv(tm, "r000", 3, false, "v3");
    nkv(tm, "r000", 2, false, "v2");
    nkv(tm, "r000", 2, true, "");
    nkv(tm, "r000", 1, false, "v1");

    DeletingIterator it = new DeletingIterator(new SortedMapIterator(tm), false);

    it.seek(nr("r000", 3), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertEquals(nk("r000", 3), it.getTopKey());
    assertEquals("v3", it.getTopValue().toString());

    it.next();

    assertFalse(it.hasTop());

    it.seek(nr("r000", 3, false), EMPTY_COL_FAMS, false);

    assertFalse(it.hasTop());
  }

  private Range nr(String row, long ts, boolean inclusive) {
    return new Range(nk(row, ts), inclusive, null, true);
  }

  private Range nr(String row, long ts) {
    return nr(row, ts, true);
  }

  private Key nk(String row, long ts) {
    return new Key(new Text(row), ts);
  }

  private void nkv(TreeMap<Key,Value> tm, String row, long ts, boolean deleted, String val) {
    Key k = nk(row, ts);
    k.setDeleted(deleted);
    tm.put(k, new Value(val.getBytes()));
  }
}

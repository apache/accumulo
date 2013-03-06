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
import java.util.List;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.io.Text;

public class MultiIteratorTest extends TestCase {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

  public static Key nk(int row, long ts) {
    return new Key(nr(row), ts);
  }

  public static Range nrng(int row, long ts) {
    return new Range(nk(row, ts), null);
  }

  public static void nkv(TreeMap<Key,Value> tm, int row, long ts, boolean deleted, String val) {
    Key k = nk(row, ts);
    k.setDeleted(deleted);
    tm.put(k, new Value(val.getBytes()));
  }

  public static Text nr(int row) {
    return new Text(String.format("r%03d", row));
  }

  void verify(int start, int end, Key seekKey, Text endRow, Text prevEndRow, boolean init, boolean incrRow, List<TreeMap<Key,Value>> maps) throws IOException {
    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(maps.size());

    for (TreeMap<Key,Value> map : maps) {
      iters.add(new SortedMapIterator(map));
    }

    MultiIterator mi;
    if (endRow == null && prevEndRow == null)
      mi = new MultiIterator(iters, init);
    else {
      Range range = new Range(prevEndRow, false, endRow, true);
      if (init)
        for (SortedKeyValueIterator<Key,Value> iter : iters)
          iter.seek(range, LocalityGroupUtil.EMPTY_CF_SET, false);
      mi = new MultiIterator(iters, range);

      if (init)
        mi.seek(range, LocalityGroupUtil.EMPTY_CF_SET, false);
    }

    if (seekKey != null)
      mi.seek(new Range(seekKey, null), EMPTY_COL_FAMS, false);
    else
      mi.seek(new Range(), EMPTY_COL_FAMS, false);

    int i = start;
    while (mi.hasTop()) {
      if (incrRow)
        assertEquals(nk(i, 0), mi.getTopKey());
      else
        assertEquals(nk(0, i), mi.getTopKey());

      assertEquals("v" + i, mi.getTopValue().toString());

      mi.next();
      if (incrRow)
        i++;
      else
        i--;
    }

    assertEquals("start=" + start + " end=" + end + " seekKey=" + seekKey + " endRow=" + endRow + " prevEndRow=" + prevEndRow + " init=" + init + " incrRow="
        + incrRow + " maps=" + maps, end, i);
  }

  void verify(int start, Key seekKey, List<TreeMap<Key,Value>> maps) throws IOException {
    if (seekKey != null) {
      verify(start, -1, seekKey, null, null, false, false, maps);
    }

    verify(start, -1, seekKey, null, null, true, false, maps);
  }

  void verify(int start, int end, Key seekKey, Text endRow, Text prevEndRow, List<TreeMap<Key,Value>> maps) throws IOException {
    if (seekKey != null) {
      verify(start, end, seekKey, endRow, prevEndRow, false, false, maps);
    }

    verify(start, end, seekKey, endRow, prevEndRow, true, false, maps);
  }

  public void test1() throws IOException {
    // TEST non overlapping inputs

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    List<TreeMap<Key,Value>> tmpList = new ArrayList<TreeMap<Key,Value>>(2);

    for (int i = 0; i < 4; i++) {
      nkv(tm1, 0, i, false, "v" + i);
    }
    tmpList.add(tm1);
    tm1 = new TreeMap<Key,Value>();
    for (int i = 4; i < 8; i++) {
      nkv(tm1, 0, i, false, "v" + i);
    }
    tmpList.add(tm1);
    for (int seek = -1; seek < 8; seek++) {
      if (seek == 7) {
        verify(seek, null, tmpList);
      }
      verify(seek, nk(0, seek), tmpList);
    }
  }

  public void test2() throws IOException {
    // TEST overlapping inputs

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    TreeMap<Key,Value> tm2 = new TreeMap<Key,Value>();
    List<TreeMap<Key,Value>> tmpList = new ArrayList<TreeMap<Key,Value>>(2);

    for (int i = 0; i < 8; i++) {
      if (i % 2 == 0)
        nkv(tm1, 0, i, false, "v" + i);
      else
        nkv(tm2, 0, i, false, "v" + i);
    }
    tmpList.add(tm1);
    tmpList.add(tm2);
    for (int seek = -1; seek < 8; seek++) {
      if (seek == 7) {
        verify(seek, null, tmpList);
      }
      verify(seek, nk(0, seek), tmpList);
    }
  }

  public void test3() throws IOException {
    // TEST single input

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    List<TreeMap<Key,Value>> tmpList = new ArrayList<TreeMap<Key,Value>>(2);

    for (int i = 0; i < 8; i++) {
      nkv(tm1, 0, i, false, "v" + i);
    }
    tmpList.add(tm1);

    for (int seek = -1; seek < 8; seek++) {
      if (seek == 7) {
        verify(seek, null, tmpList);
      }
      verify(seek, nk(0, seek), tmpList);
    }
  }

  public void test4() throws IOException {
    // TEST empty input

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    List<SortedKeyValueIterator<Key,Value>> skvil = new ArrayList<SortedKeyValueIterator<Key,Value>>(1);
    skvil.add(new SortedMapIterator(tm1));
    MultiIterator mi = new MultiIterator(skvil, true);

    assertFalse(mi.hasTop());

    mi.seek(nrng(0, 6), EMPTY_COL_FAMS, false);
    assertFalse(mi.hasTop());
  }

  public void test5() throws IOException {
    // TEST overlapping inputs AND prevRow AND endRow AND seek

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    TreeMap<Key,Value> tm2 = new TreeMap<Key,Value>();
    List<TreeMap<Key,Value>> tmpList = new ArrayList<TreeMap<Key,Value>>(2);

    for (int i = 0; i < 8; i++) {
      if (i % 2 == 0)
        nkv(tm1, i, 0, false, "v" + i);
      else
        nkv(tm2, i, 0, false, "v" + i);
    }

    tmpList.add(tm1);
    tmpList.add(tm2);
    for (int seek = -1; seek < 9; seek++) {
      verify(Math.max(0, seek), 8, nk(seek, 0), null, null, true, true, tmpList);
      verify(Math.max(0, seek), 8, nk(seek, 0), null, null, false, true, tmpList);

      for (int er = seek; er < 10; er++) {

        int end = seek > er ? seek : Math.min(er + 1, 8);

        int noSeekEnd = Math.min(er + 1, 8);
        if (er < 0) {
          noSeekEnd = 0;
        }

        verify(0, noSeekEnd, null, nr(er), null, true, true, tmpList);
        verify(Math.max(0, seek), end, nk(seek, 0), nr(er), null, true, true, tmpList);
        verify(Math.max(0, seek), end, nk(seek, 0), nr(er), null, false, true, tmpList);

        for (int per = -1; per < er; per++) {

          int start = Math.max(per + 1, seek);

          if (start > er)
            end = start;

          if (per >= 8)
            end = start;

          int noSeekStart = Math.max(0, per + 1);

          if (er < 0 || per >= 7) {
            noSeekEnd = noSeekStart;
          }

          verify(noSeekStart, noSeekEnd, null, nr(er), nr(per), true, true, tmpList);
          verify(Math.max(0, start), end, nk(seek, 0), nr(er), nr(per), true, true, tmpList);
          verify(Math.max(0, start), end, nk(seek, 0), nr(er), nr(per), false, true, tmpList);
        }
      }
    }
  }

  public void test6() throws IOException {
    // TEst setting an endKey
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    nkv(tm1, 3, 0, false, "1");
    nkv(tm1, 4, 0, false, "2");
    nkv(tm1, 6, 0, false, "3");

    List<SortedKeyValueIterator<Key,Value>> skvil = new ArrayList<SortedKeyValueIterator<Key,Value>>(1);
    skvil.add(new SortedMapIterator(tm1));
    MultiIterator mi = new MultiIterator(skvil, true);
    mi.seek(new Range(null, true, nk(5, 9), false), EMPTY_COL_FAMS, false);

    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(nk(3, 0)));
    assertTrue(mi.getTopValue().toString().equals("1"));
    mi.next();

    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(nk(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(nk(4, 10), true, nk(5, 9), false), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(nk(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(nk(4, 10), true, nk(6, 0), false), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(nk(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(nk(4, 10), true, nk(6, 0), true), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(nk(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(nk(6, 0)));
    assertTrue(mi.getTopValue().toString().equals("3"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(nk(4, 0), true, nk(6, 0), false), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(nk(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(nk(4, 0), false, nk(6, 0), false), EMPTY_COL_FAMS, false);
    assertFalse(mi.hasTop());

    mi.seek(new Range(nk(4, 0), false, nk(6, 0), true), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(nk(6, 0)));
    assertTrue(mi.getTopValue().toString().equals("3"));
    mi.next();
    assertFalse(mi.hasTop());

  }

  public void test7() throws IOException {
    // TEst setting an endKey
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    nkv(tm1, 0, 3, false, "1");
    nkv(tm1, 0, 2, false, "2");
    nkv(tm1, 0, 1, false, "3");
    nkv(tm1, 0, 0, false, "4");
    nkv(tm1, 1, 2, false, "5");
    nkv(tm1, 1, 1, false, "6");
    nkv(tm1, 1, 0, false, "7");
    nkv(tm1, 2, 1, false, "8");
    nkv(tm1, 2, 0, false, "9");

    List<SortedKeyValueIterator<Key,Value>> skvil = new ArrayList<SortedKeyValueIterator<Key,Value>>(1);
    skvil.add(new SortedMapIterator(tm1));

    KeyExtent extent = new KeyExtent(new Text("tablename"), nr(1), nr(0));
    MultiIterator mi = new MultiIterator(skvil, extent);

    Range r1 = new Range((Text) null, (Text) null);
    mi.seek(r1, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("5"));
    mi.next();
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("6"));
    mi.next();
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("7"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r2 = new Range(nk(0, 0), true, nk(1, 1), true);
    mi.seek(r2, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("5"));
    mi.next();
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("6"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r3 = new Range(nk(0, 0), false, nk(1, 1), false);
    mi.seek(r3, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("5"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r4 = new Range(nk(1, 2), true, nk(1, 1), false);
    mi.seek(r4, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("5"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r5 = new Range(nk(1, 2), false, nk(1, 1), true);
    mi.seek(r5, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("6"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r6 = new Range(nk(2, 1), true, nk(2, 0), true);
    mi.seek(r6, EMPTY_COL_FAMS, false);
    assertFalse(mi.hasTop());

    Range r7 = new Range(nk(0, 3), true, nk(0, 1), true);
    mi.seek(r7, EMPTY_COL_FAMS, false);
    assertFalse(mi.hasTop());
  }
}

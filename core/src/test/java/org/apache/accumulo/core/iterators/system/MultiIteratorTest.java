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

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class MultiIteratorTest extends TestCase {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  public static Key newKey(int row, long ts) {
    return new Key(newRow(row), ts);
  }

  public static Range newRange(int row, long ts) {
    return new Range(newKey(row, ts), null);
  }

  public static void newKeyValue(TreeMap<Key,Value> tm, int row, long ts, boolean deleted, String val) {
    Key k = newKey(row, ts);
    k.setDeleted(deleted);
    tm.put(k, new Value(val.getBytes()));
  }

  public static Text newRow(int row) {
    return new Text(String.format("r%03d", row));
  }

  void verify(int start, int end, Key seekKey, Text endRow, Text prevEndRow, boolean init, boolean incrRow, List<TreeMap<Key,Value>> maps) throws IOException {
    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(maps.size());

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
        assertEquals(newKey(i, 0), mi.getTopKey());
      else
        assertEquals(newKey(0, i), mi.getTopKey());

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

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    List<TreeMap<Key,Value>> tmpList = new ArrayList<>(2);

    for (int i = 0; i < 4; i++) {
      newKeyValue(tm1, 0, i, false, "v" + i);
    }
    tmpList.add(tm1);
    tm1 = new TreeMap<>();
    for (int i = 4; i < 8; i++) {
      newKeyValue(tm1, 0, i, false, "v" + i);
    }
    tmpList.add(tm1);
    for (int seek = -1; seek < 8; seek++) {
      if (seek == 7) {
        verify(seek, null, tmpList);
      }
      verify(seek, newKey(0, seek), tmpList);
    }
  }

  public void test2() throws IOException {
    // TEST overlapping inputs

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    TreeMap<Key,Value> tm2 = new TreeMap<>();
    List<TreeMap<Key,Value>> tmpList = new ArrayList<>(2);

    for (int i = 0; i < 8; i++) {
      if (i % 2 == 0)
        newKeyValue(tm1, 0, i, false, "v" + i);
      else
        newKeyValue(tm2, 0, i, false, "v" + i);
    }
    tmpList.add(tm1);
    tmpList.add(tm2);
    for (int seek = -1; seek < 8; seek++) {
      if (seek == 7) {
        verify(seek, null, tmpList);
      }
      verify(seek, newKey(0, seek), tmpList);
    }
  }

  public void test3() throws IOException {
    // TEST single input

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    List<TreeMap<Key,Value>> tmpList = new ArrayList<>(2);

    for (int i = 0; i < 8; i++) {
      newKeyValue(tm1, 0, i, false, "v" + i);
    }
    tmpList.add(tm1);

    for (int seek = -1; seek < 8; seek++) {
      if (seek == 7) {
        verify(seek, null, tmpList);
      }
      verify(seek, newKey(0, seek), tmpList);
    }
  }

  public void test4() throws IOException {
    // TEST empty input

    TreeMap<Key,Value> tm1 = new TreeMap<>();

    List<SortedKeyValueIterator<Key,Value>> skvil = new ArrayList<>(1);
    skvil.add(new SortedMapIterator(tm1));
    MultiIterator mi = new MultiIterator(skvil, true);

    assertFalse(mi.hasTop());

    mi.seek(newRange(0, 6), EMPTY_COL_FAMS, false);
    assertFalse(mi.hasTop());
  }

  public void test5() throws IOException {
    // TEST overlapping inputs AND prevRow AND endRow AND seek

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    TreeMap<Key,Value> tm2 = new TreeMap<>();
    List<TreeMap<Key,Value>> tmpList = new ArrayList<>(2);

    for (int i = 0; i < 8; i++) {
      if (i % 2 == 0)
        newKeyValue(tm1, i, 0, false, "v" + i);
      else
        newKeyValue(tm2, i, 0, false, "v" + i);
    }

    tmpList.add(tm1);
    tmpList.add(tm2);
    for (int seek = -1; seek < 9; seek++) {
      verify(Math.max(0, seek), 8, newKey(seek, 0), null, null, true, true, tmpList);
      verify(Math.max(0, seek), 8, newKey(seek, 0), null, null, false, true, tmpList);

      for (int er = seek; er < 10; er++) {

        int end = seek > er ? seek : Math.min(er + 1, 8);

        int noSeekEnd = Math.min(er + 1, 8);
        if (er < 0) {
          noSeekEnd = 0;
        }

        verify(0, noSeekEnd, null, newRow(er), null, true, true, tmpList);
        verify(Math.max(0, seek), end, newKey(seek, 0), newRow(er), null, true, true, tmpList);
        verify(Math.max(0, seek), end, newKey(seek, 0), newRow(er), null, false, true, tmpList);

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

          verify(noSeekStart, noSeekEnd, null, newRow(er), newRow(per), true, true, tmpList);
          verify(Math.max(0, start), end, newKey(seek, 0), newRow(er), newRow(per), true, true, tmpList);
          verify(Math.max(0, start), end, newKey(seek, 0), newRow(er), newRow(per), false, true, tmpList);
        }
      }
    }
  }

  public void test6() throws IOException {
    // TEst setting an endKey
    TreeMap<Key,Value> tm1 = new TreeMap<>();
    newKeyValue(tm1, 3, 0, false, "1");
    newKeyValue(tm1, 4, 0, false, "2");
    newKeyValue(tm1, 6, 0, false, "3");

    List<SortedKeyValueIterator<Key,Value>> skvil = new ArrayList<>(1);
    skvil.add(new SortedMapIterator(tm1));
    MultiIterator mi = new MultiIterator(skvil, true);
    mi.seek(new Range(null, true, newKey(5, 9), false), EMPTY_COL_FAMS, false);

    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(newKey(3, 0)));
    assertTrue(mi.getTopValue().toString().equals("1"));
    mi.next();

    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(newKey(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(newKey(4, 10), true, newKey(5, 9), false), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(newKey(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(newKey(4, 10), true, newKey(6, 0), false), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(newKey(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(newKey(4, 10), true, newKey(6, 0), true), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(newKey(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(newKey(6, 0)));
    assertTrue(mi.getTopValue().toString().equals("3"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(newKey(4, 0), true, newKey(6, 0), false), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(newKey(4, 0)));
    assertTrue(mi.getTopValue().toString().equals("2"));
    mi.next();

    assertFalse(mi.hasTop());

    mi.seek(new Range(newKey(4, 0), false, newKey(6, 0), false), EMPTY_COL_FAMS, false);
    assertFalse(mi.hasTop());

    mi.seek(new Range(newKey(4, 0), false, newKey(6, 0), true), EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopKey().equals(newKey(6, 0)));
    assertTrue(mi.getTopValue().toString().equals("3"));
    mi.next();
    assertFalse(mi.hasTop());

  }

  public void test7() throws IOException {
    // TEst setting an endKey
    TreeMap<Key,Value> tm1 = new TreeMap<>();
    newKeyValue(tm1, 0, 3, false, "1");
    newKeyValue(tm1, 0, 2, false, "2");
    newKeyValue(tm1, 0, 1, false, "3");
    newKeyValue(tm1, 0, 0, false, "4");
    newKeyValue(tm1, 1, 2, false, "5");
    newKeyValue(tm1, 1, 1, false, "6");
    newKeyValue(tm1, 1, 0, false, "7");
    newKeyValue(tm1, 2, 1, false, "8");
    newKeyValue(tm1, 2, 0, false, "9");

    List<SortedKeyValueIterator<Key,Value>> skvil = new ArrayList<>(1);
    skvil.add(new SortedMapIterator(tm1));

    KeyExtent extent = new KeyExtent(Table.ID.of("tablename"), newRow(1), newRow(0));

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

    Range r2 = new Range(newKey(0, 0), true, newKey(1, 1), true);
    mi.seek(r2, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("5"));
    mi.next();
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("6"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r3 = new Range(newKey(0, 0), false, newKey(1, 1), false);
    mi.seek(r3, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("5"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r4 = new Range(newKey(1, 2), true, newKey(1, 1), false);
    mi.seek(r4, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("5"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r5 = new Range(newKey(1, 2), false, newKey(1, 1), true);
    mi.seek(r5, EMPTY_COL_FAMS, false);
    assertTrue(mi.hasTop());
    assertTrue(mi.getTopValue().toString().equals("6"));
    mi.next();
    assertFalse(mi.hasTop());

    Range r6 = new Range(newKey(2, 1), true, newKey(2, 0), true);
    mi.seek(r6, EMPTY_COL_FAMS, false);
    assertFalse(mi.hasTop());

    Range r7 = new Range(newKey(0, 3), true, newKey(0, 1), true);
    mi.seek(r7, EMPTY_COL_FAMS, false);
    assertFalse(mi.hasTop());
  }
}

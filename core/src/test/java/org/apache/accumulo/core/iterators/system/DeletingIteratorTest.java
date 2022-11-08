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
package org.apache.accumulo.core.iterators.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.DeletingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.DeletingIterator.Behavior;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class DeletingIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  @Test
  public void test1() {
    Text colf = new Text("a");
    Text colq = new Text("b");
    Value dvOld = new Value("old");
    Value dvDel = new Value("old");
    Value dvNew = new Value("new");

    TreeMap<Key,Value> tm = new TreeMap<>();
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
    assertEquals(21, tm.size(), "Initial size was " + tm.size());

    Text checkRow = new Text("000");
    try {
      SortedKeyValueIterator<Key,Value> it =
          DeletingIterator.wrap(new SortedMapIterator(tm), false, Behavior.PROCESS);
      it.seek(new Range(), EMPTY_COL_FAMS, false);

      TreeMap<Key,Value> tmOut = new TreeMap<>();
      while (it.hasTop()) {
        tmOut.put(it.getTopKey(), it.getTopValue());
        it.next();
      }
      assertEquals(15, tmOut.size(), "size after no propagation was " + tmOut.size());
      for (Entry<Key,Value> e : tmOut.entrySet()) {
        if (e.getKey().getRow().equals(checkRow)) {
          byte[] b = e.getValue().get();
          assertEquals('n', b[0]);
          assertEquals('e', b[1]);
          assertEquals('w', b[2]);
        }
      }
    } catch (IOException e) {
      fail();
    }

    try {
      SortedKeyValueIterator<Key,Value> it =
          DeletingIterator.wrap(new SortedMapIterator(tm), true, Behavior.PROCESS);
      it.seek(new Range(), EMPTY_COL_FAMS, false);
      TreeMap<Key,Value> tmOut = new TreeMap<>();
      while (it.hasTop()) {
        tmOut.put(it.getTopKey(), it.getTopValue());
        it.next();
      }
      assertEquals(16, tmOut.size(), "size after propagation was " + tmOut.size());
      for (Entry<Key,Value> e : tmOut.entrySet()) {
        if (e.getKey().getRow().equals(checkRow)) {
          byte[] b = e.getValue().get();
          if (e.getKey().isDeleted()) {
            assertEquals('o', b[0]);
            assertEquals('l', b[1]);
            assertEquals('d', b[2]);
          } else {
            assertEquals('n', b[0]);
            assertEquals('e', b[1]);
            assertEquals('w', b[2]);
          }
        }
      }
    } catch (IOException e) {
      fail();
    }
  }

  // seek test
  @Test
  public void test2() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    newKeyValue(tm, "r000", 4, false, "v4");
    newKeyValue(tm, "r000", 3, false, "v3");
    newKeyValue(tm, "r000", 2, true, "v2");
    newKeyValue(tm, "r000", 1, false, "v1");

    SortedKeyValueIterator<Key,Value> it =
        DeletingIterator.wrap(new SortedMapIterator(tm), false, Behavior.PROCESS);

    // SEEK two keys before delete
    it.seek(newRange("r000", 4), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertEquals(newKey("r000", 4), it.getTopKey());
    assertEquals("v4", it.getTopValue().toString());

    it.next();

    assertTrue(it.hasTop());
    assertEquals(newKey("r000", 3), it.getTopKey());
    assertEquals("v3", it.getTopValue().toString());

    it.next();

    assertFalse(it.hasTop());

    // SEEK passed delete
    it.seek(newRange("r000", 1), EMPTY_COL_FAMS, false);

    assertFalse(it.hasTop());

    // SEEK to delete
    it.seek(newRange("r000", 2), EMPTY_COL_FAMS, false);

    assertFalse(it.hasTop());

    // SEEK right before delete
    it.seek(newRange("r000", 3), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertEquals(newKey("r000", 3), it.getTopKey());
    assertEquals("v3", it.getTopValue().toString());

    it.next();

    assertFalse(it.hasTop());
  }

  // test delete with same timestamp as existing key
  @Test
  public void test3() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    newKeyValue(tm, "r000", 3, false, "v3");
    newKeyValue(tm, "r000", 2, false, "v2");
    newKeyValue(tm, "r000", 2, true, "");
    newKeyValue(tm, "r000", 1, false, "v1");

    SortedKeyValueIterator<Key,Value> it =
        DeletingIterator.wrap(new SortedMapIterator(tm), false, Behavior.PROCESS);
    it.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertEquals(newKey("r000", 3), it.getTopKey());
    assertEquals("v3", it.getTopValue().toString());

    it.next();

    assertFalse(it.hasTop());

    it.seek(newRange("r000", 2), EMPTY_COL_FAMS, false);

    assertFalse(it.hasTop());
  }

  // test range inclusiveness
  @Test
  public void test4() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    newKeyValue(tm, "r000", 3, false, "v3");
    newKeyValue(tm, "r000", 2, false, "v2");
    newKeyValue(tm, "r000", 2, true, "");
    newKeyValue(tm, "r000", 1, false, "v1");

    SortedKeyValueIterator<Key,Value> it =
        DeletingIterator.wrap(new SortedMapIterator(tm), false, Behavior.PROCESS);

    it.seek(newRange("r000", 3), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertEquals(newKey("r000", 3), it.getTopKey());
    assertEquals("v3", it.getTopValue().toString());

    it.next();

    assertFalse(it.hasTop());

    it.seek(newRange("r000", 3, false), EMPTY_COL_FAMS, false);

    assertFalse(it.hasTop());
  }

  @Test
  public void testFail() throws IOException {
    TreeMap<Key,Value> tm = new TreeMap<>();

    newKeyValue(tm, "r000", 3, false, "v3");
    newKeyValue(tm, "r000", 2, false, "v2");
    newKeyValue(tm, "r000", 2, true, "");
    newKeyValue(tm, "r000", 1, false, "v1");

    SortedKeyValueIterator<Key,Value> it =
        DeletingIterator.wrap(new SortedMapIterator(tm), false, Behavior.FAIL);
    it.seek(new Range(), EMPTY_COL_FAMS, false);

    // first entry should pass
    it.getTopKey();
    it.next();

    // second entry should fail due to delete
    assertThrows(IllegalStateException.class, it::getTopKey);
    it.next();

    // third entry should pass
    it.getTopKey();
    it.next();

    // fourth entry should pass
    it.getTopKey();
    it.next();
  }

  private Range newRange(String row, long ts, boolean inclusive) {
    return new Range(newKey(row, ts), inclusive, null, true);
  }

  private Range newRange(String row, long ts) {
    return newRange(row, ts, true);
  }

  private Key newKey(String row, long ts) {
    return new Key(new Text(row), ts);
  }

  private void newKeyValue(TreeMap<Key,Value> tm, String row, long ts, boolean deleted,
      String val) {
    Key k = newKey(row, ts);
    k.setDeleted(deleted);
    tm.put(k, new Value(val));
  }
}

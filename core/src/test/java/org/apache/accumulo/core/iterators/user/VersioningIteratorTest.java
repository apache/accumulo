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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class VersioningIteratorTest {
  // add test for seek function
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  private static final Encoder<Long> encoder = LongCombiner.FIXED_LEN_ENCODER;

  void createTestData(TreeMap<Key,Value> tm, Text colf, Text colq) {
    for (int i = 0; i < 2; i++) {
      for (long j = 0; j < 20; j++) {
        Key k = new Key(new Text(String.format("%03d", i)), colf, colq, j);
        tm.put(k, new Value(encoder.encode(j)));
      }
    }

    assertTrue("Initial size was " + tm.size(), tm.size() == 40);
  }

  TreeMap<Key,Value> iteratorOverTestData(VersioningIterator it) throws IOException {
    TreeMap<Key,Value> tmOut = new TreeMap<Key,Value>();
    while (it.hasTop()) {
      tmOut.put(it.getTopKey(), it.getTopValue());
      it.next();
    }

    return tmOut;
  }

  @Test
  public void test1() {
    Text colf = new Text("a");
    Text colq = new Text("b");

    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    createTestData(tm, colf, colq);

    try {
      VersioningIterator it = new VersioningIterator();
      IteratorSetting is = new IteratorSetting(1, VersioningIterator.class);
      VersioningIterator.setMaxVersions(is, 3);
      it.init(new SortedMapIterator(tm), is.getOptions(), null);
      it.seek(new Range(), EMPTY_COL_FAMS, false);

      TreeMap<Key,Value> tmOut = iteratorOverTestData(it);

      for (Entry<Key,Value> e : tmOut.entrySet()) {
        assertTrue(e.getValue().get().length == 8);
        assertTrue(16 < encoder.decode(e.getValue().get()));
      }
      assertTrue("size after keeping 3 versions was " + tmOut.size(), tmOut.size() == 6);
    } catch (IOException e) {
      assertFalse(true);
    } catch (Exception e) {
      e.printStackTrace();
      assertFalse(true);
    }
  }

  @Test
  public void test2() {
    Text colf = new Text("a");
    Text colq = new Text("b");

    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    createTestData(tm, colf, colq);

    try {
      VersioningIterator it = new VersioningIterator();
      IteratorSetting is = new IteratorSetting(1, VersioningIterator.class);
      VersioningIterator.setMaxVersions(is, 3);
      it.init(new SortedMapIterator(tm), is.getOptions(), null);

      // after doing this seek, should only get two keys for row 1
      // since we are seeking to the middle of the most recent
      // three keys
      Key seekKey = new Key(new Text(String.format("%03d", 1)), colf, colq, 18);
      it.seek(new Range(seekKey, null), EMPTY_COL_FAMS, false);

      TreeMap<Key,Value> tmOut = iteratorOverTestData(it);

      for (Entry<Key,Value> e : tmOut.entrySet()) {
        assertTrue(e.getValue().get().length == 8);
        assertTrue(16 < encoder.decode(e.getValue().get()));
      }
      assertTrue("size after keeping 2 versions was " + tmOut.size(), tmOut.size() == 2);
    } catch (IOException e) {
      assertFalse(true);
    } catch (Exception e) {
      e.printStackTrace();
      assertFalse(true);
    }
  }

  @Test
  public void test3() {
    Text colf = new Text("a");
    Text colq = new Text("b");

    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    createTestData(tm, colf, colq);

    try {
      VersioningIterator it = new VersioningIterator();
      IteratorSetting is = new IteratorSetting(1, VersioningIterator.class);
      VersioningIterator.setMaxVersions(is, 3);
      it.init(new SortedMapIterator(tm), is.getOptions(), null);

      // after doing this seek, should get zero keys for row 1
      Key seekKey = new Key(new Text(String.format("%03d", 1)), colf, colq, 15);
      it.seek(new Range(seekKey, null), EMPTY_COL_FAMS, false);

      TreeMap<Key,Value> tmOut = iteratorOverTestData(it);

      for (Entry<Key,Value> e : tmOut.entrySet()) {
        assertTrue(e.getValue().get().length == 8);
        assertTrue(16 < encoder.decode(e.getValue().get()));
      }

      assertTrue("size after seeking past versions was " + tmOut.size(), tmOut.size() == 0);

      // after doing this seek, should get zero keys for row 0 and 3 keys for row 1
      seekKey = new Key(new Text(String.format("%03d", 0)), colf, colq, 15);
      it.seek(new Range(seekKey, null), EMPTY_COL_FAMS, false);

      tmOut = iteratorOverTestData(it);

      for (Entry<Key,Value> e : tmOut.entrySet()) {
        assertTrue(e.getValue().get().length == 8);
        assertTrue(16 < encoder.decode(e.getValue().get()));
      }

      assertTrue("size after seeking past versions was " + tmOut.size(), tmOut.size() == 3);

    } catch (IOException e) {
      assertFalse(true);
    } catch (Exception e) {
      e.printStackTrace();
      assertFalse(true);
    }
  }

  @Test
  public void test4() {
    Text colf = new Text("a");
    Text colq = new Text("b");

    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    createTestData(tm, colf, colq);

    for (int i = 1; i <= 30; i++) {
      try {
        VersioningIterator it = new VersioningIterator();
        IteratorSetting is = new IteratorSetting(1, VersioningIterator.class);
        VersioningIterator.setMaxVersions(is, i);
        it.init(new SortedMapIterator(tm), is.getOptions(), null);
        it.seek(new Range(), EMPTY_COL_FAMS, false);

        TreeMap<Key,Value> tmOut = iteratorOverTestData(it);

        assertTrue("size after keeping " + i + " versions was " + tmOut.size(), tmOut.size() == Math.min(40, 2 * i));
      } catch (IOException e) {
        assertFalse(true);
      } catch (Exception e) {
        e.printStackTrace();
        assertFalse(true);
      }
    }
  }

  @Test
  public void test5() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");

    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    createTestData(tm, colf, colq);

    VersioningIterator it = new VersioningIterator();
    IteratorSetting is = new IteratorSetting(1, VersioningIterator.class);
    VersioningIterator.setMaxVersions(is, 3);
    it.init(new SortedMapIterator(tm), is.getOptions(), null);

    Key seekKey = new Key(new Text(String.format("%03d", 1)), colf, colq, 19);
    it.seek(new Range(seekKey, false, null, true), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertTrue(it.getTopKey().getTimestamp() == 18);

  }

  @Test
  public void test6() throws IOException {
    Text colf = new Text("a");
    Text colq = new Text("b");

    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();

    createTestData(tm, colf, colq);

    VersioningIterator it = new VersioningIterator();
    IteratorSetting is = new IteratorSetting(1, VersioningIterator.class);
    VersioningIterator.setMaxVersions(is, 3);
    it.init(new SortedMapIterator(tm), is.getOptions(), null);
    VersioningIterator it2 = it.deepCopy(null);

    Key seekKey = new Key(new Text(String.format("%03d", 1)), colf, colq, 19);
    it.seek(new Range(seekKey, false, null, true), EMPTY_COL_FAMS, false);
    it2.seek(new Range(seekKey, false, null, true), EMPTY_COL_FAMS, false);

    assertTrue(it.hasTop());
    assertTrue(it.getTopKey().getTimestamp() == 18);

    assertTrue(it2.hasTop());
    assertTrue(it2.getTopKey().getTimestamp() == 18);
  }
}

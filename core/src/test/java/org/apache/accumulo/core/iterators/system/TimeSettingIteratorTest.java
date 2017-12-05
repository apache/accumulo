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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TimeSettingIteratorTest {

  @Test
  public void test1() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<>();

    tm1.put(new Key("r0", "cf1", "cq1", 9l), new Value("v0".getBytes()));
    tm1.put(new Key("r1", "cf1", "cq1", Long.MAX_VALUE), new Value("v1".getBytes()));
    tm1.put(new Key("r1", "cf1", "cq1", 90l), new Value("v2".getBytes()));
    tm1.put(new Key("r1", "cf1", "cq1", 0l), new Value("v3".getBytes()));
    tm1.put(new Key("r2", "cf1", "cq1", 6l), new Value("v4".getBytes()));

    TimeSettingIterator tsi = new TimeSettingIterator(new SortedMapIterator(tm1), 50);

    tsi.seek(new Range(new Key("r1", "cf1", "cq1", 50l), true, new Key("r1", "cf1", "cq1", 50l), true), new HashSet<ByteSequence>(), false);

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r1", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v1", tsi.getTopValue().toString());
    tsi.next();

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r1", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v2", tsi.getTopValue().toString());
    tsi.next();

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r1", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v3", tsi.getTopValue().toString());
    tsi.next();

    assertFalse(tsi.hasTop());

    tsi.seek(new Range(new Key("r1", "cf1", "cq1", 50l), false, null, true), new HashSet<ByteSequence>(), false);

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r2", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v4", tsi.getTopValue().toString());
    tsi.next();

    assertFalse(tsi.hasTop());

    tsi.seek(new Range(null, true, new Key("r1", "cf1", "cq1", 50l), false), new HashSet<ByteSequence>(), false);

    assertTrue(tsi.hasTop());
    assertEquals(new Key("r0", "cf1", "cq1", 50l), tsi.getTopKey());
    assertEquals("v0", tsi.getTopValue().toString());
    tsi.next();

    assertFalse(tsi.hasTop());

    tsi.seek(new Range(new Key("r1", "cf1", "cq1", 51l), true, new Key("r1", "cf1", "cq1", 50l), false), new HashSet<ByteSequence>(), false);
    assertFalse(tsi.hasTop());
  }

  @Test
  public void testAvoidKeyCopy() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<>();
    final Key k = new Key("r0", "cf1", "cq1", 9l);

    tm1.put(k, new Value("v0".getBytes()));

    TimeSettingIterator tsi = new TimeSettingIterator(new SortedMapIterator(tm1), 50);

    tsi.seek(new Range(), new HashSet<ByteSequence>(), false);

    assertTrue(tsi.hasTop());
    final Key topKey = tsi.getTopKey();
    assertTrue("Expected the topKey to be the same object", k == topKey);
    assertEquals(new Key("r0", "cf1", "cq1", 50l), topKey);
    assertEquals("v0", tsi.getTopValue().toString());
    tsi.next();

    assertFalse(tsi.hasTop());
  }

  @Test
  public void testEndKeyRangeAtMinLongValue() throws IOException {
    Text row = new Text("a");
    Text colf = new Text("b");
    Text colq = new Text("c");
    Text cv = new Text();

    for (boolean inclusiveEndRange : new boolean[] {true, false}) {
      TreeMap<Key,Value> sources = new TreeMap<>();
      sources.put(new Key(row.getBytes(), colf.getBytes(), colq.getBytes(), cv.getBytes(), Long.MIN_VALUE, true), new Value("00".getBytes()));
      sources.put(new Key(row.getBytes(), colf.getBytes(), colq.getBytes(), cv.getBytes(), Long.MIN_VALUE), new Value("11".getBytes()));

      TimeSettingIterator it = new TimeSettingIterator(new SortedMapIterator(sources), 111L);
      IteratorSetting is = new IteratorSetting(1, TimeSettingIterator.class);
      it.init(null, is.getOptions(), null);

      Key startKey = new Key();
      Key endKey = new Key(row, colf, colq, cv, Long.MIN_VALUE);
      Range testRange = new Range(startKey, false, endKey, inclusiveEndRange);
      it.seek(testRange, new HashSet<ByteSequence>(), false);

      assertTrue(it.hasTop());
      assertTrue(it.getTopValue().equals(new Value("00".getBytes())));
      assertTrue(it.getTopKey().getTimestamp() == 111L);
      it.next();
      assertTrue(it.hasTop());
      assertTrue(it.getTopValue().equals(new Value("11".getBytes())));
      assertTrue(it.getTopKey().getTimestamp() == 111L);
      it.next();
      assertFalse(it.hasTop());
    }
  }
}

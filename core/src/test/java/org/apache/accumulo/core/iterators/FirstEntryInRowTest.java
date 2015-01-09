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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class FirstEntryInRowTest {
  private static final Map<String,String> EMPTY_MAP = new HashMap<String,String>();
  private static final Collection<ByteSequence> EMPTY_SET = new HashSet<ByteSequence>();

  private Key nk(String row, String cf, String cq, long time) {
    return new Key(new Text(row), new Text(cf), new Text(cq), time);
  }

  private Key nk(int row, int cf, int cq, long time) {
    return nk(String.format("%06d", row), String.format("%06d", cf), String.format("%06d", cq), time);
  }

  private void put(TreeMap<Key,Value> tm, String row, String cf, String cq, long time, Value val) {
    tm.put(nk(row, cf, cq, time), val);
  }

  private void put(TreeMap<Key,Value> tm, String row, String cf, String cq, long time, String val) {
    put(tm, row, cf, cq, time, new Value(val.getBytes()));
  }

  private void put(TreeMap<Key,Value> tm, int row, int cf, int cq, long time, int val) {
    tm.put(nk(row, cf, cq, time), new Value((val + "").getBytes()));
  }

  private void aten(FirstEntryInRowIterator rdi, String row, String cf, String cq, long time, String val) throws Exception {
    assertTrue(rdi.hasTop());
    assertEquals(nk(row, cf, cq, time), rdi.getTopKey());
    assertEquals(val, rdi.getTopValue().toString());
    rdi.next();
  }

  private void aten(FirstEntryInRowIterator rdi, int row, int cf, int cq, long time, int val) throws Exception {
    assertTrue(rdi.hasTop());
    assertEquals(nk(row, cf, cq, time), rdi.getTopKey());
    assertEquals(val, Integer.parseInt(rdi.getTopValue().toString()));
    rdi.next();
  }

  @Test
  public void test1() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq3", 5, "v2");
    put(tm1, "r2", "cf1", "cq1", 5, "v3");
    put(tm1, "r2", "cf2", "cq4", 5, "v4");
    put(tm1, "r2", "cf2", "cq5", 5, "v5");
    put(tm1, "r3", "cf3", "cq6", 5, "v6");

    FirstEntryInRowIterator fei = new FirstEntryInRowIterator();
    fei.init(new SortedMapIterator(tm1), EMPTY_MAP, null);

    fei.seek(new Range(), EMPTY_SET, false);
    aten(fei, "r1", "cf1", "cq1", 5, "v1");
    aten(fei, "r2", "cf1", "cq1", 5, "v3");
    aten(fei, "r3", "cf3", "cq6", 5, "v6");
    assertFalse(fei.hasTop());

  }

  @Test
  public void test2() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();

    for (int r = 0; r < 5; r++) {
      for (int cf = r; cf < 100; cf++) {
        for (int cq = 3; cq < 6; cq++) {
          put(tm1, r, cf, cq, 6, r * cf * cq);
        }
      }
    }

    FirstEntryInRowIterator fei = new FirstEntryInRowIterator();
    fei.init(new SortedMapIterator(tm1), EMPTY_MAP, null);
    fei.seek(new Range(nk(0, 10, 0, 0), null), EMPTY_SET, false);
    aten(fei, 1, 1, 3, 6, 1 * 1 * 3);
    aten(fei, 2, 2, 3, 6, 2 * 2 * 3);
    aten(fei, 3, 3, 3, 6, 3 * 3 * 3);
    aten(fei, 4, 4, 3, 6, 4 * 4 * 3);
    assertFalse(fei.hasTop());

    fei.seek(new Range(nk(1, 1, 3, 6), nk(3, 3, 3, 6)), EMPTY_SET, false);
    aten(fei, 1, 1, 3, 6, 1 * 1 * 3);
    aten(fei, 2, 2, 3, 6, 2 * 2 * 3);
    aten(fei, 3, 3, 3, 6, 3 * 3 * 3);
    assertFalse(fei.hasTop());

    fei.seek(new Range(nk(1, 1, 3, 6), false, nk(3, 3, 3, 6), false), EMPTY_SET, false);
    aten(fei, 2, 2, 3, 6, 2 * 2 * 3);
    assertFalse(fei.hasTop());
  }
}

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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.CountingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class ColumnFamilySkippingIteratorTest {

  private static final Collection<ByteSequence> EMPTY_SET = new HashSet<>();

  Key newKey(String row, String cf, String cq, long time) {
    return new Key(new Text(row), new Text(cf), new Text(cq), time);
  }

  String format(int field) {
    return String.format("%06d", field);
  }

  Key newKey(int row, int cf, int cq, long time) {
    return newKey(format(row), format(cf), format(cq), time);
  }

  void put(TreeMap<Key,Value> tm, String row, String cf, String cq, long time, Value val) {
    tm.put(newKey(row, cf, cq, time), val);
  }

  void put(TreeMap<Key,Value> tm, String row, String cf, String cq, long time, String val) {
    put(tm, row, cf, cq, time, new Value(val));
  }

  void put(TreeMap<Key,Value> tm, int row, int cf, int cq, long time, int val) {
    tm.put(newKey(row, cf, cq, time), new Value(val + ""));
  }

  private void testAndCallnext(ColumnFamilySkippingIterator rdi, String row, String cf, String cq,
      long time, String val) throws Exception {
    assertTrue(rdi.hasTop());
    assertEquals(newKey(row, cf, cq, time), rdi.getTopKey());
    assertEquals(val, rdi.getTopValue().toString());
    rdi.next();
  }

  @Test
  public void test1() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<>();
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq3", 5, "v2");
    put(tm1, "r2", "cf1", "cq1", 5, "v3");
    put(tm1, "r2", "cf2", "cq4", 5, "v4");
    put(tm1, "r2", "cf2", "cq5", 5, "v5");
    put(tm1, "r3", "cf3", "cq6", 5, "v6");

    ColumnFamilySkippingIterator cfi = new ColumnFamilySkippingIterator(new SortedMapIterator(tm1));

    cfi.seek(new Range(), EMPTY_SET, true);
    assertFalse(cfi.hasTop());

    cfi.seek(new Range(), EMPTY_SET, false);
    assertTrue(cfi.hasTop());
    TreeMap<Key,Value> tm2 = new TreeMap<>();
    while (cfi.hasTop()) {
      tm2.put(cfi.getTopKey(), cfi.getTopValue());
      cfi.next();
    }
    assertEquals(tm1, tm2);

    HashSet<ByteSequence> colfams = new HashSet<>();
    colfams.add(new ArrayByteSequence("cf2"));
    cfi.seek(new Range(), colfams, true);
    testAndCallnext(cfi, "r2", "cf2", "cq4", 5, "v4");
    testAndCallnext(cfi, "r2", "cf2", "cq5", 5, "v5");
    assertFalse(cfi.hasTop());

    colfams.add(new ArrayByteSequence("cf3"));
    colfams.add(new ArrayByteSequence("cf4"));
    cfi.seek(new Range(), colfams, true);
    testAndCallnext(cfi, "r2", "cf2", "cq4", 5, "v4");
    testAndCallnext(cfi, "r2", "cf2", "cq5", 5, "v5");
    testAndCallnext(cfi, "r3", "cf3", "cq6", 5, "v6");
    assertFalse(cfi.hasTop());

    cfi.seek(new Range(), colfams, false);
    testAndCallnext(cfi, "r1", "cf1", "cq1", 5, "v1");
    testAndCallnext(cfi, "r1", "cf1", "cq3", 5, "v2");
    testAndCallnext(cfi, "r2", "cf1", "cq1", 5, "v3");
    assertFalse(cfi.hasTop());

  }

  @Test
  public void test2() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<>();

    for (int r = 0; r < 10; r++) {
      for (int cf = 0; cf < 1000; cf++) {
        for (int cq = 0; cq < 3; cq++) {
          put(tm1, r, cf, cq, 6, r * cf * cq);
        }
      }
    }

    HashSet<ByteSequence> allColfams = new HashSet<>();
    for (int cf = 0; cf < 1000; cf++) {
      allColfams.add(new ArrayByteSequence(String.format("%06d", cf)));
    }

    ColumnFamilySkippingIterator cfi = new ColumnFamilySkippingIterator(new SortedMapIterator(tm1));
    HashSet<ByteSequence> colfams = new HashSet<>();

    runTest(cfi, 30000, 0, allColfams, colfams);

    colfams.add(new ArrayByteSequence(String.format("%06d", 60)));
    runTest(cfi, 30000, 30, allColfams, colfams);

    colfams.add(new ArrayByteSequence(String.format("%06d", 602)));
    runTest(cfi, 30000, 60, allColfams, colfams);

    colfams.add(new ArrayByteSequence(String.format("%06d", 0)));
    runTest(cfi, 30000, 90, allColfams, colfams);

    colfams.add(new ArrayByteSequence(String.format("%06d", 999)));
    runTest(cfi, 30000, 120, allColfams, colfams);

    colfams.remove(new ArrayByteSequence(String.format("%06d", 0)));
    runTest(cfi, 30000, 90, allColfams, colfams);

    colfams.add(new ArrayByteSequence(String.format("%06d", 1000)));
    runTest(cfi, 30000, 90, allColfams, colfams);

    colfams.remove(new ArrayByteSequence(String.format("%06d", 999)));
    runTest(cfi, 30000, 60, allColfams, colfams);

    colfams.add(new ArrayByteSequence(String.format("%06d", 61)));
    runTest(cfi, 30000, 90, allColfams, colfams);

    for (int i = 62; i < 100; i++) {
      colfams.add(new ArrayByteSequence(String.format("%06d", i)));
    }

    runTest(cfi, 30000, 1230, allColfams, colfams);

  }

  private void runTest(ColumnFamilySkippingIterator cfi, int total, int expected,
      HashSet<ByteSequence> allColfams, HashSet<ByteSequence> colfams) throws Exception {
    cfi.seek(new Range(), colfams, true);
    HashSet<ByteSequence> excpected1 = new HashSet<>(colfams);
    excpected1.retainAll(allColfams);
    runTest(cfi, expected, excpected1);

    HashSet<ByteSequence> excpected2 = new HashSet<>(allColfams);
    excpected2.removeAll(colfams);
    cfi.seek(new Range(), colfams, false);
    runTest(cfi, total - expected, excpected2);
  }

  private void runTest(ColumnFamilySkippingIterator cfi, int expected,
      HashSet<ByteSequence> colfams) throws Exception {
    int count = 0;

    HashSet<ByteSequence> ocf = new HashSet<>();

    while (cfi.hasTop()) {
      count++;
      ocf.add(cfi.getTopKey().getColumnFamilyData());
      cfi.next();
    }

    assertEquals(expected, count);
    assertEquals(colfams, ocf);
  }

  @Test
  public void test3() throws Exception {
    // construct test where ColumnFamilySkippingIterator might try to seek past the end of the user
    // supplied range
    TreeMap<Key,Value> tm1 = new TreeMap<>();

    for (int r = 0; r < 3; r++) {
      for (int cf = 4; cf < 1000; cf++) {
        for (int cq = 0; cq < 1; cq++) {
          put(tm1, r, cf, cq, 6, r * cf * cq);
        }
      }
    }

    CountingIterator ci = new CountingIterator(new SortedMapIterator(tm1));
    ColumnFamilySkippingIterator cfi = new ColumnFamilySkippingIterator(ci);
    HashSet<ByteSequence> colfams = new HashSet<>();
    colfams.add(new ArrayByteSequence(String.format("%06d", 4)));

    Range range = new Range(newKey(0, 4, 0, 6), true, newKey(0, 400, 0, 6), true);
    cfi.seek(range, colfams, true);

    assertTrue(cfi.hasTop());
    assertEquals(newKey(0, 4, 0, 6), cfi.getTopKey());
    cfi.next();
    assertFalse(cfi.hasTop());

    colfams.add(new ArrayByteSequence(String.format("%06d", 500)));
    cfi.seek(range, colfams, true);

    assertTrue(cfi.hasTop());
    assertEquals(newKey(0, 4, 0, 6), cfi.getTopKey());
    cfi.next();
    assertFalse(cfi.hasTop());

    range = new Range(newKey(0, 4, 0, 6), true, newKey(1, 400, 0, 6), true);
    cfi.seek(range, colfams, true);

    assertTrue(cfi.hasTop());
    assertEquals(newKey(0, 4, 0, 6), cfi.getTopKey());
    cfi.next();
    assertTrue(cfi.hasTop());
    assertEquals(newKey(0, 500, 0, 6), cfi.getTopKey());
    cfi.next();
    assertTrue(cfi.hasTop());
    assertEquals(newKey(1, 4, 0, 6), cfi.getTopKey());
    cfi.next();
    assertFalse(cfi.hasTop());

    // System.out.println(ci.getCount());
  }

  private static class SeekCaptureIter extends WrappingIterator {

    private final Consumer<Range> rangeConsumer;

    SeekCaptureIter(Consumer<Range> rangeConsumer, SortedKeyValueIterator<Key,Value> source) {
      this.rangeConsumer = rangeConsumer;
      setSource(source);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {
      rangeConsumer.accept(range);
      super.seek(range, columnFamilies, inclusive);
    }
  }

  /**
   * Test to ensure the seek range is bounded using the min and max column families.
   */
  @Test
  public void testBoundedSeek() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<>();

    for (int r = 0; r < 3; r++) {
      for (int cf = 4; cf < 1000; cf++) {
        for (int cq = 0; cq < 1; cq++) {
          put(tm1, r, cf, cq, 6, r * cf * cq);
        }
      }
    }

    List<Range> capturedRanges = new ArrayList<>();
    // Add an iterator that captures the seek range that ColumnFamilySkippingIterator passes down
    SeekCaptureIter seekCaptureIter =
        new SeekCaptureIter(capturedRanges::add, new SortedMapIterator(tm1));
    ColumnFamilySkippingIterator cfi = new ColumnFamilySkippingIterator(seekCaptureIter);

    cfi.seek(new Range(format(1)),
        Set.of(new ArrayByteSequence(format(8)), new ArrayByteSequence(format(10))), true);
    assertEquals(1, capturedRanges.size());
    // verify the seek on the underlying iterator was narrowed using the min and max columns
    var startKey = new Key(format(1), format(8));
    startKey.setDeleted(true);
    var endKey = new Key(format(1), format(10)).followingKey(PartialKey.ROW_COLFAM);
    // check that the seek range was bounded using the columns
    assertEquals(new Range(startKey, true, endKey, false), capturedRanges.get(0));

    // check the data is correct after the seek
    for (int cf = 8; cf <= 10; cf += 2) {
      for (int cq = 0; cq < 1; cq++) {
        assertTrue(cfi.hasTop());
        assertEquals(cfi.getTopKey(), newKey(1, cf, cq, 6));
        cfi.next();
      }
    }

    assertFalse(cfi.hasTop());

    // ensure that bounding does not widen the range
    var seekRange =
        new Range(new Key(format(1), format(9)), true, new Key(format(2), format(9)), false);
    capturedRanges.clear();
    cfi.seek(seekRange, Set.of(new ArrayByteSequence(format(8)), new ArrayByteSequence(format(10))),
        true);
    assertEquals(1, capturedRanges.size());
    // the range is more narrow than what bounding the range by the min and max family would have
    // produced so nothing should be done to the seek range
    assertEquals(seekRange, capturedRanges.get(0));

    // with the range supplied should see family 10 for row 1 and family 8 for row 2
    for (int cq = 0; cq < 1; cq++) {
      assertTrue(cfi.hasTop());
      assertEquals(cfi.getTopKey(), newKey(1, 10, cq, 6));
      cfi.next();
    }
    for (int cq = 0; cq < 1; cq++) {
      assertTrue(cfi.hasTop());
      assertEquals(cfi.getTopKey(), newKey(2, 8, cq, 6));
      cfi.next();
    }
    assertFalse(cfi.hasTop());

    // test narrowing a range that crosses multiple rows
    seekRange = new Range(format(1), true, format(2), true);
    capturedRanges.clear();
    cfi.seek(seekRange, Set.of(new ArrayByteSequence(format(7))), true);
    startKey = new Key(format(1), format(7));
    startKey.setDeleted(true);
    endKey = new Key(format(2), format(7)).followingKey(PartialKey.ROW_COLFAM);
    // check that the seek range was bounded using the columns
    assertEquals(new Range(startKey, true, endKey, false), capturedRanges.get(0));

    for (int r = 1; r <= 2; r++) {
      for (int cq = 0; cq < 1; cq++) {
        assertTrue(cfi.hasTop());
        assertEquals(cfi.getTopKey(), newKey(r, 7, cq, 6));
        cfi.next();
      }
    }
    assertFalse(cfi.hasTop());
  }
}

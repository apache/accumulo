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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.accumulo.core.client.impl.BaseIteratorEnvironment;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class RowDeletingIteratorTest extends TestCase {

  public static class TestIE extends BaseIteratorEnvironment {

    private IteratorScope scope;
    private boolean fmc;

    public TestIE(IteratorScope scope, boolean fmc) {
      this.scope = scope;
      this.fmc = fmc;
    }

    @Override
    public IteratorScope getIteratorScope() {
      return scope;
    }

    @Override
    public boolean isFullMajorCompaction() {
      return fmc;
    }
  }

  Key newKey(String row, String cf, String cq, long time) {
    return new Key(new Text(row), new Text(cf), new Text(cq), time);
  }

  void put(TreeMap<Key,Value> tm, String row, String cf, String cq, long time, Value val) {
    tm.put(newKey(row, cf, cq, time), val);
  }

  void put(TreeMap<Key,Value> tm, String row, String cf, String cq, long time, String val) {
    put(tm, row, cf, cq, time, new Value(val.getBytes()));
  }

  private void testAssertions(RowDeletingIterator rdi, String row, String cf, String cq, long time, String val) {
    assertTrue(rdi.hasTop());
    assertEquals(newKey(row, cf, cq, time), rdi.getTopKey());
    assertEquals(val, rdi.getTopValue().toString());
  }

  public void test1() throws Exception {

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    put(tm1, "r1", "", "", 10, RowDeletingIterator.DELETE_ROW_VALUE);
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq3", 5, "v1");
    put(tm1, "r2", "cf1", "cq1", 5, "v1");

    RowDeletingIterator rdi = new RowDeletingIterator();
    rdi.init(new SortedMapIterator(tm1), null, new TestIE(IteratorScope.scan, false));

    rdi.seek(new Range(), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    for (int i = 0; i < 5; i++) {
      rdi.seek(new Range(newKey("r1", "cf1", "cq" + i, 5), null), new ArrayList<ByteSequence>(), false);
      testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");
    }

    rdi.seek(new Range(newKey("r11", "cf1", "cq1", 5), null), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    put(tm1, "r2", "", "", 10, RowDeletingIterator.DELETE_ROW_VALUE);
    rdi.seek(new Range(), new ArrayList<ByteSequence>(), false);
    assertFalse(rdi.hasTop());

    for (int i = 0; i < 5; i++) {
      rdi.seek(new Range(newKey("r1", "cf1", "cq" + i, 5), null), new ArrayList<ByteSequence>(), false);
      assertFalse(rdi.hasTop());
    }

    put(tm1, "r0", "cf1", "cq1", 5, "v1");
    rdi.seek(new Range(), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r0", "cf1", "cq1", 5, "v1");
    rdi.next();
    assertFalse(rdi.hasTop());

  }

  public void test2() throws Exception {

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    put(tm1, "r1", "", "", 10, RowDeletingIterator.DELETE_ROW_VALUE);
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq3", 15, "v1");
    put(tm1, "r1", "cf1", "cq4", 5, "v1");
    put(tm1, "r1", "cf1", "cq5", 15, "v1");
    put(tm1, "r2", "cf1", "cq1", 5, "v1");

    RowDeletingIterator rdi = new RowDeletingIterator();
    rdi.init(new SortedMapIterator(tm1), null, new TestIE(IteratorScope.scan, false));

    rdi.seek(new Range(), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r1", "cf1", "cq3", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r1", "cf1", "cq5", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    rdi.seek(new Range(newKey("r1", "cf1", "cq1", 5), null), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r1", "cf1", "cq3", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r1", "cf1", "cq5", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    rdi.seek(new Range(newKey("r1", "cf1", "cq4", 5), null), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r1", "cf1", "cq5", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    rdi.seek(new Range(newKey("r1", "cf1", "cq5", 20), null), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r1", "cf1", "cq5", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    rdi.seek(new Range(newKey("r1", "cf1", "cq9", 20), null), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");
  }

  public void test3() throws Exception {

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    put(tm1, "r1", "", "", 10, RowDeletingIterator.DELETE_ROW_VALUE);
    put(tm1, "r1", "", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r2", "", "cq1", 5, "v1");
    put(tm1, "r2", "cf1", "cq1", 5, "v1");

    RowDeletingIterator rdi = new RowDeletingIterator();
    rdi.init(new ColumnFamilySkippingIterator(new SortedMapIterator(tm1)), null, new TestIE(IteratorScope.scan, false));

    HashSet<ByteSequence> cols = new HashSet<>();
    cols.add(new ArrayByteSequence("cf1".getBytes()));

    rdi.seek(new Range(), cols, true);
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    cols.clear();
    cols.add(new ArrayByteSequence("".getBytes()));
    rdi.seek(new Range(), cols, false);
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    cols.clear();
    rdi.seek(new Range(), cols, false);
    testAssertions(rdi, "r2", "", "cq1", 5, "v1");
    rdi.next();
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");
  }

  public void test4() throws Exception {

    TreeMap<Key,Value> tm1 = new TreeMap<>();
    put(tm1, "r1", "", "", 10, RowDeletingIterator.DELETE_ROW_VALUE);
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq3", 15, "v1");
    put(tm1, "r1", "cf1", "cq4", 5, "v1");
    put(tm1, "r2", "cf1", "cq1", 5, "v1");

    RowDeletingIterator rdi = new RowDeletingIterator();
    rdi.init(new SortedMapIterator(tm1), null, new TestIE(IteratorScope.minc, false));

    rdi.seek(new Range(), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r1", "", "", 10, RowDeletingIterator.DELETE_ROW_VALUE.toString());
    rdi.next();
    testAssertions(rdi, "r1", "cf1", "cq3", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    rdi.seek(new Range(newKey("r1", "cf1", "cq3", 20), null), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r1", "cf1", "cq3", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

    rdi.seek(new Range(newKey("r1", "", "", 42), null), new ArrayList<ByteSequence>(), false);
    testAssertions(rdi, "r1", "", "", 10, RowDeletingIterator.DELETE_ROW_VALUE.toString());
    rdi.next();
    testAssertions(rdi, "r1", "cf1", "cq3", 15, "v1");
    rdi.next();
    testAssertions(rdi, "r2", "cf1", "cq1", 5, "v1");

  }

}

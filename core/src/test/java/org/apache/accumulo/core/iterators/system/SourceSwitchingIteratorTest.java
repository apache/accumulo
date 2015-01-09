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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator.DataSource;
import org.apache.hadoop.io.Text;

public class SourceSwitchingIteratorTest extends TestCase {

  Key nk(String row, String cf, String cq, long time) {
    return new Key(new Text(row), new Text(cf), new Text(cq), time);
  }

  void put(TreeMap<Key,Value> tm, String row, String cf, String cq, long time, Value val) {
    tm.put(nk(row, cf, cq, time), val);
  }

  void put(TreeMap<Key,Value> tm, String row, String cf, String cq, long time, String val) {
    put(tm, row, cf, cq, time, new Value(val.getBytes()));
  }

  private void ane(SortedKeyValueIterator<Key,Value> rdi, String row, String cf, String cq, long time, String val, boolean callNext) throws Exception {
    assertTrue(rdi.hasTop());
    assertEquals(nk(row, cf, cq, time), rdi.getTopKey());
    assertEquals(val, rdi.getTopValue().toString());
    if (callNext)
      rdi.next();
  }

  class TestDataSource implements DataSource {

    DataSource next;
    SortedKeyValueIterator<Key,Value> iter;
    List<TestDataSource> copies = new ArrayList<TestDataSource>();
    AtomicBoolean iflag;

    TestDataSource(SortedKeyValueIterator<Key,Value> iter) {
      this(iter, new ArrayList<TestDataSource>());
    }

    public TestDataSource(SortedKeyValueIterator<Key,Value> iter, List<TestDataSource> copies) {
      this.iter = iter;
      this.copies = copies;
      copies.add(this);
    }

    @Override
    public DataSource getNewDataSource() {
      return next;
    }

    @Override
    public boolean isCurrent() {
      return next == null;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> iterator() {
      if (iflag != null)
        ((InterruptibleIterator) iter).setInterruptFlag(iflag);
      return iter;
    }

    @Override
    public DataSource getDeepCopyDataSource(IteratorEnvironment env) {
      return new TestDataSource(iter.deepCopy(env), copies);
    }

    void setNext(TestDataSource next) {
      this.next = next;

      for (TestDataSource tds : copies) {
        if (tds != this)
          tds.next = new TestDataSource(next.iter.deepCopy(null), next.copies);
      }
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      this.iflag = flag;
    }
  }

  public void test1() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq3", 5, "v2");
    put(tm1, "r2", "cf1", "cq1", 5, "v3");

    SortedMapIterator smi = new SortedMapIterator(tm1);
    TestDataSource tds = new TestDataSource(smi);
    SourceSwitchingIterator ssi = new SourceSwitchingIterator(tds);

    ssi.seek(new Range(), new ArrayList<ByteSequence>(), false);
    ane(ssi, "r1", "cf1", "cq1", 5, "v1", true);
    ane(ssi, "r1", "cf1", "cq3", 5, "v2", true);
    ane(ssi, "r2", "cf1", "cq1", 5, "v3", true);
    assertFalse(ssi.hasTop());
  }

  public void test2() throws Exception {
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq3", 5, "v2");
    put(tm1, "r2", "cf1", "cq1", 5, "v3");

    SortedMapIterator smi = new SortedMapIterator(tm1);
    TestDataSource tds = new TestDataSource(smi);
    SourceSwitchingIterator ssi = new SourceSwitchingIterator(tds);

    ssi.seek(new Range(), new ArrayList<ByteSequence>(), false);
    ane(ssi, "r1", "cf1", "cq1", 5, "v1", true);

    TreeMap<Key,Value> tm2 = new TreeMap<Key,Value>();
    put(tm2, "r1", "cf1", "cq1", 5, "v4");
    put(tm2, "r1", "cf1", "cq3", 5, "v5");
    put(tm2, "r2", "cf1", "cq1", 5, "v6");

    SortedMapIterator smi2 = new SortedMapIterator(tm2);
    TestDataSource tds2 = new TestDataSource(smi2);
    tds.next = tds2;

    ane(ssi, "r1", "cf1", "cq3", 5, "v2", true);
    ane(ssi, "r2", "cf1", "cq1", 5, "v6", true);
    assertFalse(ssi.hasTop());
  }

  public void test3() throws Exception {
    // test switching after a row

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq2", 5, "v2");
    put(tm1, "r1", "cf1", "cq3", 5, "v3");
    put(tm1, "r1", "cf1", "cq4", 5, "v4");
    put(tm1, "r3", "cf1", "cq1", 5, "v5");
    put(tm1, "r3", "cf1", "cq2", 5, "v6");

    SortedMapIterator smi = new SortedMapIterator(tm1);
    TestDataSource tds = new TestDataSource(smi);
    SourceSwitchingIterator ssi = new SourceSwitchingIterator(tds, true);

    ssi.seek(new Range(), new ArrayList<ByteSequence>(), false);
    ane(ssi, "r1", "cf1", "cq1", 5, "v1", true);

    TreeMap<Key,Value> tm2 = new TreeMap<Key,Value>(tm1);
    put(tm2, "r1", "cf1", "cq5", 5, "v7"); // should not see this because it should not switch until the row is finished
    put(tm2, "r2", "cf1", "cq1", 5, "v8"); // should see this new row after it switches

    // setup a new data source, but it should not switch until the current row is finished
    SortedMapIterator smi2 = new SortedMapIterator(tm2);
    TestDataSource tds2 = new TestDataSource(smi2);
    tds.next = tds2;

    ane(ssi, "r1", "cf1", "cq2", 5, "v2", true);
    ane(ssi, "r1", "cf1", "cq3", 5, "v3", true);
    ane(ssi, "r1", "cf1", "cq4", 5, "v4", true);
    ane(ssi, "r2", "cf1", "cq1", 5, "v8", true);
    ane(ssi, "r3", "cf1", "cq1", 5, "v5", true);
    ane(ssi, "r3", "cf1", "cq2", 5, "v6", true);

  }

  public void test4() throws Exception {
    // ensure switch is done on initial seek
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq2", 5, "v2");

    SortedMapIterator smi = new SortedMapIterator(tm1);
    TestDataSource tds = new TestDataSource(smi);
    SourceSwitchingIterator ssi = new SourceSwitchingIterator(tds, false);

    TreeMap<Key,Value> tm2 = new TreeMap<Key,Value>();
    put(tm2, "r1", "cf1", "cq1", 6, "v3");
    put(tm2, "r1", "cf1", "cq2", 6, "v4");

    SortedMapIterator smi2 = new SortedMapIterator(tm2);
    TestDataSource tds2 = new TestDataSource(smi2);
    tds.next = tds2;

    ssi.seek(new Range(), new ArrayList<ByteSequence>(), false);

    ane(ssi, "r1", "cf1", "cq1", 6, "v3", true);
    ane(ssi, "r1", "cf1", "cq2", 6, "v4", true);

  }

  public void test5() throws Exception {
    // esnure switchNow() works w/ deepCopy()
    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    put(tm1, "r1", "cf1", "cq1", 5, "v1");
    put(tm1, "r1", "cf1", "cq2", 5, "v2");

    SortedMapIterator smi = new SortedMapIterator(tm1);
    TestDataSource tds = new TestDataSource(smi);
    SourceSwitchingIterator ssi = new SourceSwitchingIterator(tds, false);

    SortedKeyValueIterator<Key,Value> dc1 = ssi.deepCopy(null);

    TreeMap<Key,Value> tm2 = new TreeMap<Key,Value>();
    put(tm2, "r1", "cf1", "cq1", 6, "v3");
    put(tm2, "r2", "cf1", "cq2", 6, "v4");

    SortedMapIterator smi2 = new SortedMapIterator(tm2);
    TestDataSource tds2 = new TestDataSource(smi2);
    tds.setNext(tds2);

    ssi.switchNow();

    ssi.seek(new Range("r1"), new ArrayList<ByteSequence>(), false);
    dc1.seek(new Range("r2"), new ArrayList<ByteSequence>(), false);

    ane(ssi, "r1", "cf1", "cq1", 6, "v3", true);
    assertFalse(ssi.hasTop());
    ane(dc1, "r2", "cf1", "cq2", 6, "v4", true);
    assertFalse(dc1.hasTop());
  }

  public void testSetInterrupt() throws Exception {

    TreeMap<Key,Value> tm1 = new TreeMap<Key,Value>();
    put(tm1, "r1", "cf1", "cq1", 5, "v1");

    SortedMapIterator smi = new SortedMapIterator(tm1);
    TestDataSource tds = new TestDataSource(smi);
    SourceSwitchingIterator ssi = new SourceSwitchingIterator(tds, false);

    AtomicBoolean flag = new AtomicBoolean();
    ssi.setInterruptFlag(flag);

    assertSame(flag, tds.iflag);

    ssi.seek(new Range("r1"), new ArrayList<ByteSequence>(), false);
    ane(ssi, "r1", "cf1", "cq1", 5, "v1", true);
    assertFalse(ssi.hasTop());

    flag.set(true);

    try {
      ssi.seek(new Range("r1"), new ArrayList<ByteSequence>(), false);
      fail("expected to see IterationInterruptedException");
    } catch (IterationInterruptedException iie) {}

  }
}

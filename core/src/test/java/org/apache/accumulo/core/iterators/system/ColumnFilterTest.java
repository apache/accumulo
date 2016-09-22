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

import java.util.HashSet;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class ColumnFilterTest extends TestCase {

  Key newKey(String row, String cf, String cq) {
    return new Key(new Text(row), new Text(cf), new Text(cq));
  }

  Column newColumn(String cf) {
    return new Column(cf.getBytes(), null, null);
  }

  Column newColumn(String cf, String cq) {
    return new Column(cf.getBytes(), cq.getBytes(), null);
  }

  public void test1() {
    HashSet<Column> columns = new HashSet<>();

    columns.add(newColumn("cf1"));

    ColumnQualifierFilter cf = new ColumnQualifierFilter(null, columns);

    assertTrue(cf.accept(newKey("r1", "cf1", "cq1"), new Value(new byte[0])));
    assertTrue(cf.accept(newKey("r1", "cf2", "cq1"), new Value(new byte[0])));

  }

  public void test2() {
    HashSet<Column> columns = new HashSet<>();

    columns.add(newColumn("cf1"));
    columns.add(newColumn("cf2", "cq1"));

    ColumnQualifierFilter cf = new ColumnQualifierFilter(null, columns);

    assertTrue(cf.accept(newKey("r1", "cf1", "cq1"), new Value(new byte[0])));
    assertTrue(cf.accept(newKey("r1", "cf2", "cq1"), new Value(new byte[0])));
    assertFalse(cf.accept(newKey("r1", "cf2", "cq2"), new Value(new byte[0])));
  }

  public void test3() {
    HashSet<Column> columns = new HashSet<>();

    columns.add(newColumn("cf2", "cq1"));

    ColumnQualifierFilter cf = new ColumnQualifierFilter(null, columns);

    assertFalse(cf.accept(newKey("r1", "cf1", "cq1"), new Value(new byte[0])));
    assertTrue(cf.accept(newKey("r1", "cf2", "cq1"), new Value(new byte[0])));
    assertFalse(cf.accept(newKey("r1", "cf2", "cq2"), new Value(new byte[0])));
  }

  private static class CloseTestIter extends SortedMapIterator {

    int closeCallCount = 0;

    public CloseTestIter(SortedMap<Key,Value> map) {
      super(map);
    }

    @Override
    public void close() {
      System.out.println("Closing inner CloseIterator.");
      closeCallCount++;
    }
  }

  public void testClose() throws Exception {
    TreeMap<Key,Value> tm = new TreeMap<>();
    tm.put(nk("r1", "cf1", "cq1"), new Value("val1".getBytes()));
    tm.put(nk("r2", "cf2", "cq1"), new Value("val2".getBytes()));
    HashSet<Column> columns = new HashSet<>();
    columns.add(nc("cf2", "cq1"));

    CloseTestIter closeIter = new CloseTestIter(tm);
    ColumnQualifierFilter cf = new ColumnQualifierFilter(closeIter, columns);

    assertEquals(0, closeIter.closeCallCount);
    assertTrue(cf.accept(nk("r1", "cf2", "cq1"), new Value(new byte[0])));
    assertFalse(cf.accept(nk("r1", "cf2", "cq2"), new Value(new byte[0])));

    System.out.println("Closing ColumnQualifierFilter");
    cf.close();

    assertEquals(1, closeIter.closeCallCount);
  }
}

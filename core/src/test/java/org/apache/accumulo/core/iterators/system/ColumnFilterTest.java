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

import java.util.Collections;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.Assert;

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
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(newKey("r1", "cf1", "cq1"), new Value(""));
    data.put(newKey("r1", "cf2", "cq1"), new Value(""));

    HashSet<Column> columns = new HashSet<>();
    columns.add(newColumn("cf1"));

    SortedMapIterator smi = new SortedMapIterator(data);
    SortedKeyValueIterator<Key,Value> cf = ColumnQualifierFilter.wrap(smi, columns);

    Assert.assertSame(smi, cf);
  }

  public void test2() throws Exception {

    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(newKey("r1", "cf1", "cq1"), new Value(""));
    data.put(newKey("r1", "cf2", "cq1"), new Value(""));
    data.put(newKey("r1", "cf2", "cq2"), new Value(""));

    HashSet<Column> columns = new HashSet<>();

    columns.add(newColumn("cf1"));
    columns.add(newColumn("cf2", "cq1"));

    SortedKeyValueIterator<Key,Value> cf = ColumnQualifierFilter.wrap(new SortedMapIterator(data), columns);
    cf.seek(new Range(), Collections.emptySet(), false);

    Assert.assertTrue(cf.hasTop());
    Assert.assertEquals(newKey("r1", "cf1", "cq1"), cf.getTopKey());
    cf.next();
    Assert.assertTrue(cf.hasTop());
    Assert.assertEquals(newKey("r1", "cf2", "cq1"), cf.getTopKey());
    cf.next();
    Assert.assertFalse(cf.hasTop());
  }

  public void test3() throws Exception {

    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(newKey("r1", "cf1", "cq1"), new Value(""));
    data.put(newKey("r1", "cf2", "cq1"), new Value(""));
    data.put(newKey("r1", "cf2", "cq2"), new Value(""));

    HashSet<Column> columns = new HashSet<>();

    columns.add(newColumn("cf2", "cq1"));

    SortedKeyValueIterator<Key,Value> cf = ColumnQualifierFilter.wrap(new SortedMapIterator(data), columns);
    cf.seek(new Range(), Collections.emptySet(), false);

    Assert.assertTrue(cf.hasTop());
    Assert.assertEquals(newKey("r1", "cf2", "cq1"), cf.getTopKey());
    cf.next();
    Assert.assertFalse(cf.hasTop());
  }
}

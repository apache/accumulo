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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.hadoop.io.Text;
import org.junit.Assert;

public class WholeColumnFamilyIteratorTest extends TestCase {

  public void testEmptyStuff() throws IOException {
    SortedMap<Key,Value> map = new TreeMap<Key,Value>();
    SortedMap<Key,Value> map2 = new TreeMap<Key,Value>();
    final Map<Text,Boolean> toInclude = new HashMap<Text,Boolean>();
    map.put(new Key(new Text("r1"), new Text("cf1"), new Text("cq1"), new Text("cv1"), 1l), new Value("val1".getBytes()));
    map.put(new Key(new Text("r1"), new Text("cf1"), new Text("cq2"), new Text("cv1"), 2l), new Value("val2".getBytes()));
    map.put(new Key(new Text("r2"), new Text("cf1"), new Text("cq1"), new Text("cv1"), 3l), new Value("val3".getBytes()));
    map.put(new Key(new Text("r2"), new Text("cf2"), new Text("cq1"), new Text("cv1"), 4l), new Value("val4".getBytes()));
    map.put(new Key(new Text("r3"), new Text("cf1"), new Text("cq1"), new Text("cv1"), 5l), new Value("val4".getBytes()));
    map.put(new Key(new Text("r3"), new Text("cf1"), new Text("cq1"), new Text("cv2"), 6l), new Value("val4".getBytes()));
    map.put(new Key(new Text("r4"), new Text("cf1"), new Text("cq1"), new Text("cv1"), 7l), new Value("".getBytes()));
    map.put(new Key(new Text("r4"), new Text("cf1"), new Text("cq1"), new Text(""), 8l), new Value("val1".getBytes()));
    map.put(new Key(new Text("r4"), new Text("cf1"), new Text(""), new Text("cv1"), 9l), new Value("val1".getBytes()));
    map.put(new Key(new Text("r4"), new Text(""), new Text("cq1"), new Text("cv1"), 10l), new Value("val1".getBytes()));
    map.put(new Key(new Text(""), new Text("cf1"), new Text("cq1"), new Text("cv1"), 11l), new Value("val1".getBytes()));
    boolean b = true;
    int trueCount = 0;
    for (Key k : map.keySet()) {
      if (toInclude.containsKey(k.getRow())) {
        if (toInclude.get(k.getRow())) {
          map2.put(k, map.get(k));
        }
        continue;
      }
      b = !b;
      toInclude.put(k.getRow(), b);
      if (b) {
        trueCount++;
        map2.put(k, map.get(k));
      }
    }
    SortedMapIterator source = new SortedMapIterator(map);
    WholeColumnFamilyIterator iter = new WholeColumnFamilyIterator(source);
    SortedMap<Key,Value> resultMap = new TreeMap<Key,Value>();
    iter.seek(new Range(), new ArrayList<ByteSequence>(), false);
    int numRows = 0;
    while (iter.hasTop()) {
      numRows++;
      Key rowKey = iter.getTopKey();
      Value rowValue = iter.getTopValue();
      resultMap.putAll(WholeColumnFamilyIterator.decodeColumnFamily(rowKey, rowValue));
      iter.next();
    }

    // we have 7 groups of row key/cf
    Assert.assertEquals(7, numRows);

    assertEquals(resultMap, map);

    WholeColumnFamilyIterator iter2 = new WholeColumnFamilyIterator(source) {
      @Override
      public boolean filter(Text row, List<Key> keys, List<Value> values) {
        return toInclude.get(row);
      }
    };
    resultMap.clear();
    iter2.seek(new Range(), new ArrayList<ByteSequence>(), false);
    numRows = 0;
    while (iter2.hasTop()) {
      numRows++;
      Key rowKey = iter2.getTopKey();
      Value rowValue = iter2.getTopValue();
      resultMap.putAll(WholeColumnFamilyIterator.decodeColumnFamily(rowKey, rowValue));
      iter2.next();
    }
    assertTrue(numRows == trueCount);
    assertEquals(resultMap, map2);
  }

  private void pkv(SortedMap<Key,Value> map, String row, String cf, String cq, String cv, long ts, String val) {
    map.put(new Key(new Text(row), new Text(cf), new Text(cq), new Text(cv), ts), new Value(val.getBytes()));
  }

  public void testContinue() throws Exception {
    SortedMap<Key,Value> map1 = new TreeMap<Key,Value>();
    pkv(map1, "row1", "cf1", "cq1", "cv1", 5, "foo");
    pkv(map1, "row1", "cf1", "cq2", "cv1", 6, "bar");

    SortedMap<Key,Value> map2 = new TreeMap<Key,Value>();
    pkv(map2, "row2", "cf1", "cq1", "cv1", 5, "foo");
    pkv(map2, "row2", "cf1", "cq2", "cv1", 6, "bar");

    SortedMap<Key,Value> map3 = new TreeMap<Key,Value>();
    pkv(map3, "row3", "cf1", "cq1", "cv1", 5, "foo");
    pkv(map3, "row3", "cf1", "cq2", "cv1", 6, "bar");

    SortedMap<Key,Value> map = new TreeMap<Key,Value>();
    map.putAll(map1);
    map.putAll(map2);
    map.putAll(map3);

    SortedMapIterator source = new SortedMapIterator(map);
    WholeColumnFamilyIterator iter = new WholeColumnFamilyIterator(source);

    Range range = new Range(new Text("row1"), true, new Text("row2"), true);
    iter.seek(range, new ArrayList<ByteSequence>(), false);

    assertTrue(iter.hasTop());
    assertEquals(map1, WholeColumnFamilyIterator.decodeColumnFamily(iter.getTopKey(), iter.getTopValue()));

    // simulate something continuing using the last key from the iterator
    // this is what client and server code will do
    range = new Range(iter.getTopKey(), false, range.getEndKey(), range.isEndKeyInclusive());
    iter.seek(range, new ArrayList<ByteSequence>(), false);

    assertTrue(iter.hasTop());
    assertEquals(map2, WholeColumnFamilyIterator.decodeColumnFamily(iter.getTopKey(), iter.getTopValue()));

    iter.next();

    assertFalse(iter.hasTop());

  }

  public void testBug1() throws Exception {
    SortedMap<Key,Value> map1 = new TreeMap<Key,Value>();
    pkv(map1, "row1", "cf1", "cq1", "cv1", 5, "foo");
    pkv(map1, "row1", "cf1", "cq2", "cv1", 6, "bar");

    SortedMap<Key,Value> map2 = new TreeMap<Key,Value>();
    pkv(map2, "row2", "cf1", "cq1", "cv1", 5, "foo");

    SortedMap<Key,Value> map = new TreeMap<Key,Value>();
    map.putAll(map1);
    map.putAll(map2);

    MultiIterator source = new MultiIterator(Collections.singletonList((SortedKeyValueIterator<Key,Value>) new SortedMapIterator(map)), new Range(null, true,
        new Text("row1"), true));
    WholeColumnFamilyIterator iter = new WholeColumnFamilyIterator(source);

    Range range = new Range(new Text("row1"), true, new Text("row2"), true);
    iter.seek(range, new ArrayList<ByteSequence>(), false);

    assertTrue(iter.hasTop());
    assertEquals(map1, WholeColumnFamilyIterator.decodeColumnFamily(iter.getTopKey(), iter.getTopValue()));

    // simulate something continuing using the last key from the iterator
    // this is what client and server code will do
    range = new Range(iter.getTopKey(), false, range.getEndKey(), range.isEndKeyInclusive());
    iter.seek(range, new ArrayList<ByteSequence>(), false);

    assertFalse(iter.hasTop());

  }

}

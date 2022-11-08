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
package org.apache.accumulo.core.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.CountingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class FirstEntryInRowIteratorTest {

  private static long process(TreeMap<Key,Value> sourceMap, TreeMap<Key,Value> resultMap,
      Range range, IteratorSetting iteratorSetting) throws IOException {
    SortedMapIterator source = new SortedMapIterator(sourceMap);
    CountingIterator counter = new CountingIterator(source);
    FirstEntryInRowIterator feiri = new FirstEntryInRowIterator();
    IteratorEnvironment env = new DefaultIteratorEnvironment();

    feiri.init(counter, iteratorSetting.getOptions(), env);

    feiri.seek(range, Set.of(), false);
    while (feiri.hasTop()) {
      resultMap.put(feiri.getTopKey(), feiri.getTopValue());
      feiri.next();
    }
    return counter.getCount();
  }

  @Test
  public void test() throws IOException {
    TreeMap<Key,Value> sourceMap = new TreeMap<>();
    Value emptyValue = new Value("");
    IteratorSetting iteratorSetting = new IteratorSetting(1, FirstEntryInRowIterator.class);
    FirstEntryInRowIterator.setNumScansBeforeSeek(iteratorSetting, 10);
    assertTrue(
        iteratorSetting.getOptions().containsKey(FirstEntryInRowIterator.NUM_SCANS_STRING_NAME));
    sourceMap.put(new Key("r1", "cf", "cq"), emptyValue);
    sourceMap.put(new Key("r2", "cf", "cq"), emptyValue);
    sourceMap.put(new Key("r3", "cf", "cq"), emptyValue);
    TreeMap<Key,Value> resultMap = new TreeMap<>();
    long numSourceEntries = sourceMap.size();
    long numNexts = process(sourceMap, resultMap, new Range(), iteratorSetting);
    assertEquals(numNexts, numSourceEntries);
    assertEquals(sourceMap.size(), resultMap.size());

    for (int i = 0; i < 20; i++) {
      sourceMap.put(new Key("r2", "cf", "cq" + i), emptyValue);
    }
    resultMap.clear();

    numNexts = process(sourceMap, resultMap,
        new Range(new Key("r1"), (new Key("r2")).followingKey(PartialKey.ROW)), iteratorSetting);
    assertEquals(numNexts, resultMap.size() + 10);
    assertEquals(resultMap.size(), 2);

    resultMap.clear();
    numNexts = process(sourceMap, resultMap, new Range(new Key("r1"), new Key("r2", "cf2")),
        iteratorSetting);
    assertEquals(numNexts, resultMap.size() + 10);
    assertEquals(resultMap.size(), 2);

    resultMap.clear();
    numNexts =
        process(sourceMap, resultMap, new Range(new Key("r1"), new Key("r4")), iteratorSetting);
    assertEquals(numNexts, resultMap.size() + 10);
    assertEquals(resultMap.size(), 3);
  }

}

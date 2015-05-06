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

import java.io.IOException;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.system.CountingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

public class FirstEntryInRowIteratorTest {

  @SuppressWarnings("unchecked")
  private static long process(TreeMap<Key,Value> sourceMap, TreeMap<Key,Value> resultMap, Range range, int numScans) throws IOException {
    org.apache.accumulo.core.iterators.SortedMapIterator source = new SortedMapIterator(sourceMap);
    CountingIterator counter = new CountingIterator(source);
    FirstEntryInRowIterator feiri = new FirstEntryInRowIterator();
    IteratorEnvironment env = new IteratorEnvironment() {

      @Override
      public AccumuloConfiguration getConfig() {
        return null;
      }

      @Override
      public IteratorScope getIteratorScope() {
        return null;
      }

      @Override
      public boolean isFullMajorCompaction() {
        return false;
      }

      @Override
      public void registerSideChannel(SortedKeyValueIterator<Key,Value> arg0) {

      }

      @Override
      public Authorizations getAuthorizations() {
        return null;
      }

      @Override
      public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String arg0) throws IOException {
        return null;
      }
    };

    feiri.init(counter, Collections.singletonMap(FirstEntryInRowIterator.NUM_SCANS_STRING_NAME, Integer.toString(numScans)), env);

    feiri.seek(range, Collections.EMPTY_SET, false);
    while (feiri.hasTop()) {
      resultMap.put(feiri.getTopKey(), feiri.getTopValue());
      feiri.next();
    }
    return counter.getCount();
  }

  @Test
  public void test() throws IOException {
    TreeMap<Key,Value> sourceMap = new TreeMap<Key,Value>();
    Value emptyValue = new Value("".getBytes());
    sourceMap.put(new Key("r1", "cf", "cq"), emptyValue);
    sourceMap.put(new Key("r2", "cf", "cq"), emptyValue);
    sourceMap.put(new Key("r3", "cf", "cq"), emptyValue);
    TreeMap<Key,Value> resultMap = new TreeMap<Key,Value>();
    long numSourceEntries = sourceMap.size();
    long numNexts = process(sourceMap, resultMap, new Range(), 10);
    assertEquals(numNexts, numSourceEntries);
    assertEquals(sourceMap.size(), resultMap.size());

    for (int i = 0; i < 20; i++) {
      sourceMap.put(new Key("r2", "cf", "cq" + i), emptyValue);
    }
    resultMap.clear();
    numNexts = process(sourceMap, resultMap, new Range(new Key("r1"), (new Key("r2")).followingKey(PartialKey.ROW)), 10);
    assertEquals(numNexts, resultMap.size() + 10);
    assertEquals(resultMap.size(), 2);

    resultMap.clear();
    numNexts = process(sourceMap, resultMap, new Range(new Key("r1"), new Key("r2", "cf2")), 10);
    assertEquals(numNexts, resultMap.size() + 10);
    assertEquals(resultMap.size(), 2);

    resultMap.clear();
    numNexts = process(sourceMap, resultMap, new Range(new Key("r1"), new Key("r4")), 10);
    assertEquals(numNexts, resultMap.size() + 10);
    assertEquals(resultMap.size(), 3);
  }

}

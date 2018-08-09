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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class ExactDeletingIteratorTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private Key nk(Object... entry) {
    Key k = new Key((String) entry[0], (String) entry[1], (String) entry[2]);
    k.setTimestamp((int) entry[3]);
    k.setDeleted((boolean) entry[4]);
    return k;
  }

  private Range nr(Key key, boolean inclusive) {
    return new Range(key, inclusive, null, false);
  }

  private TreeMap<Key,Value> parse(Object[][] data) {
    TreeMap<Key,Value> tm = new TreeMap<>();

    for (Object[] entry : data) {
      Key k = nk(entry);
      Value v = new Value((String) entry[5]);
      tm.put(k, v);
    }

    return tm;
  }

  private void runTest(Object[][] testData, Range range, boolean propDel, Object[][] expectedData)
      throws IOException {
    ExactDeletingIterator iter = new ExactDeletingIterator(new SortedMapIterator(parse(testData)),
        propDel);
    TreeMap<Key,Value> actual = new TreeMap<>();

    iter.seek(range, EMPTY_COL_FAMS, false);

    while (iter.hasTop()) {
      actual.put(iter.getTopKey(), iter.getTopValue());
      iter.next();
    }

    Assert.assertEquals(parse(expectedData), actual);
  }

  private void runTest(Object[][] testData, boolean propDel, Object[][] expectedData)
      throws IOException {
    runTest(testData, new Range(), propDel, expectedData);
  }

  @Test
  public void testEverything() throws Exception {
    // @formatter:off
    Object[][] data = {
        {"r01", "f01", "q01", 7, true, ""},
        {"r01", "f01", "q01", 6, false, "v01"},
        {"r01", "f01", "q01", 5, true, ""},
        {"r01", "f01", "q01", 5, false, "v02"},
        {"r01", "f01", "q01", 4, false, "v03"},
        {"r01", "f01", "q01", 3, true, ""},
        {"r01", "f01", "q01", 3, false, "v04"},
        {"r01", "f01", "q01", 2, true, ""},
        {"r01", "f01", "q01", 2, false, "v05"},
        {"r01", "f01", "q01", 1, false, "v06"}};


    Object[][] expected1 = {
        {"r01", "f01", "q01", 6, false, "v01"},
        {"r01", "f01", "q01", 4, false, "v03"},
        {"r01", "f01", "q01", 1, false, "v06"}};

    Object[][] expected2 = {
        {"r01", "f01", "q01", 7, true, ""},
        {"r01", "f01", "q01", 6, false, "v01"},
        {"r01", "f01", "q01", 5, true, ""},
        {"r01", "f01", "q01", 4, false, "v03"},
        {"r01", "f01", "q01", 3, true, ""},
        {"r01", "f01", "q01", 2, true, ""},
        {"r01", "f01", "q01", 1, false, "v06"}};

    Object[][] expected3 = {
        {"r01", "f01", "q01", 4, false, "v03"},
        {"r01", "f01", "q01", 1, false, "v06"}};

    Object[][] expected4 = {
        {"r01", "f01", "q01", 5, true, ""},
        {"r01", "f01", "q01", 4, false, "v03"},
        {"r01", "f01", "q01", 3, true, ""},
        {"r01", "f01", "q01", 2, true, ""},
        {"r01", "f01", "q01", 1, false, "v06"}};

    Object[][] expected5 = {
        {"r01", "f01", "q01", 4, false, "v03"},
        {"r01", "f01", "q01", 3, true, ""},
        {"r01", "f01", "q01", 2, true, ""},
        {"r01", "f01", "q01", 1, false, "v06"}};
     // @formatter:on

    runTest(data, false, expected1);
    runTest(data, true, expected2);

    runTest(expected1, true, expected1);
    runTest(expected1, false, expected1);

    runTest(expected2, false, expected1);
    runTest(expected2, true, expected2);

    runTest(data, nr(nk("r01", "f01", "q01", 5, false), false), false, expected3);
    runTest(data, nr(nk("r01", "f01", "q01", 5, false), true), false, expected3);
    runTest(data, nr(nk("r01", "f01", "q01", 5, true), false), false, expected3);
    runTest(data, nr(nk("r01", "f01", "q01", 5, false), true), false, expected3);

    runTest(data, nr(nk("r01", "f01", "q01", 4, false), true), false, expected3);
    runTest(data, nr(nk("r01", "f01", "q01", 4, true), false), false, expected3);
    runTest(data, nr(nk("r01", "f01", "q01", 4, true), true), false, expected3);

    runTest(data, nr(nk("r01", "f01", "q01", 5, true), true), true, expected4);
    runTest(data, nr(nk("r01", "f01", "q01", 5, false), true), true, expected5);
    runTest(data, nr(nk("r01", "f01", "q01", 5, true), false), true, expected5);
    runTest(data, nr(nk("r01", "f01", "q01", 5, false), false), true, expected5);
  }
}

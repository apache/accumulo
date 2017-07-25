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
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.util.LocalityGroupUtil;

import junit.framework.TestCase;

public class LargeRowFilterTest extends TestCase {

  private String genRow(int r) {
    return String.format("row%03d", r);
  }

  private String genCQ(int cq) {
    return String.format("cf%03d", cq);
  }

  private void genRow(TreeMap<Key,Value> testData, int row, int startCQ, int stopCQ) {
    for (int cq = startCQ; cq < stopCQ; cq++) {
      testData.put(new Key(genRow(row), "cf001", genCQ(cq), 5), new Value(("v" + row + "_" + cq).getBytes()));
    }
  }

  private void genTestData(TreeMap<Key,Value> testData, int numRows) {
    for (int i = 1; i <= numRows; i++) {
      genRow(testData, i, 0, i);
    }
  }

  private LargeRowFilter setupIterator(TreeMap<Key,Value> testData, int maxColumns, IteratorScope scope) throws IOException {
    SortedMapIterator smi = new SortedMapIterator(testData);
    LargeRowFilter lrfi = new LargeRowFilter();
    IteratorSetting is = new IteratorSetting(1, LargeRowFilter.class);
    LargeRowFilter.setMaxColumns(is, maxColumns);
    lrfi.init(new ColumnFamilySkippingIterator(smi), is.getOptions(), new RowDeletingIteratorTest.TestIE(scope, false));
    return lrfi;
  }

  public void testBasic() throws Exception {
    TreeMap<Key,Value> testData = new TreeMap<>();

    genTestData(testData, 20);

    for (int i = 1; i <= 20; i++) {
      TreeMap<Key,Value> expectedData = new TreeMap<>();
      genTestData(expectedData, i);

      LargeRowFilter lrfi = setupIterator(testData, i, IteratorScope.scan);
      lrfi.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);

      TreeMap<Key,Value> filteredData = new TreeMap<>();

      while (lrfi.hasTop()) {
        filteredData.put(lrfi.getTopKey(), lrfi.getTopValue());
        lrfi.next();
      }

      assertEquals(expectedData, filteredData);
    }
  }

  public void testSeek() throws Exception {
    TreeMap<Key,Value> testData = new TreeMap<>();

    genTestData(testData, 20);

    for (int i = 1; i <= 20; i++) {
      TreeMap<Key,Value> expectedData = new TreeMap<>();
      genTestData(expectedData, i);

      LargeRowFilter lrfi = setupIterator(testData, i, IteratorScope.scan);

      TreeMap<Key,Value> filteredData = new TreeMap<>();

      // seek to each row... rows that exceed max columns should be filtered
      for (int j = 1; j <= i; j++) {
        lrfi.seek(new Range(genRow(j), genRow(j)), LocalityGroupUtil.EMPTY_CF_SET, false);

        while (lrfi.hasTop()) {
          assertEquals(genRow(j), lrfi.getTopKey().getRow().toString());
          filteredData.put(lrfi.getTopKey(), lrfi.getTopValue());
          lrfi.next();
        }
      }

      assertEquals(expectedData, filteredData);
    }
  }

  public void testSeek2() throws Exception {
    TreeMap<Key,Value> testData = new TreeMap<>();

    genTestData(testData, 20);

    LargeRowFilter lrfi = setupIterator(testData, 13, IteratorScope.scan);

    // test seeking to the middle of a row
    lrfi.seek(new Range(new Key(genRow(15), "cf001", genCQ(4), 5), true, new Key(genRow(15)).followingKey(PartialKey.ROW), false),
        LocalityGroupUtil.EMPTY_CF_SET, false);
    assertFalse(lrfi.hasTop());

    lrfi.seek(new Range(new Key(genRow(10), "cf001", genCQ(4), 5), true, new Key(genRow(10)).followingKey(PartialKey.ROW), false),
        LocalityGroupUtil.EMPTY_CF_SET, false);
    TreeMap<Key,Value> expectedData = new TreeMap<>();
    genRow(expectedData, 10, 4, 10);

    TreeMap<Key,Value> filteredData = new TreeMap<>();
    while (lrfi.hasTop()) {
      filteredData.put(lrfi.getTopKey(), lrfi.getTopValue());
      lrfi.next();
    }

    assertEquals(expectedData, filteredData);
  }

  public void testCompaction() throws Exception {
    TreeMap<Key,Value> testData = new TreeMap<>();

    genTestData(testData, 20);

    LargeRowFilter lrfi = setupIterator(testData, 13, IteratorScope.majc);
    lrfi.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);

    TreeMap<Key,Value> compactedData = new TreeMap<>();
    while (lrfi.hasTop()) {
      compactedData.put(lrfi.getTopKey(), lrfi.getTopValue());
      lrfi.next();
    }

    // compacted data should now contain suppression markers
    // add column to row that should be suppressed\
    genRow(compactedData, 15, 15, 16);

    // scanning over data w/ higher max columns should not change behavior
    // because there are suppression markers.. if there was a bug and data
    // was not suppressed, increasing the threshold would expose the bug
    lrfi = setupIterator(compactedData, 20, IteratorScope.scan);
    lrfi.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);

    // only expect to see 13 rows
    TreeMap<Key,Value> expectedData = new TreeMap<>();
    genTestData(expectedData, 13);

    TreeMap<Key,Value> filteredData = new TreeMap<>();
    while (lrfi.hasTop()) {
      filteredData.put(lrfi.getTopKey(), lrfi.getTopValue());
      lrfi.next();
    }

    assertEquals(expectedData.size() + 8, compactedData.size());
    assertEquals(expectedData, filteredData);

    // try seeking to the middle of row 15... row has data and suppression marker... this seeks past the marker but before the column
    lrfi.seek(new Range(new Key(genRow(15), "cf001", genCQ(4), 5), true, new Key(genRow(15)).followingKey(PartialKey.ROW), false),
        LocalityGroupUtil.EMPTY_CF_SET, false);
    assertFalse(lrfi.hasTop());

    // test seeking w/ column families
    HashSet<ByteSequence> colfams = new HashSet<>();
    colfams.add(new ArrayByteSequence("cf001"));
    lrfi.seek(new Range(new Key(genRow(15), "cf001", genCQ(4), 5), true, new Key(genRow(15)).followingKey(PartialKey.ROW), false), colfams, true);
    assertFalse(lrfi.hasTop());
  }

  // in other test data is generated in such a way that once a row
  // is suppressed, all subsequent rows are suppressed
  public void testSuppressInner() throws Exception {
    TreeMap<Key,Value> testData = new TreeMap<>();
    genRow(testData, 1, 0, 2);
    genRow(testData, 2, 0, 50);
    genRow(testData, 3, 0, 15);
    genRow(testData, 4, 0, 5);

    TreeMap<Key,Value> expectedData = new TreeMap<>();
    genRow(expectedData, 1, 0, 2);
    genRow(expectedData, 4, 0, 5);

    LargeRowFilter lrfi = setupIterator(testData, 13, IteratorScope.scan);
    lrfi.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);

    TreeMap<Key,Value> filteredData = new TreeMap<>();
    while (lrfi.hasTop()) {
      filteredData.put(lrfi.getTopKey(), lrfi.getTopValue());
      lrfi.next();
    }

    assertEquals(expectedData, filteredData);

  }
}

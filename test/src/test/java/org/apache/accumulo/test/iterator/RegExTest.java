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
package org.apache.accumulo.test.iterator;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class RegExTest {

  private static TreeMap<Key,Value> data = new TreeMap<>();

  @BeforeClass
  public static void setupTests() throws Exception {

    ArrayList<Character> chars = new ArrayList<>();
    for (char c = 'a'; c <= 'z'; c++)
      chars.add(c);

    for (char c = '0'; c <= '9'; c++)
      chars.add(c);

    // insert some data into accumulo
    for (Character rc : chars) {
      String row = "r" + rc;
      for (Character cfc : chars) {
        for (Character cqc : chars) {
          Value v = new Value(("v" + rc + cfc + cqc).getBytes());
          data.put(new Key(row, "cf" + cfc, "cq" + cqc, "", 9), v);
        }
      }
    }
  }

  private void check(String regex, String val) throws Exception {
    if (regex != null && !val.matches(regex))
      throw new Exception(" " + val + " does not match " + regex);
  }

  private void check(String regex, Text val) throws Exception {
    check(regex, val.toString());
  }

  private void check(String regex, Value val) throws Exception {
    check(regex, val.toString());
  }

  @Test
  public void runTest1() throws Exception {
    // try setting all regex
    Range range = new Range(new Text("rf"), true, new Text("rl"), true);
    runTest(range, "r[g-k]", "cf[1-5]", "cq[x-z]", "v[g-k][1-5][t-y]", 5 * 5 * (3 - 1));
  }

  @Test
  public void runTest2() throws Exception {
    // try setting only a row regex
    Range range = new Range(new Text("rf"), true, new Text("rl"), true);
    runTest(range, "r[g-k]", null, null, null, 5 * 36 * 36);
  }

  @Test
  public void runTest3() throws Exception {
    // try setting only a col fam regex
    Range range = new Range((Key) null, (Key) null);
    runTest(range, null, "cf[a-f]", null, null, 36 * 6 * 36);
  }

  @Test
  public void runTest4() throws Exception {
    // try setting only a col qual regex
    Range range = new Range((Key) null, (Key) null);
    runTest(range, null, null, "cq[1-7]", null, 36 * 36 * 7);
  }

  @Test
  public void runTest5() throws Exception {
    // try setting only a value regex
    Range range = new Range((Key) null, (Key) null);
    runTest(range, null, null, null, "v[a-c][d-f][g-i]", 3 * 3 * 3);
  }

  private void runTest(Range range, String rowRegEx, String cfRegEx, String cqRegEx, String valRegEx, int expected) throws Exception {

    SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);
    Set<ByteSequence> es = ImmutableSet.of();
    IteratorSetting is = new IteratorSetting(50, "regex", RegExFilter.class);
    RegExFilter.setRegexs(is, rowRegEx, cfRegEx, cqRegEx, valRegEx, false);
    RegExFilter iter = new RegExFilter();
    iter.init(source, is.getOptions(), null);
    iter.seek(range, es, false);
    runTest(iter, rowRegEx, cfRegEx, cqRegEx, valRegEx, expected);
  }

  private void runTest(RegExFilter scanner, String rowRegEx, String cfRegEx, String cqRegEx, String valRegEx, int expected) throws Exception {

    int counter = 0;

    while (scanner.hasTop()) {
      Key k = scanner.getTopKey();

      check(rowRegEx, k.getRow());
      check(cfRegEx, k.getColumnFamily());
      check(cqRegEx, k.getColumnQualifier());
      check(valRegEx, scanner.getTopValue());

      scanner.next();

      counter++;
    }

    if (counter != expected) {
      throw new Exception("scan did not return the expected number of entries " + counter + " " + expected);
    }
  }
}

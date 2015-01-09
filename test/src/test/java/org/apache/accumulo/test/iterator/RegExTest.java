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
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class RegExTest {

  Instance inst = new MockInstance();
  Connector conn;

  @Test
  public void runTest() throws Exception {
    conn = inst.getConnector("user", new PasswordToken("pass"));
    conn.tableOperations().create("ret");
    BatchWriter bw = conn.createBatchWriter("ret", new BatchWriterConfig());

    ArrayList<Character> chars = new ArrayList<Character>();
    for (char c = 'a'; c <= 'z'; c++)
      chars.add(c);

    for (char c = '0'; c <= '9'; c++)
      chars.add(c);

    // insert some data into accumulo
    for (Character rc : chars) {
      Mutation m = new Mutation(new Text("r" + rc));
      for (Character cfc : chars) {
        for (Character cqc : chars) {
          Value v = new Value(("v" + rc + cfc + cqc).getBytes());
          m.put(new Text("cf" + cfc), new Text("cq" + cqc), v);
        }
      }

      bw.addMutation(m);
    }

    bw.close();

    runTest1();
    runTest2();
    runTest3();
    runTest4();
    runTest5();
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

  private void runTest1() throws Exception {
    // try setting all regex
    Range range = new Range(new Text("rf"), true, new Text("rl"), true);
    runTest(range, "r[g-k]", "cf[1-5]", "cq[x-z]", "v[g-k][1-5][t-y]", 5 * 5 * (3 - 1));
  }

  private void runTest2() throws Exception {
    // try setting only a row regex
    Range range = new Range(new Text("rf"), true, new Text("rl"), true);
    runTest(range, "r[g-k]", null, null, null, 5 * 36 * 36);
  }

  private void runTest3() throws Exception {
    // try setting only a col fam regex
    Range range = new Range((Key) null, (Key) null);
    runTest(range, null, "cf[a-f]", null, null, 36 * 6 * 36);
  }

  private void runTest4() throws Exception {
    // try setting only a col qual regex
    Range range = new Range((Key) null, (Key) null);
    runTest(range, null, null, "cq[1-7]", null, 36 * 36 * 7);
  }

  private void runTest5() throws Exception {
    // try setting only a value regex
    Range range = new Range((Key) null, (Key) null);
    runTest(range, null, null, null, "v[a-c][d-f][g-i]", 3 * 3 * 3);
  }

  private void runTest(Range range, String rowRegEx, String cfRegEx, String cqRegEx, String valRegEx, int expected) throws Exception {

    Scanner s = conn.createScanner("ret", Authorizations.EMPTY);
    s.setRange(range);
    setRegexs(s, rowRegEx, cfRegEx, cqRegEx, valRegEx);
    runTest(s, rowRegEx, cfRegEx, cqRegEx, valRegEx, expected);

    BatchScanner bs = conn.createBatchScanner("ret", Authorizations.EMPTY, 1);
    bs.setRanges(Collections.singletonList(range));
    setRegexs(bs, rowRegEx, cfRegEx, cqRegEx, valRegEx);
    runTest(bs, rowRegEx, cfRegEx, cqRegEx, valRegEx, expected);
    bs.close();
  }

  private void setRegexs(ScannerBase scanner, String rowRegEx, String cfRegEx, String cqRegEx, String valRegEx) {
    IteratorSetting regex = new IteratorSetting(50, "regex", RegExFilter.class);
    if (rowRegEx != null)
      regex.addOption(RegExFilter.ROW_REGEX, rowRegEx);
    if (cfRegEx != null)
      regex.addOption(RegExFilter.COLF_REGEX, cfRegEx);
    if (cqRegEx != null)
      regex.addOption(RegExFilter.COLQ_REGEX, cqRegEx);
    if (valRegEx != null)
      regex.addOption(RegExFilter.VALUE_REGEX, valRegEx);
    scanner.addScanIterator(regex);
  }

  private void runTest(Iterable<Entry<Key,Value>> scanner, String rowRegEx, String cfRegEx, String cqRegEx, String valRegEx, int expected) throws Exception {

    int counter = 0;

    for (Entry<Key,Value> entry : scanner) {
      Key k = entry.getKey();

      check(rowRegEx, k.getRow());
      check(cfRegEx, k.getColumnFamily());
      check(cqRegEx, k.getColumnQualifier());
      check(valRegEx, entry.getValue());

      counter++;
    }

    if (counter != expected) {
      throw new Exception("scan did not return the expected number of entries " + counter + " " + expected);
    }
  }
}

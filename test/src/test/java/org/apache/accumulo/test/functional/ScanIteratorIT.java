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
package org.apache.accumulo.test.functional;

import static com.google.common.base.Charsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ScanIteratorIT extends AccumuloClusterIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 30;
  }

  @Test
  public void run() throws Exception {
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(tableName);

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());

    for (int i = 0; i < 1000; i++) {
      Mutation m = new Mutation(new Text(String.format("%06d", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value(Integer.toString(1000 - i).getBytes(UTF_8)));
      m.put(new Text("cf1"), new Text("cq2"), new Value(Integer.toString(i - 1000).getBytes(UTF_8)));

      bw.addMutation(m);
    }

    bw.close();

    Scanner scanner = c.createScanner(tableName, new Authorizations());

    setupIter(scanner);
    verify(scanner, 1, 999);

    BatchScanner bscanner = c.createBatchScanner(tableName, new Authorizations(), 3);
    bscanner.setRanges(Collections.singleton(new Range((Key) null, null)));

    setupIter(bscanner);
    verify(bscanner, 1, 999);

    ArrayList<Range> ranges = new ArrayList<Range>();
    ranges.add(new Range(new Text(String.format("%06d", 1))));
    ranges.add(new Range(new Text(String.format("%06d", 6)), new Text(String.format("%06d", 16))));
    ranges.add(new Range(new Text(String.format("%06d", 20))));
    ranges.add(new Range(new Text(String.format("%06d", 23))));
    ranges.add(new Range(new Text(String.format("%06d", 56)), new Text(String.format("%06d", 61))));
    ranges.add(new Range(new Text(String.format("%06d", 501)), new Text(String.format("%06d", 504))));
    ranges.add(new Range(new Text(String.format("%06d", 998)), new Text(String.format("%06d", 1000))));

    HashSet<Integer> got = new HashSet<Integer>();
    HashSet<Integer> expected = new HashSet<Integer>();
    for (int i : new int[] {1, 7, 9, 11, 13, 15, 23, 57, 59, 61, 501, 503, 999}) {
      expected.add(i);
    }

    bscanner.setRanges(ranges);

    for (Entry<Key,Value> entry : bscanner) {
      got.add(Integer.parseInt(entry.getKey().getRow().toString()));
    }

    System.out.println("got : " + got);

    if (!got.equals(expected)) {
      throw new Exception(got + " != " + expected);
    }

    bscanner.close();

  }

  private void verify(Iterable<Entry<Key,Value>> scanner, int start, int finish) throws Exception {

    int expected = start;
    for (Entry<Key,Value> entry : scanner) {
      if (Integer.parseInt(entry.getKey().getRow().toString()) != expected) {
        throw new Exception("Saw unexpexted " + entry.getKey().getRow() + " " + expected);
      }

      if (entry.getKey().getColumnQualifier().toString().equals("cq2")) {
        expected += 2;
      }
    }

    if (expected != finish + 2) {
      throw new Exception("Ended at " + expected + " not " + (finish + 2));
    }
  }

  private void setupIter(ScannerBase scanner) throws Exception {
    IteratorSetting dropMod = new IteratorSetting(50, "dropMod", "org.apache.accumulo.test.functional.DropModIter");
    dropMod.addOption("mod", "2");
    dropMod.addOption("drop", "0");
    scanner.addScanIterator(dropMod);
  }

}

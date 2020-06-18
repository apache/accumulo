/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class CreateAndUseIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private static SortedSet<Text> splits;

  @BeforeClass
  public static void createData() {
    splits = new TreeSet<>();

    for (int i = 1; i < 256; i++) {
      splits.add(new Text(String.format("%08x", i << 8)));
    }
  }

  @Test
  public void verifyDataIsPresent() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      Text cf = new Text("cf1");
      Text cq = new Text("cq1");

      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      client.tableOperations().addSplits(tableName, splits);
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = 1; i < 257; i++) {
          Mutation m = new Mutation(new Text(String.format("%08x", (i << 8) - 16)));
          m.put(cf, cq, new Value(Integer.toString(i)));
          bw.addMutation(m);
        }
      }

      try (Scanner scanner1 = client.createScanner(tableName, Authorizations.EMPTY)) {

        int ei = 1;

        for (Entry<Key,Value> entry : scanner1) {
          assertEquals(String.format("%08x", (ei << 8) - 16), entry.getKey().getRow().toString());
          assertEquals(Integer.toString(ei), entry.getValue().toString());

          ei++;
        }
        assertEquals("Did not see expected number of rows", 257, ei);
      }
    }
  }

  @Test
  public void createTableAndScan() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String table2 = getUniqueNames(1)[0];
      client.tableOperations().create(table2);
      client.tableOperations().addSplits(table2, splits);
      try (Scanner scanner2 = client.createScanner(table2, Authorizations.EMPTY)) {
        int count = 0;
        for (Entry<Key,Value> entry : scanner2) {
          if (entry != null)
            count++;
        }

        if (count != 0) {
          throw new Exception("Did not see expected number of entries, count = " + count);
        }
      }
    }
  }

  @Test
  public void createTableAndBatchScan() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      ArrayList<Range> ranges = new ArrayList<>();
      for (int i = 1; i < 257; i++) {
        ranges.add(new Range(new Text(String.format("%08x", (i << 8) - 16))));
      }

      String table3 = getUniqueNames(1)[0];
      client.tableOperations().create(table3);
      client.tableOperations().addSplits(table3, splits);
      try (BatchScanner bs = client.createBatchScanner(table3)) {
        bs.setRanges(ranges);
        Iterator<Entry<Key,Value>> iter = bs.iterator();
        int count = Iterators.size(iter);
        assertEquals("Did not expect to find any entries", 0, count);
      }
    }
  }
}

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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Merge;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MergeIT extends AccumuloClusterHarness {

  SortedSet<Text> splits(String[] points) {
    SortedSet<Text> result = new TreeSet<>();
    for (String point : points) {
      result.add(new Text(point));
    }
    return result;
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(8);
  }

  @Test
  public void merge() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      var ntc = new NewTableConfiguration().withSplits(splits("a b c d e f g h i j k".split(" ")));
      c.tableOperations().create(tableName, ntc);
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (String row : "a b c d e f g h i j k".split(" ")) {
          Mutation m = new Mutation(row);
          m.put("cf", "cq", "value");
          bw.addMutation(m);
        }
      }
      c.tableOperations().flush(tableName, null, null, true);
      c.tableOperations().merge(tableName, new Text("c1"), new Text("f1"));
      assertEquals(8, c.tableOperations().listSplits(tableName).size());
    }
  }

  @Test
  public void mergeSize() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration()
          .withSplits(splits("a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")));
      c.tableOperations().create(tableName, ntc);
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (String row : "c e f y".split(" ")) {
          Mutation m = new Mutation(row);
          m.put("cf", "cq", "mersydotesanddozeydotesanlittolamsiedives");
          bw.addMutation(m);
        }
      }
      c.tableOperations().flush(tableName, null, null, true);
      Merge merge = new Merge();
      merge.mergomatic(c, tableName, null, null, 100, false);
      assertArrayEquals("b c d e f x y".split(" "),
          toStrings(c.tableOperations().listSplits(tableName)));
      merge.mergomatic(c, tableName, null, null, 100, true);
      assertArrayEquals("c e f y".split(" "), toStrings(c.tableOperations().listSplits(tableName)));
    }
  }

  private String[] toStrings(Collection<Text> listSplits) {
    String[] result = new String[listSplits.size()];
    int i = 0;
    for (Text t : listSplits) {
      result[i++] = t.toString();
    }
    return result;
  }

  private String[] ns(String... strings) {
    return strings;
  }

  @Test
  public void mergeTest() throws Exception {
    int tc = 0;
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      runMergeTest(c, tableName + tc++, ns(), ns(), ns("l", "m", "n"), ns(null, "l"),
          ns(null, "n"));

      runMergeTest(c, tableName + tc++, ns("m"), ns(), ns("l", "m", "n"), ns(null, "l"),
          ns(null, "n"));
      runMergeTest(c, tableName + tc++, ns("m"), ns("m"), ns("l", "m", "n"), ns("m", "n"),
          ns(null, "z"));
      runMergeTest(c, tableName + tc++, ns("m"), ns("m"), ns("l", "m", "n"), ns(null, "b"),
          ns("l", "m"));

      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns(),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns(null, "s"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("m", "r"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("c", "m"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("r"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("n", "r"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns(null, "s"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("m", "n"), ns(null, "s"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "r"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns("q", "r"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m", "r"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("aa", "b"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m", "r"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("r", "s"), ns(null, "z"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m", "r"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns("l", "m"));
      runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m", "r"),
          ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("m", "n"), ns("q", "r"));
    }
  }

  private void runMergeTest(AccumuloClient c, String table, String[] splits,
      String[] expectedSplits, String[] inserts, String[] start, String[] end) throws Exception {
    int count = 0;

    for (String s : start) {
      for (String e : end) {
        runMergeTest(c, table + "_" + count++, splits, expectedSplits, inserts, s, e);
      }
    }
  }

  private void runMergeTest(AccumuloClient client, String table, String[] splits,
      String[] expectedSplits, String[] inserts, String start, String end) throws Exception {
    System.out.println(
        "Running merge test " + table + " " + Arrays.asList(splits) + " " + start + " " + end);

    SortedSet<Text> splitSet = splits(splits);

    NewTableConfiguration ntc = new NewTableConfiguration().setTimeType(TimeType.LOGICAL);
    if (!splitSet.isEmpty()) {
      ntc = ntc.withSplits(splitSet);
    }
    client.tableOperations().create(table, ntc);

    HashSet<String> expected = new HashSet<>();
    try (BatchWriter bw = client.createBatchWriter(table)) {
      for (String row : inserts) {
        Mutation m = new Mutation(row);
        m.put("cf", "cq", row);
        bw.addMutation(m);
        expected.add(row);
      }
    }

    client.tableOperations().merge(table, start == null ? null : new Text(start),
        end == null ? null : new Text(end));

    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {

      HashSet<String> observed = new HashSet<>();
      for (Entry<Key,Value> entry : scanner) {
        String row = entry.getKey().getRowData().toString();
        if (!observed.add(row)) {
          throw new Exception("Saw data twice " + table + " " + row);
        }
      }

      if (!observed.equals(expected)) {
        throw new Exception("data inconsistency " + table + " " + observed + " != " + expected);
      }

      HashSet<Text> currentSplits = new HashSet<>(client.tableOperations().listSplits(table));
      HashSet<Text> ess = new HashSet<>();
      for (String es : expectedSplits) {
        ess.add(new Text(es));
      }

      if (!currentSplits.equals(ess)) {
        throw new Exception("split inconsistency " + table + " " + currentSplits + " != " + ess);
      }
    }
  }
}

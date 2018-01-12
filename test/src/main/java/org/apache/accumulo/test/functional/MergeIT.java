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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Merge;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.util.TabletIterator;
import org.apache.accumulo.server.util.TabletIterator.TabletDeletedException;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MergeIT extends AccumuloClusterHarness {

  @Override
  public int defaultTimeoutSeconds() {
    return 8 * 60;
  }

  SortedSet<Text> splits(String[] points) {
    SortedSet<Text> result = new TreeSet<>();
    for (String point : points)
      result.add(new Text(point));
    return result;
  }

  @Test
  public void merge() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().addSplits(tableName, splits("a b c d e f g h i j k".split(" ")));
    BatchWriter bw = c.createBatchWriter(tableName, null);
    for (String row : "a b c d e f g h i j k".split(" ")) {
      Mutation m = new Mutation(row);
      m.put("cf", "cq", "value");
      bw.addMutation(m);
    }
    bw.close();
    c.tableOperations().flush(tableName, null, null, true);
    c.tableOperations().merge(tableName, new Text("c1"), new Text("f1"));
    assertEquals(8, c.tableOperations().listSplits(tableName).size());
  }

  @Test
  public void mergeSize() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().addSplits(tableName, splits("a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")));
    BatchWriter bw = c.createBatchWriter(tableName, null);
    for (String row : "c e f y".split(" ")) {
      Mutation m = new Mutation(row);
      m.put("cf", "cq", "mersydotesanddozeydotesanlittolamsiedives");
      bw.addMutation(m);
    }
    bw.close();
    c.tableOperations().flush(tableName, null, null, true);
    Merge merge = new Merge();
    merge.mergomatic(c, tableName, null, null, 100, false);
    assertArrayEquals("b c d e f x y".split(" "), toStrings(c.tableOperations().listSplits(tableName)));
    merge.mergomatic(c, tableName, null, null, 100, true);
    assertArrayEquals("c e f y".split(" "), toStrings(c.tableOperations().listSplits(tableName)));
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
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    runMergeTest(c, tableName + tc++, ns(), ns(), ns("l", "m", "n"), ns(null, "l"), ns(null, "n"));

    runMergeTest(c, tableName + tc++, ns("m"), ns(), ns("l", "m", "n"), ns(null, "l"), ns(null, "n"));
    runMergeTest(c, tableName + tc++, ns("m"), ns("m"), ns("l", "m", "n"), ns("m", "n"), ns(null, "z"));
    runMergeTest(c, tableName + tc++, ns("m"), ns("m"), ns("l", "m", "n"), ns(null, "b"), ns("l", "m"));

    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns(), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns(null, "s"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("c", "m"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("n", "r"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns(null, "s"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("m", "n"), ns(null, "s"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns("q", "r"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns(null, "a"), ns("aa", "b"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("r", "s"), ns(null, "z"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("b", "c"), ns("l", "m"));
    runMergeTest(c, tableName + tc++, ns("b", "m", "r"), ns("b", "m", "r"), ns("a", "b", "c", "l", "m", "n", "q", "r", "s"), ns("m", "n"), ns("q", "r"));

  }

  private void runMergeTest(Connector c, String table, String[] splits, String[] expectedSplits, String[] inserts, String[] start, String[] end)
      throws Exception {
    int count = 0;

    for (String s : start) {
      for (String e : end) {
        runMergeTest(c, table + "_" + count++, splits, expectedSplits, inserts, s, e);
      }
    }
  }

  private void runMergeTest(Connector conn, String table, String[] splits, String[] expectedSplits, String[] inserts, String start, String end)
      throws Exception {
    System.out.println("Running merge test " + table + " " + Arrays.asList(splits) + " " + start + " " + end);

    conn.tableOperations().create(table, new NewTableConfiguration().setTimeType(TimeType.LOGICAL));
    TreeSet<Text> splitSet = new TreeSet<>();
    for (String split : splits) {
      splitSet.add(new Text(split));
    }
    conn.tableOperations().addSplits(table, splitSet);

    BatchWriter bw = conn.createBatchWriter(table, null);
    HashSet<String> expected = new HashSet<>();
    for (String row : inserts) {
      Mutation m = new Mutation(row);
      m.put("cf", "cq", row);
      bw.addMutation(m);
      expected.add(row);
    }

    bw.close();

    conn.tableOperations().merge(table, start == null ? null : new Text(start), end == null ? null : new Text(end));

    try (Scanner scanner = conn.createScanner(table, Authorizations.EMPTY)) {

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

      HashSet<Text> currentSplits = new HashSet<>(conn.tableOperations().listSplits(table));
      HashSet<Text> ess = new HashSet<>();
      for (String es : expectedSplits) {
        ess.add(new Text(es));
      }

      if (!currentSplits.equals(ess)) {
        throw new Exception("split inconsistency " + table + " " + currentSplits + " != " + ess);
      }
    }
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static class TestTabletIterator extends TabletIterator {

    private final Connector conn;
    private final String metadataTableName;

    public TestTabletIterator(Connector conn, String metadataTableName) throws Exception {
      super(conn.createScanner(metadataTableName, Authorizations.EMPTY), MetadataSchema.TabletsSection.getRange(), true, true);
      this.conn = conn;
      this.metadataTableName = metadataTableName;
    }

    @Override
    protected void resetScanner() {
      try (Scanner ds = conn.createScanner(metadataTableName, Authorizations.EMPTY)) {

        Text tablet = new KeyExtent(Table.ID.of("0"), new Text("m"), null).getMetadataEntry();
        ds.setRange(new Range(tablet, true, tablet, true));

        Mutation m = new Mutation(tablet);

        BatchWriter bw = conn.createBatchWriter(metadataTableName, new BatchWriterConfig());
        for (Entry<Key,Value> entry : ds) {
          Key k = entry.getKey();
          m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), k.getTimestamp());
        }
        bw.addMutation(m);
        bw.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      super.resetScanner();
    }

  }

  // simulate a merge happening while iterating over tablets
  @Test
  public void testMerge() throws Exception {
    // create a fake metadata table
    String metadataTableName = getUniqueNames(1)[0];
    getConnector().tableOperations().create(metadataTableName);

    KeyExtent ke1 = new KeyExtent(Table.ID.of("0"), new Text("m"), null);
    Mutation mut1 = ke1.getPrevRowUpdateMutation();
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut1, new Value("/d1".getBytes()));

    KeyExtent ke2 = new KeyExtent(Table.ID.of("0"), null, null);
    Mutation mut2 = ke2.getPrevRowUpdateMutation();
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut2, new Value("/d2".getBytes()));

    BatchWriter bw1 = getConnector().createBatchWriter(metadataTableName, new BatchWriterConfig());
    bw1.addMutation(mut1);
    bw1.addMutation(mut2);
    bw1.close();

    TestTabletIterator tabIter = new TestTabletIterator(getConnector(), metadataTableName);

    exception.expect(TabletDeletedException.class);
    while (tabIter.hasNext()) {
      tabIter.next();
    }
  }
}

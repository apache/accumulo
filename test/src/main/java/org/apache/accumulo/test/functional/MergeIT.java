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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.Merge;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.fs.Path;
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

  // Test that merge handles metadata from compactions
  @Test
  public void testCompactionMetadata() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      var split = new Text("m");
      c.tableOperations().addSplits(tableName, new TreeSet<>(List.of(split)));

      TableId tableId = getServerContext().getTableId(tableName);

      // add metadata from compactions to tablets prior to merge
      try (var tabletsMutator = getServerContext().getAmple().mutateTablets()) {
        for (var extent : List.of(new KeyExtent(tableId, split, null),
            new KeyExtent(tableId, null, split))) {
          var tablet = tabletsMutator.mutateTablet(extent);
          ExternalCompactionId ecid = ExternalCompactionId.generate(UUID.randomUUID());

          TabletFile tmpFile = new TabletFile(new Path("file:///accumulo/tables/t-0/b-0/c1.rf"));
          CompactionExecutorId ceid = CompactionExecutorIdImpl.externalId("G1");
          Set<StoredTabletFile> jobFiles =
              Set.of(new StoredTabletFile("file:///accumulo/tables/t-0/b-0/b2.rf"));
          ExternalCompactionMetadata ecMeta = new ExternalCompactionMetadata(jobFiles, jobFiles,
              tmpFile, "localhost:4444", CompactionKind.SYSTEM, (short) 2, ceid, false, false, 44L);
          tablet.putExternalCompaction(ecid, ecMeta);
          tablet.mutate();
        }
      }

      // ensure data is in metadata table as expected
      try (var tablets = getServerContext().getAmple().readTablets().forTable(tableId).build()) {
        for (var tablet : tablets) {
          assertFalse(tablet.getExternalCompactions().isEmpty());
        }
      }

      c.tableOperations().merge(tableName, null, null);

      // ensure merge operation remove compaction entries
      try (var tablets = getServerContext().getAmple().readTablets().forTable(tableId).build()) {
        for (var tablet : tablets) {
          assertTrue(tablet.getExternalCompactions().isEmpty());
        }
      }
    }
  }

  @Test
  public void testMetadataTableMergesSuccessfully() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      c.tableOperations().addSplits(MetadataTable.NAME,
          new TreeSet<>(List.of(new Text("~del"), new Text("~sserv"), new Text("~err"))));

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      TableId tableId = getServerContext().getTableId(tableName);
      try (BatchWriter writer = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", "NonNull Value");
        writer.addMutation(m);
      }
      c.tableOperations().flush(tableName, null, null, true);

      // Grab the current number of tablets
      long expectedTablets = getServerContext().getAmple().readTablets().forTable(MetadataTable.ID)
          .build().stream().count();
      // We have added two adjacent splits, so the number of tablets should now be increased by 2.
      c.tableOperations().addSplits(MetadataTable.NAME, new TreeSet<>(
          List.of(new Text(tableId.canonical()), new Text(tableId.canonical() + "<"))));

      try (var tablets =
          getServerContext().getAmple().readTablets().forTable(MetadataTable.ID).build()) {
        assertEquals(expectedTablets + 2, tablets.stream().count());
      }

      List<String> args = new ArrayList<>(List.of("-t", MetadataTable.NAME, "-e", "~"));
      getClientProps().stringPropertyNames().forEach(keyProp -> {
        args.add("-o");
        args.add(keyProp + "=" + getClientProps().getProperty(keyProp));
      });
      Merge.main(args.toArray(String[]::new));
      try (var tablets =
          getServerContext().getAmple().readTablets().forTable(MetadataTable.ID).build()) {
        assertEquals(expectedTablets, tablets.stream().count());
      }
    }
  }

  @Test
  public void testMetadataTableSingleByteLimit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      TableId tableId = getServerContext().getTableId(tableName);
      try (BatchWriter writer = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", "NonNull Value");
        writer.addMutation(m);
      }
      c.tableOperations().flush(tableName, null, null, true);

      // Grab the current number of tablets
      long expectedTablets = getServerContext().getAmple().readTablets().forTable(MetadataTable.ID)
          .build().stream().count();
      // We have added two adjacent splits, so the number of tablets should now be increased by 2.
      c.tableOperations().addSplits(MetadataTable.NAME, new TreeSet<>(
          List.of(new Text(tableId.canonical()), new Text(tableId.canonical() + "<"))));

      try (var tablets =
          getServerContext().getAmple().readTablets().forTable(MetadataTable.ID).build()) {
        assertEquals(expectedTablets + 2, tablets.stream().count());
      }

      List<String> args = new ArrayList<>(List.of("-t", MetadataTable.NAME, "-e", "~", "-s", "1"));
      getClientProps().stringPropertyNames().forEach(keyProp -> {
        args.add("-o");
        args.add(keyProp + "=" + getClientProps().getProperty(keyProp));
      });
      Merge.main(args.toArray(String[]::new));
      try (var tablets =
          getServerContext().getAmple().readTablets().forTable(MetadataTable.ID).build()) {
        assertEquals(expectedTablets + 2, tablets.stream().count());
      }
      // Add splits for zero length entries in the ~del markers
      c.tableOperations().addSplits(MetadataTable.NAME, new TreeSet<>(
          List.of(new Text("~del0"), new Text("~del1"), new Text("~del2"), new Text("~del3"))));
      try (var tablets =
          getServerContext().getAmple().readTablets().forTable(MetadataTable.ID).build()) {
        assertEquals(expectedTablets + 6, tablets.stream().count());
      }
      // Perform a merge that will collapse the empty tablets into a single "~del" tablet.
      List<String> newArgs =
          new ArrayList<>(List.of("-t", MetadataTable.NAME, "-b", "~", "-e", "~del9", "-s", "1"));
      getClientProps().stringPropertyNames().forEach(keyProp -> {
        newArgs.add("-o");
        newArgs.add(keyProp + "=" + getClientProps().getProperty(keyProp));
      });
      Merge.main(newArgs.toArray(String[]::new));
      try (var tablets =
          getServerContext().getAmple().readTablets().forTable(MetadataTable.ID).build()) {
        for (var tablet : tablets) {
          System.out.println("Row Extent" + tablet.getExtent());
        }
        // ~del3 is now an empty tablet and cannot be merged with the ~ tablet that is over 1 byte
        // in size
        assertEquals(expectedTablets + 3, tablets.stream().count());
      }
    }
  }

  @Test
  public void testRootTableDoesNotMerge() {
    List<String> args = new ArrayList<>(List.of("-t", RootTable.NAME));
    getClientProps().stringPropertyNames().forEach(keyProp -> {
      args.add("-o");
      args.add(keyProp + "=" + getClientProps().getProperty(keyProp));
    });
    Exception e =
        assertThrows(Merge.MergeException.class, () -> Merge.main(args.toArray(String[]::new)));
    assertInstanceOf(IllegalArgumentException.class, e.getCause());
    assertTrue(e.getMessage().contains("Cannot merge the root table"));
    try (var tablets = getServerContext().getAmple().readTablets().forTable(RootTable.ID).build()) {
      assertEquals(1, tablets.stream().count());
    }
  }
}

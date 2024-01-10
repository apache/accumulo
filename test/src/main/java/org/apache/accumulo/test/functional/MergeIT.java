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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.test.util.FileMetadataUtil.printAndVerifyFileMetadata;
import static org.apache.accumulo.test.util.FileMetadataUtil.verifyMergedMarkerCleared;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.TabletHostingGoalUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Merge;
import org.apache.accumulo.core.util.compaction.CompactorGroupIdImpl;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class MergeIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(MergeIT.class);

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
  public void tooManyFilesMergeTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName,
          new NewTableConfiguration().setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(),
              "20000", Property.TABLE_MERGE_FILE_MAX.getKey(), "12345")));

      c.tableOperations().addSplits(tableName,
          IntStream.range(1, 10001).mapToObj(i -> String.format("%06d", i)).map(Text::new)
              .collect(Collectors.toCollection(TreeSet::new)));
      c.tableOperations().addSplits(tableName,
          IntStream.range(10001, 20001).mapToObj(i -> String.format("%06d", i)).map(Text::new)
              .collect(Collectors.toCollection(TreeSet::new)));

      // add two bogus files to each tablet, creating 40K file entries
      c.tableOperations().offline(tableName, true);
      try (
          var tablets = getServerContext().getAmple().readTablets()
              .forTable(getServerContext().getTableId(tableName)).build();
          var mutator = getServerContext().getAmple().mutateTablets()) {
        int fc = 0;
        for (var tabletMeta : tablets) {
          StoredTabletFile f1 = StoredTabletFile.of(new Path(
              "file:///accumulo/tables/1/" + tabletMeta.getDirName() + "/F" + fc++ + ".rf"));
          StoredTabletFile f2 = StoredTabletFile.of(new Path(
              "file:///accumulo/tables/1/" + tabletMeta.getDirName() + "/F" + fc++ + ".rf"));
          DataFileValue dfv1 = new DataFileValue(4200, 42);
          DataFileValue dfv2 = new DataFileValue(4200, 42);
          mutator.mutateTablet(tabletMeta.getExtent()).putFile(f1, dfv1).putFile(f2, dfv2).mutate();
        }
      }
      c.tableOperations().online(tableName, true);

      // should fail to merge because there are too many files in the merge range
      var exception = assertThrows(AccumuloException.class,
          () -> c.tableOperations().merge(tableName, null, null));
      // message should contain the observed number of files
      assertTrue(exception.getMessage().contains("40002"));
      // message should contain the max files limit it saw
      assertTrue(exception.getMessage().contains("12345"));

      assertEquals(20000, c.tableOperations().listSplits(tableName).size());

      // attempt to merge smaller ranges with less files, should work.. want to make sure the
      // aborted merge did not leave the table in a bad state
      Text prev = null;
      for (int i = 1000; i <= 20000; i += 1000) {
        Text end = new Text(String.format("%06d", i));
        c.tableOperations().merge(tableName, prev, end);
        prev = end;
      }

      assertEquals(20, c.tableOperations().listSplits(tableName).size());
      try (var tablets = getServerContext().getAmple().readTablets()
          .forTable(getServerContext().getTableId(tableName)).build()) {
        assertEquals(40002,
            tablets.stream().mapToInt(tabletMetadata -> tabletMetadata.getFiles().size()).sum());
      }
    }
  }

  @Test
  public void merge() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      var ntc = new NewTableConfiguration().withSplits(splits("a b c d e f g h i j k".split(" ")));
      createTableAndDisableCompactions(c, tableName, ntc);
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (String row : "a b c d e f g h i j k".split(" ")) {
          Mutation m = new Mutation(row);
          m.put("cf", "cq", "value");
          bw.addMutation(m);
        }
      }
      c.tableOperations().setTabletHostingGoal(tableName, new Range("d", "e"),
          TabletHostingGoal.ALWAYS);
      c.tableOperations().setTabletHostingGoal(tableName, new Range("e", "f"),
          TabletHostingGoal.NEVER);
      c.tableOperations().flush(tableName, null, null, true);
      c.tableOperations().merge(tableName, new Text("c1"), new Text("f1"));
      assertEquals(8, c.tableOperations().listSplits(tableName).size());
      // Verify that the MERGED marker was cleared
      verifyMergedMarkerCleared(getServerContext(),
          TableId.of(c.tableOperations().tableIdMap().get(tableName)));
      try (Scanner s = c.createScanner(MetadataTable.NAME)) {
        String tid = c.tableOperations().tableIdMap().get(tableName);
        s.setRange(new Range(tid + ";g"));
        TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
        HostingColumnFamily.GOAL_COLUMN.fetch(s);
        assertEquals(2, Iterables.size(s));
        for (Entry<Key,Value> rows : s) {
          if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(rows.getKey())) {
            assertEquals("c", TabletColumnFamily.decodePrevEndRow(rows.getValue()).toString());
          } else if (HostingColumnFamily.GOAL_COLUMN.hasColumns(rows.getKey())) {
            assertEquals(TabletHostingGoal.ALWAYS,
                TabletHostingGoalUtil.fromValue(rows.getValue()));
          } else {
            fail("Unknown column");
          }
        }
      }
    }
  }

  @Test
  public void mergeSize() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration()
          .withSplits(splits("a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")));
      createTableAndDisableCompactions(c, tableName, ntc);
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

  @Test
  public void noChopMergeTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      createTableAndDisableCompactions(c, tableName, new NewTableConfiguration());
      final TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      // First write 1000 rows to a file in the default tablet
      ingest(c, 1000, 1, tableName);
      c.tableOperations().flush(tableName, null, null, true);

      log.debug("Metadata after Ingest");
      printAndVerifyFileMetadata(getServerContext(), tableId, 1);

      // Add splits so we end up with 4 tablets
      final SortedSet<Text> splits = new TreeSet<>();
      for (int i = 250; i <= 750; i += 250) {
        splits.add(new Text("row_" + String.format("%010d", i)));
      }
      c.tableOperations().addSplits(tableName, splits);

      log.debug("Metadata after Split");
      verify(c, 1000, 1, tableName);
      printAndVerifyFileMetadata(getServerContext(), tableId, 4);

      // Go through and delete two blocks of rows, 101 - 200
      // and also 301 - 400 so we can test that the data doesn't come
      // back on merge
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        byte[] COL_PREFIX = "col_".getBytes(UTF_8);
        Text colq = new Text(FastFormat.toZeroPaddedString(0, 7, 10, COL_PREFIX));

        for (int i = 101; i <= 200; i++) {
          Mutation m = new Mutation(new Text("row_" + String.format("%010d", i)));
          m.putDelete(new Text("colf"), colq);
          bw.addMutation(m);
        }
        for (int i = 301; i <= 400; i++) {
          Mutation m = new Mutation(new Text("row_" + String.format("%010d", i)));
          m.putDelete(new Text("colf"), colq);
          bw.addMutation(m);
        }
      }

      c.tableOperations().flush(tableName, null, null, true);

      // compact the first 2 tablets so the new files with the deletes are gone
      // so we can test that the data does not come back when the 3rd tablet is
      // merged back with the other tablets as it still contains the original file
      c.tableOperations().compact(tableName, new CompactionConfig().setStartRow(null)
          .setEndRow(List.copyOf(splits).get(1)).setWait(true));
      log.debug("Metadata after deleting rows 101 - 200 and 301 - 400");
      printAndVerifyFileMetadata(getServerContext(), tableId, 4);

      // Merge and print results
      c.tableOperations().merge(tableName, null, null);
      log.debug("Metadata after Merge");
      printAndVerifyFileMetadata(getServerContext(), tableId, 4);

      // Verify that the deleted rows can't be read after merge
      verify(c, 100, 1, tableName);
      verifyNoRows(c, 100, 101, tableName);
      verify(c, 100, 201, tableName);
      verifyNoRows(c, 100, 301, tableName);
      verify(c, 600, 401, tableName);

      // Verify that the MERGED marker was cleared
      verifyMergedMarkerCleared(getServerContext(), tableId);
    }
  }

  @Test
  public void noChopMergeDeleteAcrossTablets() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      createTableAndDisableCompactions(c, tableName, new NewTableConfiguration());
      // disable compactions
      c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "9999");
      final TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      // First write 1000 rows to a file in the default tablet
      ingest(c, 1000, 1, tableName);
      c.tableOperations().flush(tableName, null, null, true);

      log.debug("Metadata after Ingest");
      printAndVerifyFileMetadata(getServerContext(), tableId, 1);

      // Add splits so we end up with 10 tablets
      final SortedSet<Text> splits = new TreeSet<>();
      for (int i = 100; i <= 900; i += 100) {
        splits.add(new Text("row_" + String.format("%010d", i)));
      }
      c.tableOperations().addSplits(tableName, splits);

      log.debug("Metadata after Split");
      verify(c, 1000, 1, tableName);
      printAndVerifyFileMetadata(getServerContext(), tableId, 10);

      // Go through and delete three blocks of rows
      // 151 - 250, 451 - 550, 751 - 850
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        byte[] COL_PREFIX = "col_".getBytes(UTF_8);
        Text colq = new Text(FastFormat.toZeroPaddedString(0, 7, 10, COL_PREFIX));

        for (int j = 0; j <= 2; j++) {
          for (int i = 151; i <= 250; i++) {
            Mutation m = new Mutation(new Text("row_" + String.format("%010d", i + (j * 300))));
            m.putDelete(new Text("colf"), colq);
            bw.addMutation(m);
          }
        }
      }

      c.tableOperations().flush(tableName, null, null, true);

      log.debug("Metadata after deleting rows 151 - 250, 451 - 550, 751 - 850");
      // compact some of the tablets with deletes so we can test that the data does not come back
      c.tableOperations().compact(tableName,
          new CompactionConfig().setStartRow(new Text("row_" + String.format("%010d", 150)))
              .setEndRow(new Text("row_" + String.format("%010d", 250))).setWait(true));
      c.tableOperations().compact(tableName,
          new CompactionConfig().setStartRow(new Text("row_" + String.format("%010d", 750)))
              .setEndRow(new Text("row_" + String.format("%010d", 850))).setWait(true));
      // Should be 16 files (10 for the original splits plus 2 extra files per deletion range across
      // tablets)
      printAndVerifyFileMetadata(getServerContext(), tableId, 12);

      c.tableOperations().merge(tableName, null, null);
      log.debug("Metadata after Merge");
      printAndVerifyFileMetadata(getServerContext(), tableId, 12);
      // Verify that the MERGED marker was cleared
      verifyMergedMarkerCleared(getServerContext(), tableId);

      // Verify that the deleted rows can't be read after merge
      verify(c, 150, 1, tableName);
      verifyNoRows(c, 100, 151, tableName);
      verify(c, 200, 251, tableName);
      verifyNoRows(c, 100, 451, tableName);
      verify(c, 200, 551, tableName);
      verifyNoRows(c, 100, 751, tableName);
      verify(c, 150, 851, tableName);

      // Compact and verify we clean up all the files and only 1 left
      // Verify only 700 entries
      c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      log.debug("Metadata after compact");
      // Should just be 1 file with infinite range
      Map<StoredTabletFile,DataFileValue> files =
          printAndVerifyFileMetadata(getServerContext(), tableId, 1);
      assertEquals(new Range(), files.keySet().stream().findFirst().orElseThrow().getRange());
      assertEquals(700, files.values().stream().findFirst().orElseThrow().getNumEntries());
    }
  }

  // Multiple splits/deletes/merges to show ranges work and carry forward
  // Testing that we can split -> delete, merge, split -> delete, merge
  // with deletions across boundaries
  @Test
  public void noChopMergeDeleteAcrossTabletsMultiple() throws Exception {
    // Run initial test to populate table and merge which adds ranges to files
    noChopMergeDeleteAcrossTablets();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      final TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      log.debug("Metadata after initial test run");
      printAndVerifyFileMetadata(getServerContext(), tableId, -1);

      // Add splits so we end up with 10 tablets
      final SortedSet<Text> splits = new TreeSet<>();
      for (int i = 100; i <= 900; i += 100) {
        splits.add(new Text("row_" + String.format("%010d", i)));
      }
      c.tableOperations().addSplits(tableName, splits);

      log.debug("Metadata after Split for second time");
      // Verify that the deleted rows can't be read after merge
      verify(c, 150, 1, tableName);
      verifyNoRows(c, 100, 151, tableName);
      verify(c, 200, 251, tableName);
      verifyNoRows(c, 100, 451, tableName);
      verify(c, 200, 551, tableName);
      verifyNoRows(c, 100, 751, tableName);
      verify(c, 150, 851, tableName);
      printAndVerifyFileMetadata(getServerContext(), tableId, -1);

      c.tableOperations().flush(tableName, null, null, true);

      // Go through and also delete 651 - 700
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        byte[] COL_PREFIX = "col_".getBytes(UTF_8);
        Text colq = new Text(FastFormat.toZeroPaddedString(0, 7, 10, COL_PREFIX));

        for (int i = 651; i <= 700; i++) {
          Mutation m = new Mutation(new Text("row_" + String.format("%010d", i)));
          m.putDelete(new Text("colf"), colq);
          bw.addMutation(m);
        }
      }

      c.tableOperations().flush(tableName, null, null, true);

      log.debug("Metadata after deleting rows 151 - 250, 451 - 550, 651 - 700, 751 - 850");
      // compact some of the tablets with deletes so we can test that the data does not come back
      c.tableOperations().compact(tableName,
          new CompactionConfig().setStartRow(new Text("row_" + String.format("%010d", 150)))
              .setEndRow(new Text("row_" + String.format("%010d", 700))).setWait(true));

      // Re-merge a second time after deleting more rows
      c.tableOperations().merge(tableName, null, null);
      log.debug("Metadata after second Merge");
      printAndVerifyFileMetadata(getServerContext(), tableId, -1);
      // Verify that the MERGED marker was cleared
      verifyMergedMarkerCleared(getServerContext(), tableId);

      // Verify that the deleted rows can't be read after merge
      verify(c, 150, 1, tableName);
      verifyNoRows(c, 100, 151, tableName);
      verify(c, 200, 251, tableName);
      verifyNoRows(c, 100, 451, tableName);
      verify(c, 100, 551, tableName);
      verifyNoRows(c, 50, 651, tableName);
      verify(c, 50, 701, tableName);
      verifyNoRows(c, 100, 751, tableName);
      verify(c, 150, 851, tableName);
    }
  }

  // Tests that after we merge and fence files, we can split and then
  // merge a second time the same table which shows splits/merges work
  // for files that already have ranges
  @Test
  public void noChopMergeTestMultipleMerges() throws Exception {
    // Do initial merge which will fence off files
    noChopMergeTest();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      final TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      log.debug("Metadata after initial no chop merge test");
      printAndVerifyFileMetadata(getServerContext(), tableId, 4);

      // Add splits so we end up with 4 tablets
      final SortedSet<Text> splits = new TreeSet<>();
      for (int i = 250; i <= 750; i += 250) {
        splits.add(new Text("row_" + String.format("%010d", i)));
      }
      c.tableOperations().addSplits(tableName, splits);

      log.debug("Metadata after Split");
      // Verify after splitting for the second time
      verify(c, 100, 1, tableName);
      verifyNoRows(c, 100, 101, tableName);
      verify(c, 100, 201, tableName);
      verifyNoRows(c, 100, 301, tableName);
      verify(c, 600, 401, tableName);
      printAndVerifyFileMetadata(getServerContext(), tableId, -1);

      // Re-Merge and print results. This tests merging with files
      // that already have a range
      c.tableOperations().merge(tableName, null, null);
      log.debug("Metadata after Merge");
      printAndVerifyFileMetadata(getServerContext(), tableId, -1);
      // Verify that the MERGED marker was cleared
      verifyMergedMarkerCleared(getServerContext(), tableId);

      // Verify that the deleted rows can't be read after merge
      verify(c, 100, 1, tableName);
      verifyNoRows(c, 100, 101, tableName);
      verify(c, 100, 201, tableName);
      verifyNoRows(c, 100, 301, tableName);
      verify(c, 600, 401, tableName);
    }
  }

  private static void createTableAndDisableCompactions(AccumuloClient c, String tableName,
      NewTableConfiguration ntc) throws Exception {
    // disable compactions
    ntc.setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(), "9999"));
    c.tableOperations().create(tableName, ntc);
  }

  public static void ingest(AccumuloClient accumuloClient, int rows, int offset, String tableName)
      throws Exception {
    IngestParams params = new IngestParams(accumuloClient.properties(), tableName, rows);
    params.cols = 1;
    params.dataSize = 10;
    params.startRow = offset;
    params.columnFamily = "colf";
    params.createTable = true;
    TestIngest.ingest(accumuloClient, params);
  }

  private static void verify(AccumuloClient accumuloClient, int rows, int offset, String tableName)
      throws Exception {
    VerifyParams params = new VerifyParams(accumuloClient.properties(), tableName, rows);
    params.rows = rows;
    params.dataSize = 10;
    params.startRow = offset;
    params.columnFamily = "colf";
    params.cols = 1;
    VerifyIngest.verifyIngest(accumuloClient, params);
  }

  private static void verifyNoRows(AccumuloClient accumuloClient, int rows, int offset,
      String tableName) throws Exception {
    try {
      verify(accumuloClient, rows, offset, tableName);
      fail("Should have failed");
    } catch (AccumuloException e) {
      assertTrue(e.getMessage().contains("Did not read expected number of rows. Saw 0"));
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
    log.debug(
        "Running merge test " + table + " " + Arrays.asList(splits) + " " + start + " " + end);

    SortedSet<Text> splitSet = splits(splits);

    NewTableConfiguration ntc = new NewTableConfiguration().setTimeType(TimeType.LOGICAL);
    if (!splitSet.isEmpty()) {
      ntc = ntc.withSplits(splitSet);
    }
    createTableAndDisableCompactions(client, table, ntc);

    HashSet<String> expected = new HashSet<>();
    try (BatchWriter bw = client.createBatchWriter(table)) {
      for (String row : inserts) {
        Mutation m = new Mutation(row);
        m.put("cf", "cq", row);
        bw.addMutation(m);
        expected.add(row);
      }
    }

    log.debug("Before Merge");
    client.tableOperations().flush(table, null, null, true);
    TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(table));
    printAndVerifyFileMetadata(getServerContext(), tableId);

    client.tableOperations().merge(table, start == null ? null : new Text(start),
        end == null ? null : new Text(end));

    client.tableOperations().flush(table, null, null, true);
    log.debug("After Merge");
    printAndVerifyFileMetadata(getServerContext(), tableId);
    // Verify that the MERGED marker was cleared
    verifyMergedMarkerCleared(getServerContext(), tableId);

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

          ReferencedTabletFile tmpFile =
              ReferencedTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/c1.rf"));
          CompactorGroupId ceid = CompactorGroupIdImpl.groupId("G1");
          Set<StoredTabletFile> jobFiles =
              Set.of(StoredTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/b2.rf")));
          CompactionMetadata ecMeta = new CompactionMetadata(jobFiles, tmpFile, "localhost:4444",
              CompactionKind.SYSTEM, (short) 2, ceid, false, 44L);
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
}

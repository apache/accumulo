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

import static org.apache.accumulo.test.util.FileMetadataUtil.printAndVerifyFileMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteRowsIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(DeleteRowsIT.class);

  private static final int ROWS_PER_TABLET = 10;
  private static final List<String> LETTERS = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i",
      "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
  static final TreeSet<Text> SPLITS =
      LETTERS.stream().map(Text::new).collect(Collectors.toCollection(TreeSet::new));

  static final List<String> ROWS = new ArrayList<>(LETTERS);
  // put data on first and last tablet
  static {
    ROWS.add("A");
    ROWS.add("{");
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Test
  public void testDeleteAllRows() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = this.getUniqueNames(20);
      for (String tableName : tableNames) {
        c.tableOperations().create(tableName);
        c.tableOperations().deleteRows(tableName, null, null);
        try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
          assertTrue(scanner.stream().findAny().isEmpty());
        }
      }
    }
  }

  @Test
  public void testManyRows() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      // Delete ranges of rows, and verify the tablets are removed.
      int i = 0;
      // Eliminate whole tablets
      String tableName = getUniqueNames(1)[0];
      testSplit(c, tableName + i++, "f", "h", "abcdefijklmnopqrstuvwxyz", 260);
      // Eliminate whole tablets, partial first tablet
      testSplit(c, tableName + i++, "f1", "h", "abcdefgijklmnopqrstuvwxyz", 262);
      // Eliminate whole tablets, partial last tablet
      testSplit(c, tableName + i++, "f", "h1", "abcdefijklmnopqrstuvwxyz", 258);
      // Eliminate whole tablets, partial first and last tablet
      testSplit(c, tableName + i++, "f1", "h1", "abcdefgijklmnopqrstuvwxyz", 260);
      // Eliminate one tablet
      testSplit(c, tableName + i++, "f", "g", "abcdefhijklmnopqrstuvwxyz", 270);
      // Eliminate first tablet
      testSplit(c, tableName + i++, null, "a", "bcdefghijklmnopqrstuvwxyz", 270);
      // Eliminate last tablet
      testSplit(c, tableName + i++, "z", null, "abcdefghijklmnopqrstuvwxyz", 260);
      // Eliminate partial tablet, matches start split
      testSplit(c, tableName + i++, "f", "f1", "abcdefghijklmnopqrstuvwxyz", 278);
      // Eliminate partial tablet, matches end split
      testSplit(c, tableName + i++, "f1", "g", "abcdefghijklmnopqrstuvwxyz", 272);
      // Eliminate tablets starting at -inf
      testSplit(c, tableName + i++, null, "h", "ijklmnopqrstuvwxyz", 200);
      // Eliminate tablets ending at +inf
      testSplit(c, tableName + i++, "t", null, "abcdefghijklmnopqrst", 200);
      // Eliminate some rows inside one tablet
      testSplit(c, tableName + i++, "t0", "t2", "abcdefghijklmnopqrstuvwxyz", 278);
      // Eliminate some rows in the first tablet
      testSplit(c, tableName + i++, null, "A1", "abcdefghijklmnopqrstuvwxyz", 278);
      // Eliminate some rows in the last tablet
      testSplit(c, tableName + i++, "{1", null, "abcdefghijklmnopqrstuvwxyz", 272);
      // Delete everything
      testSplit(c, tableName + i++, null, null, "", 0);
    }
  }

  // Test that deletion works on tablets that have files that have already been fenced
  // The fenced files are created by doing merges first
  @Test
  public void testManyRowsAlreadyFenced() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      // Delete ranges of rows, and verify the tablets are removed.
      int i = 0;
      // Eliminate whole tablets
      String tableName = getUniqueNames(1)[0];
      testSplit(c, tableName + i++, "f", "h", "abcdefijklmnopqrstuvwxyz", 260, "f", "h");
      // Eliminate whole tablets, partial first tablet
      testSplit(c, tableName + i++, "f1", "h", "abcdefgijklmnopqrstuvwxyz", 262, "f", "h");
      // Eliminate whole tablets, partial last tablet
      testSplit(c, tableName + i++, "f", "h1", "abcdefijklmnopqrstuvwxyz", 258, "f", "h");
      // Eliminate whole tablets, partial first and last tablet
      testSplit(c, tableName + i++, "f1", "h1", "abcdefgijklmnopqrstuvwxyz", 260, "f", "h");
      // Eliminate one tablet
      testSplit(c, tableName + i++, "f", "g", "abcdefhijklmnopqrstuvwxyz", 270, "f", "g");
      // Eliminate first tablet
      testSplit(c, tableName + i++, null, "a", "bcdefghijklmnopqrstuvwxyz", 270, "a", "a");
      // Eliminate last tablet
      testSplit(c, tableName + i++, "z", null, "abcdefghijklmnopqrstuvwxyz", 260, "z", "z");
      // Eliminate partial tablet, matches start split
      testSplit(c, tableName + i++, "f", "f1", "abcdefghijklmnopqrstuvwxyz", 278, "f", "f");
      // Eliminate partial tablet, matches end split
      testSplit(c, tableName + i++, "f1", "g", "abcdefghijklmnopqrstuvwxyz", 272, "f", "g");
      // Eliminate tablets starting at -inf
      testSplit(c, tableName + i++, null, "h", "ijklmnopqrstuvwxyz", 200, "a", "h");
      // Eliminate tablets ending at +inf
      testSplit(c, tableName + i++, "t", null, "abcdefghijklmnopqrst", 200, "t", "z");
      // Eliminate some rows inside one tablet
      testSplit(c, tableName + i++, "t0", "t2", "abcdefghijklmnopqrstuvwxyz", 278, "t", "t");
      // Eliminate some rows in the first tablet
      testSplit(c, tableName + i++, null, "A1", "abcdefghijklmnopqrstuvwxyz", 278, "a", "a");
      // Eliminate some rows in the last tablet
      testSplit(c, tableName + i++, "{1", null, "abcdefghijklmnopqrstuvwxyz", 272, "z", "z");
      // Delete everything
      testSplit(c, tableName + i++, null, null, "", 0, "a", "z");
    }
  }

  private void testSplit(AccumuloClient c, String table, String start, String end, String result,
      int entries) throws Exception {
    testSplit(c, table, start, end, result, entries, null, null);
  }

  private void testSplit(AccumuloClient c, String table, String start, String end, String result,
      int entries, String mergeStart, String mergeEnd) throws Exception {
    // Put a bunch of rows on each tablet
    c.tableOperations().create(table);
    try (BatchWriter bw = c.createBatchWriter(table)) {
      for (String row : ROWS) {
        for (int j = 0; j < ROWS_PER_TABLET; j++) {
          Mutation m = new Mutation(row + j);
          m.put("cf", "cq", "value");
          bw.addMutation(m);
        }
      }
      bw.flush();
    }

    final TableId tableId = TableId.of(c.tableOperations().tableIdMap().get(table));
    // Split the table

    // If a merge range is defined then merge the tablets given in the range after
    // The purpose of the merge is to generate file metadata that contains ranges
    // so this will test deletings on existing ranged files
    if (mergeStart != null) {
      SortedSet<Text> splits = new TreeSet<>(SPLITS);
      // Generate 2 split points for each existing split and add
      SortedSet<Text> mergeSplits =
          SPLITS.subSet(new Text(mergeStart), true, new Text(mergeEnd), true);
      mergeSplits.forEach(split -> splits.add(new Text(split.toString() + (ROWS_PER_TABLET / 2))));

      log.debug("After splits");
      c.tableOperations().addSplits(table, splits);
      printAndVerifyFileMetadata(getServerContext(), tableId);

      // Merge back the extra splits to a single tablet per letter to generate 2 files per tablet
      // that have a range
      mergeSplits.forEach(split -> {
        try {
          c.tableOperations().merge(table, split, new Key(split.toString() + (ROWS_PER_TABLET / 2))
              .followingKey(PartialKey.ROW).getRow());
          log.debug("After Merge");
          printAndVerifyFileMetadata(getServerContext(), tableId);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    } else {
      c.tableOperations().addSplits(table, SPLITS);
      log.debug("After splits");
      printAndVerifyFileMetadata(getServerContext(), tableId);
    }

    Text startText = start == null ? null : new Text(start);
    Text endText = end == null ? null : new Text(end);

    c.tableOperations().deleteRows(table, startText, endText);
    Collection<Text> remainingSplits = c.tableOperations().listSplits(table);
    StringBuilder sb = new StringBuilder();
    // See that whole tablets are removed
    for (Text split : remainingSplits) {
      sb.append(split);
    }
    log.debug("After delete");
    printAndVerifyFileMetadata(getServerContext(), tableId);
    assertEquals(result, sb.toString());

    // See that the rows are really deleted
    try (Scanner scanner = c.createScanner(table, Authorizations.EMPTY)) {
      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        Text row = entry.getKey().getRow();
        assertTrue((startText == null || row.compareTo(startText) <= 0)
            || (endText == null || row.compareTo(endText) > 0));
        assertTrue(startText != null || endText != null);
        count++;
      }
      log.info("Finished table {}", table);
      assertEquals(entries, count);
    }
  }

}

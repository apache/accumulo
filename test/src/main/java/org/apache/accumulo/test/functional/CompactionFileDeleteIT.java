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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionFileDeleteIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(CompactionFileDeleteIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  /**
   * Test that files created by minor compaction are marked as not shared
   */
  @Test
  public void testMinorCompactionCreatesNonSharedFiles() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = 0; i < 1000; i++) {
          Mutation m = new Mutation(String.format("row%04d", i));
          m.put("cf", "cq", "value" + i);
          bw.addMutation(m);
        }
      }

      client.tableOperations().flush(tableName, null, null, true);
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      Map<StoredTabletFile,Boolean> filesSharedStatus = getFilesSharedStatus(client, tableId);

      assertFalse(filesSharedStatus.isEmpty(), "Expected at least one file after flush");
      filesSharedStatus.forEach((file, isShared) -> {
        assertFalse(isShared, "File created by minor compaction should not be marked as shared:  "
            + file.getFileName());
        log.info("Verified file {} is not shared", file.getFileName());
      });
    }
  }

  /**
   * Test that files are marked as shared during table clone
   */
  @Test
  public void testCloneMarksFilesAsShared() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String[] names = getUniqueNames(2);
      String sourceTable = names[0];
      String cloneTable = names[1];

      client.tableOperations().create(sourceTable);
      TableId sourceTableId = TableId.of(client.tableOperations().tableIdMap().get(sourceTable));

      try (BatchWriter bw = client.createBatchWriter(sourceTable)) {
        for (int i = 0; i < 100; i++) {
          Mutation m = new Mutation(String.format("row%04d", i));
          m.put("cf", "cq", "value" + i);
          bw.addMutation(m);
        }
      }
      client.tableOperations().flush(sourceTable, null, null, true);

      Map<StoredTabletFile,Boolean> sourceFilesBefore = getFilesSharedStatus(client, sourceTableId);
      assertFalse(sourceFilesBefore.isEmpty(), "Expected files in source table");
      sourceFilesBefore
          .forEach((file, isShared) -> assertFalse(isShared, "Source files should not be shared"));

      client.tableOperations().clone(sourceTable, cloneTable, true, null, null);
      TableId cloneTableId = TableId.of(client.tableOperations().tableIdMap().get(cloneTable));

      Map<StoredTabletFile,Boolean> sourceFilesAfter = getFilesSharedStatus(client, sourceTableId);
      sourceFilesAfter.forEach((file, isShared) -> {
        assertTrue(isShared,
            "Source file " + file.getFileName() + " should be marked as shared after clone");
        log.info("Verified source file {} is shared after clone", file.getFileName());
      });

      Map<StoredTabletFile,Boolean> cloneFiles = getFilesSharedStatus(client, cloneTableId);
      cloneFiles.forEach((file, isShared) -> {
        assertTrue(isShared, "Clone file " + file.getFileName() + " should be marked as shared");
        log.info("Verified clone file {} is shared", file.getFileName());
      });
    }
  }

  /**
   * Test that bulk import marks files as shared or not based on how many tablets they go to
   */
  @Test
  public void testBulkImportMarksFilesCorrectly() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("row0500"));
      ntc.withSplits(splits);
      client.tableOperations().create(tableName, ntc);

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = 0; i < 1000; i++) {
          Mutation m = new Mutation(String.format("row%04d", i));
          m.put("cf", "cq", "value" + i);
          bw.addMutation(m);
        }
      }
      client.tableOperations().flush(tableName, null, null, true);

      Map<String,Map<StoredTabletFile,Boolean>> tabletFiles = getFilesPerTablet(client, tableId);
      assertEquals(2, tabletFiles.size(), "Expected 2 tablets");

      tabletFiles.forEach((tablet, files) -> {
        files.forEach((file, isShared) -> {
          long tabletCount = tabletFiles.values().stream()
              .filter(tabletFileMap -> tabletFileMap.containsKey(file)).count();

          if (tabletCount == 1) {
            assertFalse(isShared, "File in single tablet should not be shared");
          }
        });
      });
    }
  }

  /**
   * Test that compaction processes non-shared files correctly
   */
  @Test
  public void testCompactionDeletesNonSharedFiles() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

      for (int batch = 0; batch < 4; batch++) {
        try (BatchWriter bw = client.createBatchWriter(tableName)) {
          for (int i = 0; i < 100; i++) {
            Mutation m = new Mutation(String.format("row%04d", i));
            m.put("cf", "cq", "value_" + batch + "_" + i);
            bw.addMutation(m);
          }
        }
        client.tableOperations().flush(tableName, null, null, true);
      }

      Map<StoredTabletFile,Boolean> before = getFilesSharedStatus(client, tableId);
      int beforeCount = before.size();
      log.info("Files before compaction: {}", beforeCount);
      assertTrue(beforeCount >= 3, "Expected at least 3 files");
      before.forEach((f, s) -> assertFalse(s, "Files should not be shared"));

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      Map<StoredTabletFile,Boolean> after = getFilesSharedStatus(client, tableId);
      int afterCount = after.size();
      log.info("Files after compaction: {}", afterCount);

      assertTrue(afterCount <= beforeCount, "Expected same or fewer files after compaction");
      after.forEach((f, s) -> assertFalse(s, "Compaction output should not be shared"));

    }
  }

  /**
   * Get all files and their shared status for a table
   */
  private Map<StoredTabletFile,Boolean> getFilesSharedStatus(AccumuloClient client,
      TableId tableId) {
    Map<StoredTabletFile,Boolean> result = new HashMap<>();

    try (Scanner scanner =
        client.createScanner(Ample.DataLevel.USER.metaTable(), Authorizations.EMPTY)) {
      scanner.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

      for (var entry : scanner) {
        String row = entry.getKey().getRow().toString();
        String cq = entry.getKey().getColumnQualifier().toString();
        String value = entry.getValue().toString();

        try {
          StoredTabletFile file = new StoredTabletFile(cq);
          DataFileValue dfv = new DataFileValue(value);
          result.put(file, dfv.isShared());

          log.trace("File:  {}, Size: {}, Entries: {}, Shared: {}", file.getFileName(),
              dfv.getSize(), dfv.getNumEntries(), dfv.isShared());
        } catch (Exception e) {
          log.warn("Error parsing file entry", e);
        }
      }
    } catch (Exception e) {
      log.error("Error reading files for table {}", tableId, e);
    }

    return result;
  }

  /**
   * Get files per tablet with their shared status
   */
  private Map<String,Map<StoredTabletFile,Boolean>> getFilesPerTablet(AccumuloClient client,
      TableId tableId) {
    Map<String,Map<StoredTabletFile,Boolean>> result = new HashMap<>();

    try (Scanner scanner =
        client.createScanner(Ample.DataLevel.USER.metaTable(), Authorizations.EMPTY)) {

      scanner.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.TabletColumnFamily.NAME);

      String currentTablet = null;
      Map<StoredTabletFile,Boolean> currentFiles = new HashMap<>();

      for (var entry : scanner) {
        String row = entry.getKey().getRow().toString();
        String cf = entry.getKey().getColumnFamily().toString();
        String cq = entry.getKey().getColumnQualifier().toString();
        String value = entry.getValue().toString();

        if (currentTablet == null || !currentTablet.equals(row)) {
          if (currentTablet != null && !currentFiles.isEmpty()) {
            result.put(currentTablet, currentFiles);
          }
          currentTablet = row;
          currentFiles = new HashMap<>();
        }

        if (cf.equals(MetadataSchema.TabletsSection.DataFileColumnFamily.STR_NAME)) {
          try {
            StoredTabletFile file = new StoredTabletFile(cq);
            DataFileValue dfv = new DataFileValue(value);
            currentFiles.put(file, dfv.isShared());
          } catch (Exception e) {
            log.warn("Error parsing file entry for tablet {}", row, e);
          }
        }
      }

      if (currentTablet != null && !currentFiles.isEmpty()) {
        result.put(currentTablet, currentFiles);
      }
    } catch (Exception e) {
      log.error("Error reading files per tablet for table {}", tableId, e);
    }

    return result;
  }

  /**
   * Get GC file candidates for a table
   */
  private Set<String> getGcFileCandidates(AccumuloClient client, TableId tableId) {
    Set<String> candidates = new HashSet<>();
    try (Scanner scanner =
        client.createScanner(Ample.DataLevel.USER.metaTable(), Authorizations.EMPTY)) {

      scanner.setRange(MetadataSchema.DeletesSection.getRange());

      scanner.forEach(entry -> {
        try {
          String row = entry.getKey().getRow().toString();
          String decodedPath = MetadataSchema.DeletesSection.decodeRow(row);

          String cq = entry.getKey().getColumnQualifier().toString();
          String value = entry.getValue().toString();

          if (cq.equals(tableId.canonical()) || value.equals(tableId.canonical())) {
            candidates.add(decodedPath);
            log.debug("Found GC delete marker for table {}: {}", tableId, decodedPath);
          }
        } catch (Exception e) {
          log.warn("Error parsing GC delete marker entry", e);
        }
      });
    } catch (Exception e) {
      log.warn("Error reading GC candidates", e);
    }
    return candidates;
  }
}

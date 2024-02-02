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
package org.apache.accumulo.test;

import static org.apache.accumulo.core.Constants.IMPORT_MAPPINGS_FILE;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ImportConfiguration;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.test.util.FileMetadataUtil;
import org.apache.accumulo.test.util.Wait;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ImportTable didn't correctly place absolute paths in metadata. This resulted in the imported
 * table only being usable when the actual HDFS directory for Accumulo was the same as
 * Property.INSTANCE_DFS_DIR. If any other HDFS directory was used, any interactions with the table
 * would fail because the relative path in the metadata table (created by the ImportTable process)
 * would be converted to a non-existent absolute path.
 * <p>
 * ACCUMULO-3215
 */
public class ImportExportIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ImportExportIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  /**
   * Test that we can override a tablet availability when importing a table.
   */
  @Test
  public void overrideAvailabilityOnImport() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tableNames = getUniqueNames(2);
      String srcTable = tableNames[0], destTable = tableNames[1];

      // create the source table with the initial tablet availability
      TabletAvailability srcAvailability = TabletAvailability.HOSTED;
      NewTableConfiguration ntc =
          new NewTableConfiguration().withInitialTabletAvailability(srcAvailability);
      client.tableOperations().create(srcTable, ntc);

      populateTable(client, srcTable);

      FileSystem fs = cluster.getFileSystem();

      Path baseDir = setupBaseDir(fs);
      Path exportDir = createExportDir(baseDir, fs);
      Path[] importDirs = createImportDirs(fs, baseDir, 1);

      log.info("Exporting table to {}", exportDir);
      log.info("Importing table from {}", Arrays.toString(importDirs));

      // ensure the table has the tablet availability we expect
      waitForAvailability(client, srcTable, srcAvailability);

      // Offline the table
      client.tableOperations().offline(srcTable, true);
      // Then export it
      client.tableOperations().exportTable(srcTable, exportDir.toString());

      copyExportedFilesToImportDirs(fs, exportDir, importDirs);

      // Import the exported data into a new table
      final TabletAvailability newAvailability = TabletAvailability.UNHOSTED;
      assertNotEquals(srcAvailability, newAvailability,
          "New tablet availability should be different from the old one in order to test we can override.");

      ImportConfiguration importConfig =
          ImportConfiguration.builder().setInitialAvailability(newAvailability).build();
      client.tableOperations().importTable(destTable,
          Arrays.stream(importDirs).map(Path::toString).collect(Collectors.toSet()), importConfig);

      waitForAvailability(client, destTable, newAvailability);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testExportImportThenScan(boolean fenced) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tableNames = getUniqueNames(2);
      String srcTable = tableNames[0], destTable = tableNames[1];
      client.tableOperations().create(srcTable);

      int expected = populateTable(client, srcTable);

      // Test that files with ranges and are fenced work with export/import
      if (fenced) {
        // Split file into 3 ranges of 10000, 20000, and 5000 for a total of 35000
        FileMetadataUtil.splitFilesIntoRanges(getServerContext(), srcTable, createRanges());
        expected = 35000;
      }

      FileSystem fs = cluster.getFileSystem();

      Path baseDir = setupBaseDir(fs);
      Path exportDir = createExportDir(baseDir, fs);
      Path[] importDirs = createImportDirs(fs, baseDir, 2);

      log.info("Exporting table to {}", exportDir);
      log.info("Importing table from {}", Arrays.toString(importDirs));

      // test fast fail offline check
      assertThrows(IllegalStateException.class,
          () -> client.tableOperations().exportTable(srcTable, exportDir.toString()));

      // Offline the table
      client.tableOperations().offline(srcTable, true);
      // Then export it
      client.tableOperations().exportTable(srcTable, exportDir.toString());

      copyExportedFilesToImportDirs(fs, exportDir, importDirs);

      // Import the exported data into a new table
      client.tableOperations().importTable(destTable,
          Arrays.stream(importDirs).map(Path::toString).collect(Collectors.toSet()),
          ImportConfiguration.empty());

      // Get the table ID for the table that the importtable command created
      final String tableId = client.tableOperations().tableIdMap().get(destTable);
      assertNotNull(tableId);

      // Get all `file` colfams from the metadata table for the new table
      log.info("Imported into table with ID: {}", tableId);

      try (Scanner s =
          client.createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
        s.setRange(TabletsSection.getRange(TableId.of(tableId)));
        s.fetchColumnFamily(DataFileColumnFamily.NAME);
        ServerColumnFamily.DIRECTORY_COLUMN.fetch(s);

        // Should find a single entry
        for (Entry<Key,Value> fileEntry : s) {
          Key k = fileEntry.getKey();
          String value = fileEntry.getValue().toString();
          if (k.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            // The file should be an absolute URI (file:///...), not a relative path
            // (/b-000.../I000001.rf)
            var tabFile = StoredTabletFile.of(k.getColumnQualifier());
            // Verify that the range is set correctly on the StoredTabletFile
            assertEquals(fenced, !tabFile.getRange().isInfiniteStartKey()
                || !tabFile.getRange().isInfiniteStopKey());
            assertFalse(looksLikeRelativePath(tabFile.getMetadataPath()),
                "Imported files should have absolute URIs, not relative: " + tabFile);
          } else if (k.getColumnFamily().equals(ServerColumnFamily.NAME)) {
            assertFalse(looksLikeRelativePath(value),
                "Server directory should have absolute URI, not relative: " + value);
          } else {
            fail("Got expected pair: " + k + "=" + fileEntry.getValue());
          }
        }

      }
      // Online the original table before we verify equivalence
      client.tableOperations().online(srcTable, true);

      verifyTableEquality(client, srcTable, destTable, expected);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testExportImportOffline(boolean fenced) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tableNames = getUniqueNames(2);
      String srcTable = tableNames[0], destTable = tableNames[1];
      client.tableOperations().create(srcTable);

      int expected = populateTable(client, srcTable);

      // Test that files with ranges and are fenced work with export/import
      if (fenced) {
        // Split file into 3 ranges of 10000, 20000, and 5000 for a total of 35000
        FileMetadataUtil.splitFilesIntoRanges(getServerContext(), srcTable, createRanges());
        expected = 35000;
      }

      FileSystem fs = cluster.getFileSystem();

      Path baseDir = setupBaseDir(fs);
      Path exportDir = createExportDir(baseDir, fs);
      Path[] importDirs = createImportDirs(fs, baseDir, 2);

      log.info("Exporting table to {}", exportDir);
      log.info("Importing table from {}", Arrays.toString(importDirs));

      // Offline the table
      client.tableOperations().offline(srcTable, true);
      // Then export it
      client.tableOperations().exportTable(srcTable, exportDir.toString());

      copyExportedFilesToImportDirs(fs, exportDir, importDirs);

      // Import the exported data into a new offline table and keep mappings file
      ImportConfiguration importConfig =
          ImportConfiguration.builder().setKeepOffline(true).setKeepMappings(true).build();
      client.tableOperations().importTable(destTable,
          Arrays.stream(importDirs).map(Path::toString).collect(Collectors.toSet()), importConfig);

      // Get the table ID for the table that the importtable command created
      final String tableId = client.tableOperations().tableIdMap().get(destTable);
      assertNotNull(tableId);

      log.info("Imported into table with ID: {}", tableId);

      // verify the new table is offline
      assertFalse(client.tableOperations().isOnline(destTable), "Table should have been offline.");
      assertEquals(getServerContext().getTableState(TableId.of(tableId)), TableState.OFFLINE);
      client.tableOperations().online(destTable, true);

      // Get all `file` colfams from the metadata table for the new table
      try (Scanner s =
          client.createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
        s.setRange(TabletsSection.getRange(TableId.of(tableId)));
        s.fetchColumnFamily(DataFileColumnFamily.NAME);
        ServerColumnFamily.DIRECTORY_COLUMN.fetch(s);

        // Should find a single entry
        for (Entry<Key,Value> fileEntry : s) {
          Key k = fileEntry.getKey();
          String value = fileEntry.getValue().toString();
          if (k.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            // file should be an absolute URI (file:///...), not relative (/b-000.../I000001.rf)
            var tabFile = StoredTabletFile.of(k.getColumnQualifier());
            // Verify that the range is set correctly on the StoredTabletFile
            assertEquals(fenced, !tabFile.getRange().isInfiniteStartKey()
                || !tabFile.getRange().isInfiniteStopKey());
            assertFalse(looksLikeRelativePath(tabFile.getMetadataPath()),
                "Imported files should have absolute URIs, not relative: "
                    + tabFile.getMetadataPath());
          } else if (k.getColumnFamily().equals(ServerColumnFamily.NAME)) {
            assertFalse(looksLikeRelativePath(value),
                "Server directory should have absolute URI, not relative: " + value);
          } else {
            fail("Got expected pair: " + k + "=" + fileEntry.getValue());
          }
        }
      }
      // Online the original table before we verify equivalence
      client.tableOperations().online(srcTable, true);

      verifyTableEquality(client, srcTable, destTable, expected);
      assertTrue(verifyMappingsFile(tableId), "Did not find mappings file");
    }
  }

  private boolean verifyMappingsFile(String destTableId) throws IOException {
    AccumuloCluster cluster = getCluster();
    assertTrue(cluster instanceof MiniAccumuloClusterImpl);
    MiniAccumuloClusterImpl mac = (MiniAccumuloClusterImpl) cluster;
    String rootPath = mac.getConfig().getDir().getAbsolutePath();
    FileSystem fs = getCluster().getFileSystem();
    FileStatus[] status = fs.listStatus(new Path(rootPath + "/accumulo/tables/" + destTableId));
    for (FileStatus tabletDir : status) {
      var contents = fs.listStatus(tabletDir.getPath());
      for (FileStatus file : contents) {
        if (file.isFile() && file.getPath().getName().equals(IMPORT_MAPPINGS_FILE)) {
          log.debug("Found mappings file: {}", file);
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Validate that files exported with Accumulo 2.x without fence ranges can be imported into
   * version that require the fenced ranges (3.1 and later)
   */
  @Test
  public void importV2data() throws Exception {
    final String dataRoot = "./target/classes/v2_import_test";
    final String dataSrc = dataRoot + "/data";
    final String importDir = dataRoot + "/import";

    // copy files each run will "move the files" on import, allows multiple runs in IDE without
    // rebuild
    java.nio.file.Path importDirPath = Paths.get(importDir);
    java.nio.file.Files.createDirectories(importDirPath);
    FileUtils.copyDirectory(new File(dataSrc), new File(importDir));

    String table = getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      log.debug("importing from: {} into table: {}", importDir, table);
      client.tableOperations().importTable(table, importDir);

      int rowCount = 0;
      try (Scanner s = client.createScanner(table, Authorizations.EMPTY)) {
        for (Entry<Key,Value> entry : s) {
          log.trace("data:{}", entry);
          rowCount++;
        }
      }
      assertEquals(7, rowCount);
      int metaFileCount = 0;
      try (Scanner s =
          client.createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
        TableId tid = TableId.of(client.tableOperations().tableIdMap().get(table));
        s.setRange(TabletsSection.getRange(tid));
        s.fetchColumnFamily(DataFileColumnFamily.NAME);
        for (Entry<Key,Value> entry : s) {
          log.trace("metadata file:{}", entry);
          metaFileCount++;
        }
      }
      final List<Text> expectedSplits = List.of(new Text("2"), new Text("4"), new Text("6"));
      assertEquals(expectedSplits, client.tableOperations().listSplits(table));
      assertEquals(4, metaFileCount);
    }
  }

  private void verifyTableEquality(AccumuloClient client, String srcTable, String destTable,
      int expected) throws Exception {
    Iterator<Entry<Key,Value>> src =
        client.createScanner(srcTable, Authorizations.EMPTY).iterator(),
        dest = client.createScanner(destTable, Authorizations.EMPTY).iterator();
    assertTrue(src.hasNext(), "Could not read any data from source table");
    assertTrue(dest.hasNext(), "Could not read any data from destination table");
    int entries = 0;
    while (src.hasNext() && dest.hasNext()) {
      Entry<Key,Value> orig = src.next(), copy = dest.next();
      assertEquals(orig.getKey(), copy.getKey());
      assertEquals(orig.getValue(), copy.getValue());
      entries++;
    }
    assertFalse(src.hasNext(), "Source table had more data to read");
    assertFalse(dest.hasNext(), "Dest table had more data to read");
    assertEquals(expected, entries);
  }

  private boolean looksLikeRelativePath(String uri) {
    if (uri.startsWith("/" + Constants.BULK_PREFIX)) {
      return uri.charAt(10) == '/';
    } else {
      return uri.startsWith("/" + Constants.CLONE_PREFIX);
    }
  }

  private Set<Range> createRanges() {
    // Split file into ranges of 10000, 20000, and 5000 for a total of 35000
    return Set.of(
        new Range("row_" + String.format("%010d", 100), "row_" + String.format("%010d", 199)),
        new Range("row_" + String.format("%010d", 300), "row_" + String.format("%010d", 499)),
        new Range("row_" + String.format("%010d", 700), "row_" + String.format("%010d", 749)));
  }

  /**
   * Make a directory we can use to throw the export and import directories
   *
   * @return Path to the base directory
   */
  private Path setupBaseDir(FileSystem fs) throws IOException {
    // Must exist on the filesystem the cluster is running.
    log.info("Using FileSystem: " + fs);
    Path baseDir = new Path(cluster.getTemporaryPath(), getClass().getName());
    fs.deleteOnExit(baseDir);
    if (fs.exists(baseDir)) {
      log.info("{} exists on filesystem, deleting", baseDir);
      assertTrue(fs.delete(baseDir, true), "Failed to deleted " + baseDir);
    }
    log.info("Creating {}", baseDir);
    assertTrue(fs.mkdirs(baseDir), "Failed to create " + baseDir);
    return baseDir;
  }

  private static Path createExportDir(Path baseDir, FileSystem fs) throws IOException {
    Path exportDir = new Path(baseDir, "export");
    fs.deleteOnExit(exportDir);
    assertTrue(fs.mkdirs(exportDir), "Failed to create " + exportDir);
    return exportDir;
  }

  private Path[] createImportDirs(FileSystem fs, Path baseDir, int numberOfImportDirs)
      throws IOException {
    Path[] importDirs = new Path[numberOfImportDirs];

    for (int i = 0; i < numberOfImportDirs; i++) {
      Path importDir = new Path(baseDir, "import-" + (i + 1));
      fs.deleteOnExit(importDir);
      assertTrue(fs.mkdirs(importDir) && fs.exists(importDir), "Failed to create " + importDir);
      importDirs[i] = importDir;
    }

    return importDirs;
  }

  private static int populateTable(AccumuloClient client, String srcTable)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    int rowCount = 1000;
    int colCount = 100;
    try (BatchWriter bw = client.createBatchWriter(srcTable)) {
      for (int row = 0; row < rowCount; row++) {
        Mutation m = new Mutation("row_" + String.format("%010d", row));
        for (int col = 0; col < colCount; col++) {
          m.put(Integer.toString(col), "", Integer.toString(col * 2));
        }
        bw.addMutation(m);
      }
    }
    client.tableOperations().compact(srcTable, null, null, true, true);
    return rowCount * colCount;
  }

  private void copyExportedFilesToImportDirs(FileSystem fs, Path exportDir, Path[] importDirs)
      throws IOException {
    Path distcp = new Path(exportDir, "distcp.txt");
    assertTrue(fs.exists(distcp), "Distcp file doesn't exist");
    try (FSDataInputStream is = fs.open(distcp); InputStreamReader in = new InputStreamReader(is);
        BufferedReader reader = new BufferedReader(in)) {
      String line;
      while ((line = reader.readLine()) != null) {
        Path srcPath = new Path(line.substring(5));
        assertTrue(fs.exists(srcPath), "File doesn't exist: " + srcPath);
        // get a random import directory from the list
        Path randomImportDir = importDirs[RANDOM.get().nextInt(importDirs.length)];
        Path destPath = new Path(randomImportDir, srcPath.getName());
        assertFalse(fs.exists(destPath), "Did not expect " + destPath + " to exist");
        FileUtil.copy(fs, srcPath, fs, destPath, false, fs.getConf());
      }
    }
    log.info("Copied files to import directories");
    for (int i = 0; i < importDirs.length; i++) {
      log.info("Import dir {}: {}", i + 1, Arrays.toString(fs.listStatus(importDirs[i])));
    }
  }

  private static void waitForAvailability(AccumuloClient client, String tableName,
      TabletAvailability expectedAvailability) {
    Wait.waitFor(() -> client.tableOperations().getTabletInformation(tableName, new Range())
        .allMatch(tabletInformation -> {
          TabletAvailability currentAvailability = tabletInformation.getTabletAvailability();
          boolean goalReached = currentAvailability == expectedAvailability;
          if (!goalReached) {
            log.info("Current tablet availability: " + currentAvailability + ". Waiting for "
                + expectedAvailability);
          }
          return goalReached;
        }), 30_000L, 1_000L,
        "Timed out waiting for tablet availability " + expectedAvailability + " on " + tableName);
  }

}

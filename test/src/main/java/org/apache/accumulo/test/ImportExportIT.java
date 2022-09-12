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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.cluster.AccumuloCluster;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.ImportConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
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

  @Test
  public void testExportImportThenScan() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tableNames = getUniqueNames(2);
      String srcTable = tableNames[0], destTable = tableNames[1];
      client.tableOperations().create(srcTable);

      try (BatchWriter bw = client.createBatchWriter(srcTable)) {
        for (int row = 0; row < 1000; row++) {
          Mutation m = new Mutation(Integer.toString(row));
          for (int col = 0; col < 100; col++) {
            m.put(Integer.toString(col), "", Integer.toString(col * 2));
          }
          bw.addMutation(m);
        }
      }

      client.tableOperations().compact(srcTable, null, null, true, true);

      // Make a directory we can use to throw the export and import directories
      // Must exist on the filesystem the cluster is running.
      FileSystem fs = cluster.getFileSystem();
      log.info("Using FileSystem: " + fs);
      Path baseDir = new Path(cluster.getTemporaryPath(), getClass().getName());
      fs.deleteOnExit(baseDir);
      if (fs.exists(baseDir)) {
        log.info("{} exists on filesystem, deleting", baseDir);
        assertTrue(fs.delete(baseDir, true), "Failed to deleted " + baseDir);
      }
      log.info("Creating {}", baseDir);
      assertTrue(fs.mkdirs(baseDir), "Failed to create " + baseDir);
      Path exportDir = new Path(baseDir, "export");
      fs.deleteOnExit(exportDir);
      Path importDirA = new Path(baseDir, "import-a");
      Path importDirB = new Path(baseDir, "import-b");
      fs.deleteOnExit(importDirA);
      fs.deleteOnExit(importDirB);
      for (Path p : new Path[] {exportDir, importDirA, importDirB}) {
        assertTrue(fs.mkdirs(p), "Failed to create " + p);
      }

      Set<String> importDirs = Set.of(importDirA.toString(), importDirB.toString());

      Path[] importDirAry = new Path[] {importDirA, importDirB};

      log.info("Exporting table to {}", exportDir);
      log.info("Importing table from {}", importDirs);

      // test fast fail offline check
      assertThrows(IllegalStateException.class,
          () -> client.tableOperations().exportTable(srcTable, exportDir.toString()));

      // Offline the table
      client.tableOperations().offline(srcTable, true);
      // Then export it
      client.tableOperations().exportTable(srcTable, exportDir.toString());

      // Make sure the distcp.txt file that exporttable creates is available
      Path distcp = new Path(exportDir, "distcp.txt");
      fs.deleteOnExit(distcp);
      assertTrue(fs.exists(distcp), "Distcp file doesn't exist");
      FSDataInputStream is = fs.open(distcp);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));

      // Copy each file that was exported to one of the imports directory
      String line;

      while ((line = reader.readLine()) != null) {
        Path p = new Path(line.substring(5));
        assertTrue(fs.exists(p), "File doesn't exist: " + p);
        Path importDir = importDirAry[random.nextInt(importDirAry.length)];
        Path dest = new Path(importDir, p.getName());
        assertFalse(fs.exists(dest), "Did not expect " + dest + " to exist");
        FileUtil.copy(fs, p, fs, dest, false, fs.getConf());
      }

      reader.close();

      log.info("Import dir A: {}", Arrays.toString(fs.listStatus(importDirA)));
      log.info("Import dir B: {}", Arrays.toString(fs.listStatus(importDirB)));

      // Import the exported data into a new table
      client.tableOperations().importTable(destTable, importDirs, ImportConfiguration.empty());

      // Get the table ID for the table that the importtable command created
      final String tableId = client.tableOperations().tableIdMap().get(destTable);
      assertNotNull(tableId);

      // Get all `file` colfams from the metadata table for the new table
      log.info("Imported into table with ID: {}", tableId);

      try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
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
            String fileUri = k.getColumnQualifier().toString();
            assertFalse(looksLikeRelativePath(fileUri),
                "Imported files should have absolute URIs, not relative: " + fileUri);
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

      verifyTableEquality(client, srcTable, destTable);
    }
  }

  @Test
  public void testExportImportOffline() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tableNames = getUniqueNames(2);
      String srcTable = tableNames[0], destTable = tableNames[1];
      client.tableOperations().create(srcTable);

      try (BatchWriter bw = client.createBatchWriter(srcTable)) {
        for (int row = 0; row < 1000; row++) {
          Mutation m = new Mutation(Integer.toString(row));
          for (int col = 0; col < 100; col++) {
            m.put(Integer.toString(col), "", Integer.toString(col * 2));
          }
          bw.addMutation(m);
        }
      }

      client.tableOperations().compact(srcTable, new CompactionConfig());

      // Make export and import directories
      FileSystem fs = cluster.getFileSystem();
      log.info("Using FileSystem: " + fs);
      Path baseDir = new Path(cluster.getTemporaryPath(), getClass().getName());
      fs.deleteOnExit(baseDir);
      if (fs.exists(baseDir)) {
        log.info("{} exists on filesystem, deleting", baseDir);
        assertTrue(fs.delete(baseDir, true), "Failed to deleted " + baseDir);
      }
      log.info("Creating {}", baseDir);
      assertTrue(fs.mkdirs(baseDir), "Failed to create " + baseDir);
      Path exportDir = new Path(baseDir, "export");
      fs.deleteOnExit(exportDir);
      Path importDirA = new Path(baseDir, "import-a");
      Path importDirB = new Path(baseDir, "import-b");
      fs.deleteOnExit(importDirA);
      fs.deleteOnExit(importDirB);
      for (Path p : new Path[] {exportDir, importDirA, importDirB}) {
        assertTrue(fs.mkdirs(p), "Failed to create " + p);
      }

      Set<String> importDirs = Set.of(importDirA.toString(), importDirB.toString());

      Path[] importDirAry = new Path[] {importDirA, importDirB};

      log.info("Exporting table to {}", exportDir);
      log.info("Importing table from {}", importDirs);

      // Offline the table
      client.tableOperations().offline(srcTable, true);
      // Then export it
      client.tableOperations().exportTable(srcTable, exportDir.toString());

      // Make sure the distcp.txt file that exporttable creates is available
      Path distcp = new Path(exportDir, "distcp.txt");
      fs.deleteOnExit(distcp);
      assertTrue(fs.exists(distcp), "Distcp file doesn't exist");
      FSDataInputStream is = fs.open(distcp);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));

      // Copy each file that was exported to one of the imports directory
      String line;

      while ((line = reader.readLine()) != null) {
        Path p = new Path(line.substring(5));
        assertTrue(fs.exists(p), "File doesn't exist: " + p);
        Path importDir = importDirAry[random.nextInt(importDirAry.length)];
        Path dest = new Path(importDir, p.getName());
        assertFalse(fs.exists(dest), "Did not expect " + dest + " to exist");
        FileUtil.copy(fs, p, fs, dest, false, fs.getConf());
      }

      reader.close();

      log.info("Import dir A: {}", Arrays.toString(fs.listStatus(importDirA)));
      log.info("Import dir B: {}", Arrays.toString(fs.listStatus(importDirB)));

      // Import the exported data into a new offline table and keep mappings file
      ImportConfiguration importConfig =
          ImportConfiguration.builder().setKeepOffline(true).setKeepMappings(true).build();
      client.tableOperations().importTable(destTable, importDirs, importConfig);

      // Get the table ID for the table that the importtable command created
      final String tableId = client.tableOperations().tableIdMap().get(destTable);
      assertNotNull(tableId);

      log.info("Imported into table with ID: {}", tableId);

      // verify the new table is offline
      assertFalse(client.tableOperations().isOnline(destTable), "Table should have been offline.");
      client.tableOperations().online(destTable, true);

      // Get all `file` colfams from the metadata table for the new table
      try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        s.setRange(TabletsSection.getRange(TableId.of(tableId)));
        s.fetchColumnFamily(DataFileColumnFamily.NAME);
        ServerColumnFamily.DIRECTORY_COLUMN.fetch(s);

        // Should find a single entry
        for (Entry<Key,Value> fileEntry : s) {
          Key k = fileEntry.getKey();
          String value = fileEntry.getValue().toString();
          if (k.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            // file should be an absolute URI (file:///...), not relative (/b-000.../I000001.rf)
            String fileUri = k.getColumnQualifier().toString();
            assertFalse(looksLikeRelativePath(fileUri),
                "Imported files should have absolute URIs, not relative: " + fileUri);
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

      verifyTableEquality(client, srcTable, destTable);
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

  private void verifyTableEquality(AccumuloClient client, String srcTable, String destTable)
      throws Exception {
    Iterator<Entry<Key,Value>> src =
        client.createScanner(srcTable, Authorizations.EMPTY).iterator(),
        dest = client.createScanner(destTable, Authorizations.EMPTY).iterator();
    assertTrue(src.hasNext(), "Could not read any data from source table");
    assertTrue(dest.hasNext(), "Could not read any data from destination table");
    while (src.hasNext() && dest.hasNext()) {
      Entry<Key,Value> orig = src.next(), copy = dest.next();
      assertEquals(orig.getKey(), copy.getKey());
      assertEquals(orig.getValue(), copy.getValue());
    }
    assertFalse(src.hasNext(), "Source table had more data to read");
    assertFalse(dest.hasNext(), "Dest table had more data to read");
  }

  private boolean looksLikeRelativePath(String uri) {
    if (uri.startsWith("/" + Constants.BULK_PREFIX)) {
      return uri.charAt(10) == '/';
    } else {
      return uri.startsWith("/" + Constants.CLONE_PREFIX);
    }
  }
}

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

import static org.apache.accumulo.core.conf.Property.MANAGER_TABLE_DELETE_OPTIMIZATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests that verify files are physically removed from HDFS.
 */
public class DeleteAndVerifyFileRemovalsIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(DeleteAndVerifyFileRemovalsIT.class);

  private static final long GC_MAX_WAIT = 90_000;

  private static final long POLLING_WAIT = 500;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Speed up the GC so tests do not need to wait minutes.
    cfg.setProperty(Property.GC_CYCLE_START, "2s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "2s");
  }

  /**
   * Verify that a table's HDFS directory and all of its RFiles are eventually removed after a
   * delete operation.
   */
  @Test
  public void testManagerRemovesFilesOnTableDelete() throws Exception {
    // Stop the GC so gcCandidates persist if created.
    getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      final String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      writeAndFlush(client, tableName, 200);

      final TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

      final FileSystem fs = getCluster().getFileSystem();
      final Path tableDir = returnTableHdfsDir(tableId);

      // Verify that no GC deletion markers currently exist
      assertFalse(getServerContext().getAmple().getGcCandidates(Ample.DataLevel.USER).hasNext());

      assertTrue(fs.exists(tableDir),
          "Table HDFS directory must exist before deleting: " + tableDir);
      assertTrue(hasRFiles(fs, tableDir),
          "At least one file must exist before deleting: " + tableDir);

      client.tableOperations().delete(tableName);

      // Verify that no GC deletion markers were created
      assertFalse(getServerContext().getAmple().getGcCandidates(Ample.DataLevel.USER).hasNext());

      Wait.waitFor(() -> !fs.exists(tableDir), GC_MAX_WAIT, POLLING_WAIT,
          "Table HDFS directory must be removed after delete: " + tableDir);

      assertFalse(fs.exists(tableDir),
          "Table HDFS directory still exists after Manager deleted volumes for " + tableDir);
    }
  }

  /**
   * Verify that when {@link Property#MANAGER_TABLE_DELETE_OPTIMIZATION} is set to {@code false},
   * the manager skips the metadata table scan and writes gcCandidates. The GC is then responsible
   * for file deletion.
   */
  @Test
  public void testManagerCreatesGcCandidatesOnTableDelete() throws Exception {
    // Stop the GC so gcCandidates persist if created.
    getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.instanceOperations().setProperty(MANAGER_TABLE_DELETE_OPTIMIZATION.getKey(), "false");

      final String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      writeAndFlush(client, tableName, 200);

      final TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      assertNotNull(tableId, "tableId must be resolvable before delete");

      final FileSystem fs = getCluster().getFileSystem();
      final Path tableDir = returnTableHdfsDir(tableId);
      log.info("Table {} ({}) directory :{}", tableName, tableId, tableDir);

      assertTrue(fs.exists(tableDir), "Table HDFS directory must exist before delete: " + tableDir);
      assertTrue(hasRFiles(fs, tableDir),
          "At least one rfile must exist before delete: " + tableDir);

      // Verify no gcCandidates exist
      assertFalse(getServerContext().getAmple().getGcCandidates(Ample.DataLevel.USER).hasNext());

      client.tableOperations().delete(tableName);

      // GcCandidates should now exist
      Wait.waitFor(
          () -> getServerContext().getAmple().getGcCandidates(Ample.DataLevel.USER).hasNext(),
          1_000);
      getCluster().getClusterControl().start(ServerType.GARBAGE_COLLECTOR);

      Wait.waitFor(() -> !fs.exists(tableDir), GC_MAX_WAIT, POLLING_WAIT,
          "Table HDFS directory must be removed after delete: " + tableDir);

      assertFalse(fs.exists(tableDir),
          "Table HDFS directory still exists after GC ran: " + tableDir);
    }
  }

  /**
   * Verify that a table directory containing multiple tablets is removed after deletion.
   */
  @Test
  public void testDeletingMultipleTablets() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      final String tableName = getUniqueNames(1)[0];

      // Create a pre-split table so there will be multiple tablet directories.
      client.tableOperations().create(tableName);
      client.tableOperations().addSplits(tableName, new java.util.TreeSet<>(
          List.of(new org.apache.hadoop.io.Text("m"), new org.apache.hadoop.io.Text("t"))));

      // Write data spread across all splits and flush each tablet to produce RFiles.
      writeAndFlush(client, tableName, 300);

      final TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      assertNotNull(tableId);

      final FileSystem fs = getCluster().getFileSystem();
      final Path tableDir = returnTableHdfsDir(tableId);

      assertTrue(fs.exists(tableDir), "Table dir must exist: " + tableDir);
      assertTrue(hasRFiles(fs, tableDir), "RFiles must exist before delete: " + tableDir);

      // Capture every tablet so we can verify they are all removed.
      final List<Path> tabletDirs = listSubDirectories(fs, tableDir);
      assertFalse(tabletDirs.isEmpty(), "Expected at least one tablet subdirectory");
      log.info("Tablet subdirectories for {}: {}", tableName, tabletDirs);

      client.tableOperations().delete(tableName);

      Wait.waitFor(() -> !fs.exists(tableDir), GC_MAX_WAIT, POLLING_WAIT,
          "Table HDFS directory must be removed: " + tableDir);

      for (Path tabletDir : tabletDirs) {
        assertFalse(fs.exists(tabletDir),
            "Tablet subdirectory must not persist after table delete: " + tabletDir);
      }
    }
  }

  /**
   * Verify that when a source table is deleted, only its own files are eventually removed. Files
   * that are still referenced by a cloned table must remain.
   */
  @Test
  public void testDeletingClonedTablePersistsFiles() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      var names = getUniqueNames(2);
      final String sourceTable = names[0];
      final String cloneTable = names[1];
      ;

      client.tableOperations().create(sourceTable);
      writeAndFlush(client, sourceTable, 400);

      // Clone shares the same underlying files as the source at clone time.
      client.tableOperations().clone(sourceTable, cloneTable, true, Map.of(), Set.of());

      var tableIds = client.tableOperations().tableIdMap();
      TableId sourceTableId = TableId.of(tableIds.get(sourceTable));
      TableId cloneTableId = TableId.of(tableIds.get(cloneTable));

      Wait.waitFor(() -> client.tableOperations().exists(cloneTable));

      final FileSystem fs = getCluster().getFileSystem();
      final Path sourceDir = returnTableHdfsDir(sourceTableId);
      final Path cloneDir = returnTableHdfsDir(cloneTableId);

      assertTrue(fs.exists(sourceDir), "Source dir must exist before delete");
      client.tableOperations().delete(sourceTable);

      // The source directory and its files must still be present because the GC must not
      // delete files that are still referenced by the clone.
      assertTrue(fs.exists(sourceDir),
          "Source HDFS directory must survive after source table is deleted: " + sourceDir);
      assertTrue(hasRFiles(fs, sourceDir),
          "Source RFiles must survive after source table is deleted: " + sourceDir);

      // Verify that the files have been removed from accumulo metadata.
      try (var ample = getCluster().getServerContext().getAmple().readTablets()
          .forTable(sourceTableId).fetch(TabletMetadata.ColumnType.FILES).build()) {
        assertEquals(0, ample.stream().count());
      }

      writeAndFlush(client, cloneTable, 100);
      client.tableOperations().setProperty(cloneTable, Property.TABLE_FILE_MAX.getKey(), "1");
      client.tableOperations().compact(cloneTable, new CompactionConfig().setWait(true));

      boolean findSourceTableGcCandidates = false;
      var gcCandidates =
          getCluster().getServerContext().getAmple().getGcCandidates(Ample.DataLevel.USER);
      while (gcCandidates.hasNext()) {
        GcCandidate candidate = gcCandidates.next();
        if (candidate.getPath().contains("/accumulo/tables/" + sourceTableId.canonical())) {
          findSourceTableGcCandidates = true;
        }
      }
      assertTrue(findSourceTableGcCandidates,
          "Did not find a gcCandidate for the source table" + sourceTableId.canonical());

      Wait.waitFor(() -> !fs.exists(sourceDir), GC_MAX_WAIT, POLLING_WAIT,
          "Table HDFS directory must be removed: " + sourceDir);

      // The full compaction should have removed the file Refs to the source directory so those
      // files can now be removed.
      assertTrue(fs.exists(cloneDir),
          "Cloned HDFS directory must survive after source table is deleted: " + cloneDir);
      assertTrue(hasRFiles(fs, cloneDir),
          "Cloned RFiles must survive after source table is deleted: " + cloneDir);

      client.tableOperations().delete(cloneTable);

      Wait.waitFor(() -> !fs.exists(cloneDir), GC_MAX_WAIT, POLLING_WAIT,
          "Clone HDFS directory must be removed after clone is deleted: " + cloneDir);
    }
  }

  /**
   * Verify that rapidly deleting many tables in succession does not leave orphaned files in HDFS.
   */
  @Test
  public void testDeleteMultipleTables() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      final int tableCount = 5;
      final String[] tableNames = getUniqueNames(tableCount);

      for (String name : tableNames) {
        client.tableOperations().create(name);
        writeAndFlush(client, name, 50);
      }

      final Map<String,Path> hdfsPaths = new LinkedHashMap<>();
      for (String name : tableNames) {
        final TableId id = TableId.of(client.tableOperations().tableIdMap().get(name));
        hdfsPaths.put(name, returnTableHdfsDir(id));
      }

      final FileSystem fs = getCluster().getFileSystem();

      for (Map.Entry<String,Path> e : hdfsPaths.entrySet()) {
        assertTrue(fs.exists(e.getValue()), "Table dir must exist before delete: " + e.getValue());
      }

      for (String name : tableNames) {
        client.tableOperations().delete(name);
      }

      for (Map.Entry<String,Path> e : hdfsPaths.entrySet()) {
        final Path dir = e.getValue();
        Wait.waitFor(() -> !fs.exists(dir), GC_MAX_WAIT, POLLING_WAIT,
            "HDFS directory must be removed for deleted table " + e.getKey() + ": " + dir);
        assertFalse(fs.exists(dir),
            "HDFS directory still exists after GC for table " + e.getKey() + ": " + dir);
      }
    }
  }

  private void writeAndFlush(AccumuloClient client, String tableName, int rowCount)
      throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (int i = 0; i < rowCount; i++) {
        Mutation m = new Mutation(String.format("row%06d", i));
        m.put("cf", "cq", "value" + i);
        bw.addMutation(m);
      }
    }
    client.tableOperations().flush(tableName, null, null, true);
  }

  private Path returnTableHdfsDir(TableId tableId) {
    ServerContext ctx = getCluster().getServerContext();
    VolumeManager vm = ctx.getVolumeManager();
    String volumeBase = vm.getVolumes().iterator().next().getBasePath();
    return new Path(volumeBase, "tables/" + tableId.canonical());
  }

  private boolean hasRFiles(FileSystem fs, Path dir) throws Exception {
    if (!fs.exists(dir)) {
      return false;
    }
    var files = fs.listFiles(dir, true);
    while (files.hasNext()) {
      if (files.next().getPath().getName().endsWith(".rf")) {
        return true;
      }
    }
    return false;
  }

  private List<Path> listSubDirectories(FileSystem fs, Path parent) throws Exception {
    final List<Path> children = new ArrayList<>();
    if (!fs.exists(parent)) {
      return children;
    }
    for (FileStatus status : fs.listStatus(parent)) {
      if (status.isDirectory()) {
        children.add(status.getPath());
      }
    }
    return children;
  }
}

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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.accumulo.server.util.FindCompactionTmpFiles;
import org.apache.accumulo.server.util.FindCompactionTmpFiles.DeleteStats;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FindCompactionTmpFilesIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void before() throws Exception {
    startMiniCluster();
  }

  @AfterAll
  public static void after() throws Exception {
    stopMiniCluster();
  }

  private Set<Path> generateTmpFilePaths(ServerContext context, TableId tid, Path tabletDir,
      int numFiles) {
    final Set<Path> paths = new HashSet<>(numFiles);
    final TabletsMetadata tms = context.getAmple().readTablets().forTable(tid).build();
    final TabletMetadata tm = tms.iterator().next();

    for (int i = 0; i < numFiles; i++) {
      ReferencedTabletFile rtf = TabletNameGenerator.getNextDataFilenameForMajc(false, context, tm,
          (s) -> {}, ExternalCompactionId.generate(UUID.randomUUID()));
      paths.add(rtf.getPath());
    }
    return paths;
  }

  @Test
  public void testFindCompactionTmpFiles() throws Exception {

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      ReadWriteIT.ingest(c, 100, 1, 1, 0, tableName);
      c.tableOperations().flush(tableName);

      String tableId = c.tableOperations().tableIdMap().get(tableName);
      TableId tid = TableId.of(tableId);

      ServerContext ctx = getCluster().getServerContext();
      FileSystem fs = getCluster().getFileSystem();

      Set<String> tablesDirs = ctx.getTablesDirs();
      assertEquals(1, tablesDirs.size());

      String tdir = tablesDirs.iterator().next() + "/" + tid.canonical() + "/default_tablet";
      Path defaultTabletPath = new Path(tdir);
      assertTrue(fs.exists(defaultTabletPath));

      assertEquals(0, FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical()).size());

      Set<Path> generatedPaths = generateTmpFilePaths(ctx, tid, defaultTabletPath, 100);

      for (Path p : generatedPaths) {
        assertFalse(fs.exists(p));
        assertTrue(fs.createNewFile(p));
        assertTrue(fs.exists(p));
      }

      Set<Path> foundPaths = FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical());
      assertEquals(100, foundPaths.size());
      assertEquals(foundPaths, generatedPaths);

      DeleteStats stats = FindCompactionTmpFiles.deleteTempFiles(ctx, foundPaths);
      assertEquals(100, stats.success);
      assertEquals(0, stats.failure);
      assertEquals(0, stats.error);

      foundPaths = FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical());
      assertEquals(0, foundPaths.size());

    }
  }

  @Test
  public void testFindCompactionTmpFilesMainNoDelete() throws Exception {

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      ReadWriteIT.ingest(c, 100, 1, 1, 0, tableName);
      c.tableOperations().flush(tableName);

      String tableId = c.tableOperations().tableIdMap().get(tableName);
      TableId tid = TableId.of(tableId);

      ServerContext ctx = getCluster().getServerContext();
      FileSystem fs = getCluster().getFileSystem();

      Set<String> tablesDirs = ctx.getTablesDirs();
      assertEquals(1, tablesDirs.size());

      String tdir = tablesDirs.iterator().next() + "/" + tid.canonical() + "/default_tablet";
      Path defaultTabletPath = new Path(tdir);
      assertTrue(fs.exists(defaultTabletPath));

      assertEquals(0, FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical()).size());

      Set<Path> generatedPaths = generateTmpFilePaths(ctx, tid, defaultTabletPath, 100);

      for (Path p : generatedPaths) {
        assertFalse(fs.exists(p));
        assertTrue(fs.createNewFile(p));
        assertTrue(fs.exists(p));
      }

      Set<Path> foundPaths = FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical());
      assertEquals(100, foundPaths.size());
      assertEquals(foundPaths, generatedPaths);

      System.setProperty("accumulo.properties",
          "file://" + getCluster().getAccumuloPropertiesPath());
      FindCompactionTmpFiles.main(new String[] {"--tables", tableName});

      foundPaths = FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical());
      assertEquals(100, foundPaths.size());
      assertEquals(foundPaths, generatedPaths);

    }
  }

  @Test
  public void testFindCompactionTmpFilesMainWithDelete() throws Exception {

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      ReadWriteIT.ingest(c, 100, 1, 1, 0, tableName);
      c.tableOperations().flush(tableName);

      String tableId = c.tableOperations().tableIdMap().get(tableName);
      TableId tid = TableId.of(tableId);

      ServerContext ctx = getCluster().getServerContext();
      FileSystem fs = getCluster().getFileSystem();

      Set<String> tablesDirs = ctx.getTablesDirs();
      assertEquals(1, tablesDirs.size());

      String tdir = tablesDirs.iterator().next() + "/" + tid.canonical() + "/default_tablet";
      Path defaultTabletPath = new Path(tdir);
      assertTrue(fs.exists(defaultTabletPath));

      assertEquals(0, FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical()).size());

      Set<Path> generatedPaths = generateTmpFilePaths(ctx, tid, defaultTabletPath, 100);

      for (Path p : generatedPaths) {
        assertFalse(fs.exists(p));
        assertTrue(fs.createNewFile(p));
        assertTrue(fs.exists(p));
      }

      Set<Path> foundPaths = FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical());
      assertEquals(100, foundPaths.size());
      assertEquals(foundPaths, generatedPaths);

      System.setProperty("accumulo.properties",
          "file://" + getCluster().getAccumuloPropertiesPath());
      FindCompactionTmpFiles.main(new String[] {"--tables", tableName, "--delete"});

      foundPaths = FindCompactionTmpFiles.findTempFiles(ctx, tid.canonical());
      assertEquals(0, foundPaths.size());

    }
  }

}

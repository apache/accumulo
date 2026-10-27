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
package org.apache.accumulo.test.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.manager.upgrade.Upgrader11to12;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.FindCompactionTmpFiles.DeleteStats;
import org.apache.accumulo.test.harness.SharedMiniClusterBase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TmpFileUpgrade11to12TestIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(TmpFileUpgrade11to12TestIT.class);

  @BeforeAll
  public static void start() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void stop() throws Exception {
    stopMiniCluster();
  }

  @Test
  public void testDeleteOldCompactionTmpFiles() throws Exception {

    final Set<String> fileNames =
        Set.of("A00001.rf", "F000002.rf", "F000003.rf_tmp", "C000004.rf", "C000005.rf_tmp",
            "C000006.rf_tmp_" + ExternalCompactionId.generate(UUID.randomUUID()), "A000007.rf_tmp");

    final Set<String> validFileNames = new HashSet<>(fileNames);
    validFileNames.removeAll(Set.of("C000005.rf_tmp", "A000007.rf_tmp"));

    final ServerContext ctx = getCluster().getServerContext();
    String[] tableNames = getUniqueNames(10);

    final Set<String> volumePaths = new HashSet<>();
    ctx.getVolumeManager().getVolumes().forEach(v -> volumePaths.add(v.getBasePath()));

    SortedSet<Text> splits = new TreeSet<>();
    IntStream.range(0, 9).forEach(i -> splits.add(new Text("" + i)));

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      for (String tableName : tableNames) {
        client.tableOperations().create(tableName);
        client.tableOperations().addSplits(tableName, splits);
      }

      final Map<String,String> tableNameIdMap = ctx.tableOperations().tableIdMap();
      final Map<TableId,Set<String>> tableNameDirsMap = new HashMap<>();
      final AtomicInteger numTabletDirs = new AtomicInteger(0);
      for (Entry<String,String> e : tableNameIdMap.entrySet()) {
        final TableId tid = TableId.of(e.getValue());
        if (SystemTables.containsTableId(tid)) {
          continue;
        }
        try (TabletsMetadata tm =
            ctx.getAmple().readTablets().forTable(tid).fetch(ColumnType.DIR).build()) {
          Set<String> tableDirs = new HashSet<>();
          tm.forEach(t -> {
            numTabletDirs.incrementAndGet();
            tableDirs.add(t.getDirName());
          });
          tableNameDirsMap.put(tid, tableDirs);
        }
      }

      final Set<Path> createdPaths = new HashSet<>();
      for (Entry<TableId,Set<String>> e : tableNameDirsMap.entrySet()) {
        if (SystemTables.containsTableId(e.getKey())) {
          continue;
        }
        for (String tabletDirName : e.getValue()) {
          String tabletDirPath = Constants.HDFS_TABLES_DIR + Path.SEPARATOR + e.getKey()
              + Path.SEPARATOR + tabletDirName;
          for (String fileName : fileNames) {
            for (String volume : volumePaths) {
              Path p =
                  new Path(volume + Path.SEPARATOR + tabletDirPath + Path.SEPARATOR + fileName);
              assertTrue(ctx.getVolumeManager().createNewFile(p));
              createdPaths.add(p);
            }
          }
        }
      }
      LOG.info("Created {} files", createdPaths.size());
      LOG.info("{}", createdPaths);

      Upgrader11to12 upgrader = new Upgrader11to12();
      DeleteStats stats = new DeleteStats();
      Set<Path> deleted = new HashSet<Path>();
      upgrader.deleteCompactionTempFiles(ctx, stats, deleted);
      assertEquals(numTabletDirs.get() * 2, deleted.size());
      assertEquals(0, stats.error);
      assertEquals(0, stats.failure);
      assertEquals(deleted.size(), stats.success);

      for (Path delete : deleted) {
        assertFalse(validFileNames.contains(delete.getName()));
      }
    }
  }

}

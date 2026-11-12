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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

/** Exercises tablet files stored outside Accumulo's configured table directories. */
public class ExternalTabletFileIT extends ConfigurableMacBase {

  private static final Duration GC_WAIT = Duration.ofSeconds(45);
  private static final long GC_POLL_MS = 100;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.MANAGER_TABLE_DELETE_OPTIMIZATION, "true");
    super.configure(cfg, hadoopCoreSite);
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Test
  public void scanAndCompactExternalFile() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      FileSystem fs = getCluster().getMiniDfs().getFileSystem();
      String table = getUniqueNames(1)[0];
      client.tableOperations().create(table);

      Path externalFile = writeRFile(fs, "scan-compact");
      addFileReference(client, table, externalFile, fs.getFileStatus(externalFile).getLen(), 2);

      assertRows(client, table);

      client.tableOperations().compact(table, new CompactionConfig().setWait(true));

      assertRows(client, table);
      assertTrue(hasConfiguredVolumeFile(client, table));
      Wait.waitFor(() -> !fs.exists(externalFile), GC_WAIT.toMillis(), GC_POLL_MS,
          "GC did not remove compacted external RFile " + externalFile);
    }
  }

  @Test
  public void optimizedTableDeleteGcExternalFileAfterLastReference() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      FileSystem fs = getCluster().getMiniDfs().getFileSystem();
      String[] tables = getUniqueNames(2);
      client.tableOperations().create(tables[0]);
      client.tableOperations().create(tables[1]);

      Path externalFile = writeRFile(fs, "delete-shared");
      long fileSize = fs.getFileStatus(externalFile).getLen();
      addFileReference(client, tables[0], externalFile, fileSize, 2);
      addFileReference(client, tables[1], externalFile, fileSize, 2);
      assertRows(client, tables[0]);
      assertRows(client, tables[1]);

      getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);
      client.tableOperations().delete(tables[0]);

      assertTrue(fs.exists(externalFile), "Deleting one table must preserve the shared file");
      assertTrue(hasGcCandidate(externalFile),
          "Optimized deletion must enqueue an external-file GC candidate");

      getCluster().getClusterControl().start(ServerType.GARBAGE_COLLECTOR);
      Wait.waitFor(() -> !hasGcCandidate(externalFile), GC_WAIT.toMillis(), GC_POLL_MS,
          "GC did not clear the in-use external-file candidate");
      assertTrue(fs.exists(externalFile), "GC deleted a file still referenced by another table");
      assertRows(client, tables[1]);

      getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);
      client.tableOperations().delete(tables[1]);
      assertTrue(hasGcCandidate(externalFile),
          "Deleting the last referencing table must enqueue a GC candidate");

      getCluster().getClusterControl().start(ServerType.GARBAGE_COLLECTOR);
      Wait.waitFor(() -> !fs.exists(externalFile), GC_WAIT.toMillis(), GC_POLL_MS,
          "GC did not delete the external RFile after its last reference was removed");
    }
  }

  private Path writeRFile(FileSystem fs, String name) throws Exception {
    Path externalFile = fs.makeQualified(new Path("/external-rfiles/" + name + ".rf"));
    fs.mkdirs(externalFile.getParent());
    try (RFileWriter writer =
        RFile.newWriter().to(externalFile.toString()).withFileSystem(fs).build()) {
      writer.startDefaultLocalityGroup();
      writer.append(new Key("r1", "cf", "cq"), new Value("v1"));
      writer.append(new Key("r2", "cf", "cq"), new Value("v2"));
    }
    return externalFile;
  }

  private void addFileReference(AccumuloClient client, String table, Path file, long size,
      long entries) throws Exception {
    client.tableOperations().offline(table, true);
    TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(table));
    getCluster().getServerContext().getAmple().mutateTablet(new KeyExtent(tableId, null, null))
        .putFile(StoredTabletFile.of(file), new DataFileValue(size, entries)).mutate();
    client.tableOperations().online(table, true);
  }

  private void assertRows(AccumuloClient client, String table) throws Exception {
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      int count = 0;
      for (var entry : scanner) {
        String row = entry.getKey().getRow().toString();
        assertEquals("r" + (count + 1), row);
        assertEquals("v" + (count + 1), entry.getValue().toString());
        count++;
      }
      assertEquals(2, count);
    }
  }

  private boolean hasConfiguredVolumeFile(AccumuloClient client, String table) throws Exception {
    TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(table));
    try (var tablets = getCluster().getServerContext().getAmple().readTablets().forTable(tableId)
        .fetch(TabletMetadata.ColumnType.FILES).build()) {
      for (var tablet : tablets) {
        for (StoredTabletFile file : tablet.getFilesMap().keySet()) {
          if (file.getPath().toString().contains("/external-rfiles/")) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private boolean hasGcCandidate(Path path) {
    var candidates =
        getCluster().getServerContext().getAmple().getGcCandidates(Ample.DataLevel.USER);
    while (candidates.hasNext()) {
      GcCandidate candidate = candidates.next();
      if (new Path(candidate.getPath()).equals(path)) {
        return true;
      }
    }
    return false;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Tests that files are archived instead of deleted when configured.
 */
public class FileArchiveIT extends ConfigurableMacBase {

  @Override
  public int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    cfg.setProperty(Property.GC_FILE_ARCHIVE, "true");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
  }

  @Test
  public void testUnusuedFilesAreArchived() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    final Table.ID tableId = Table.ID.of(conn.tableOperations().tableIdMap().get(tableName));
    Assert.assertNotNull("Could not get table ID", tableId);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("", "", "value");
    bw.addMutation(m);
    bw.close();

    // Compact memory to disk
    conn.tableOperations().compact(tableName, null, null, true, true);

    try (Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      final String file = entry.getKey().getColumnQualifier().toString();
      final Path p = new Path(file);

      // Then force another to make an unreferenced file
      conn.tableOperations().compact(tableName, null, null, true, true);

      log.info("File for table: {}", file);

      FileSystem fs = getCluster().getFileSystem();
      int i = 0;
      while (fs.exists(p)) {
        i++;
        Thread.sleep(1000);
        if (0 == i % 10) {
          log.info("Waited {} iterations, file still exists", i);
        }
      }

      log.info("File was removed");

      String filePath = p.toUri().getPath().substring(getCluster().getConfig().getAccumuloDir().toString().length());

      log.info("File relative to accumulo dir: {}", filePath);

      Path fileArchiveDir = new Path(getCluster().getConfig().getAccumuloDir().toString(), ServerConstants.FILE_ARCHIVE_DIR);

      Assert.assertTrue("File archive directory didn't exist", fs.exists(fileArchiveDir));

      // Remove the leading '/' to make sure Path treats the 2nd arg as a child.
      Path archivedFile = new Path(fileArchiveDir, filePath.substring(1));

      Assert.assertTrue("File doesn't exists in archive directory: " + archivedFile, fs.exists(archivedFile));
    }
  }

  @Test
  public void testDeletedTableIsArchived() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    final Table.ID tableId = Table.ID.of(conn.tableOperations().tableIdMap().get(tableName));
    Assert.assertNotNull("Could not get table ID", tableId);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("", "", "value");
    bw.addMutation(m);
    bw.close();

    // Compact memory to disk
    conn.tableOperations().compact(tableName, null, null, true, true);

    try (Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      final String file = entry.getKey().getColumnQualifier().toString();
      final Path p = new Path(file);

      conn.tableOperations().delete(tableName);

      log.info("File for table: {}", file);

      FileSystem fs = getCluster().getFileSystem();
      int i = 0;
      while (fs.exists(p)) {
        i++;
        Thread.sleep(1000);
        if (0 == i % 10) {
          log.info("Waited {} iterations, file still exists", i);
        }
      }

      log.info("File was removed");

      String filePath = p.toUri().getPath().substring(getCluster().getConfig().getAccumuloDir().toString().length());

      log.info("File relative to accumulo dir: {}", filePath);

      Path fileArchiveDir = new Path(getCluster().getConfig().getAccumuloDir().toString(), ServerConstants.FILE_ARCHIVE_DIR);

      Assert.assertTrue("File archive directory didn't exist", fs.exists(fileArchiveDir));

      // Remove the leading '/' to make sure Path treats the 2nd arg as a child.
      Path archivedFile = new Path(fileArchiveDir, filePath.substring(1));

      Assert.assertTrue("File doesn't exists in archive directory: " + archivedFile, fs.exists(archivedFile));
    }
  }

  @Test
  public void testUnusuedFilesAndDeletedTable() throws Exception {
    final Connector conn = getConnector();
    final String tableName = getUniqueNames(1)[0];

    conn.tableOperations().create(tableName);

    final Table.ID tableId = Table.ID.of(conn.tableOperations().tableIdMap().get(tableName));
    Assert.assertNotNull("Could not get table ID", tableId);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("", "", "value");
    bw.addMutation(m);
    bw.close();

    // Compact memory to disk
    conn.tableOperations().compact(tableName, null, null, true, true);

    Entry<Key,Value> entry;
    Path fileArchiveDir;
    FileSystem fs;
    int i = 0;
    try (Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

      entry = Iterables.getOnlyElement(s);
      final String file = entry.getKey().getColumnQualifier().toString();
      final Path p = new Path(file);

      // Then force another to make an unreferenced file
      conn.tableOperations().compact(tableName, null, null, true, true);

      log.info("File for table: {}", file);

      fs = getCluster().getFileSystem();
      while (fs.exists(p)) {
        i++;
        Thread.sleep(1000);
        if (0 == i % 10) {
          log.info("Waited {} iterations, file still exists", i);
        }
      }

      log.info("File was removed");

      String filePath = p.toUri().getPath().substring(getCluster().getConfig().getAccumuloDir().toString().length());

      log.info("File relative to accumulo dir: {}", filePath);

      fileArchiveDir = new Path(getCluster().getConfig().getAccumuloDir().toString(), ServerConstants.FILE_ARCHIVE_DIR);

      Assert.assertTrue("File archive directory didn't exist", fs.exists(fileArchiveDir));

      // Remove the leading '/' to make sure Path treats the 2nd arg as a child.
      Path archivedFile = new Path(fileArchiveDir, filePath.substring(1));

      Assert.assertTrue("File doesn't exists in archive directory: " + archivedFile, fs.exists(archivedFile));

      // Offline the table so we can be sure there is a single file
      conn.tableOperations().offline(tableName, true);
    }

    // See that the file in metadata currently is
    try (Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.TabletsSection.getRange(tableId));
      s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

      entry = Iterables.getOnlyElement(s);
      final String finalFile = entry.getKey().getColumnQualifier().toString();
      final Path finalPath = new Path(finalFile);

      conn.tableOperations().delete(tableName);

      log.info("File for table: {}", finalPath);

      i = 0;
      while (fs.exists(finalPath)) {
        i++;
        Thread.sleep(1000);
        if (0 == i % 10) {
          log.info("Waited {} iterations, file still exists", i);
        }
      }

      log.info("File was removed");

      String finalFilePath = finalPath.toUri().getPath().substring(getCluster().getConfig().getAccumuloDir().toString().length());

      log.info("File relative to accumulo dir: {}", finalFilePath);

      Assert.assertTrue("File archive directory didn't exist", fs.exists(fileArchiveDir));

      // Remove the leading '/' to make sure Path treats the 2nd arg as a child.
      Path finalArchivedFile = new Path(fileArchiveDir, finalFilePath.substring(1));

      Assert.assertTrue("File doesn't exists in archive directory: " + finalArchivedFile, fs.exists(finalArchivedFile));
    }
  }
}

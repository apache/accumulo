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
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These test check that Accumulo handles misnormalized files in the metadata table correctly. If
 * Accumulo code reads a misnormalized file from the metadata table, normalizes it, and then tries
 * to update the metadata table then the key will not match. The mismatch could result in duplicate
 * entries.
 */
public class FileNormalizationIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(FileNormalizationIT.class);

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testSplits() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];

      client.tableOperations().create(table);
      VerifyIngest.VerifyParams params =
          new VerifyIngest.VerifyParams(getClientProps(), table, 100_000);
      TestIngest.ingest(client, params);

      client.tableOperations().flush(table, null, null, true);

      misnormalizeFiles(client, table);

      var splits = TestIngest.getSplitPoints(params.startRow, params.startRow + params.rows, 2);
      assertEquals(1, splits.size());

      client.tableOperations().addSplits(table, splits);

      HashSet<String> paths = new HashSet<>();

      VerifyIngest.verifyIngest(client, params);

      try (var scanner = createMetadataFileScanner(client, table)) {
        scanner.forEach((k, v) -> {
          var row = k.getRowData().toString();
          var qual = k.getColumnQualifierData().toString();
          var path = StoredTabletFile.of(k.getColumnQualifierData().toString()).getMetadataPath();
          var rowPath = row + "+" + path;

          log.debug("split test, inspecting {} {} {}", row, qual, v);

          assertFalse(paths.contains(rowPath),
              "Tablet " + row + " has duplicate normalized path " + path);
          paths.add(rowPath);
        });
      }

      VerifyIngest.verifyIngest(client, params);
    }
  }

  @Test
  public void testCompaction() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];

      client.tableOperations().create(table);
      VerifyIngest.VerifyParams params =
          new VerifyIngest.VerifyParams(getClientProps(), table, 100_000);
      TestIngest.ingest(client, params);

      client.tableOperations().flush(table, null, null, true);

      misnormalizeFiles(client, table);

      client.tableOperations().compact(table, new CompactionConfig().setWait(true));

      try (var scanner = createMetadataFileScanner(client, table)) {
        Set<String> filenames = new HashSet<>();

        scanner.forEach((k, v) -> {
          var path = StoredTabletFile.of(k.getColumnQualifierData().toString()).getPath();
          assertFalse(filenames.contains(path.getName()));
          assertTrue(path.getName().startsWith("A"));
          filenames.add(path.getName());
        });

        assertEquals(1, filenames.size());

        VerifyIngest.verifyIngest(client, params);
      }
    }
  }

  @Test
  public void testMerge() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];

      // disable compactions
      Map<String,String> props = Map.of(Property.TABLE_MAJC_RATIO.getKey(), "10");
      client.tableOperations().create(table, new NewTableConfiguration().setProperties(props));
      VerifyIngest.VerifyParams params =
          new VerifyIngest.VerifyParams(getClientProps(), table, 100_000);
      TestIngest.ingest(client, params);

      client.tableOperations().flush(table, null, null, true);

      var splits = TestIngest.getSplitPoints(params.startRow, params.startRow + params.rows, 4);
      assertEquals(3, splits.size());

      client.tableOperations().addSplits(table, splits);

      // create a unique file per tablet
      client.tableOperations().compact(table, new CompactionConfig().setWait(true));

      misnormalizeFiles(client, table);

      Set<String> filesBeforeMerge = new HashSet<>();
      try (var scanner = createMetadataFileScanner(client, table)) {
        scanner.forEach((k, v) -> {
          var qual = k.getColumnQualifierData().toString();
          assertTrue(qual.contains("//tables//"));
          filesBeforeMerge.add(StoredTabletFile.of(qual).getMetadataPath());
        });
      }

      // expect 4 files. one for each tablet
      assertEquals(4, filesBeforeMerge.size());

      client.tableOperations().merge(table, null, null);

      Set<String> filesAfterMerge = new HashSet<>();
      try (var scanner = createMetadataFileScanner(client, table)) {
        scanner.forEach((k, v) -> {
          // should only see the default tablet
          assertTrue(k.getRow().toString().endsWith("<"));
          filesAfterMerge.add(StoredTabletFile.of(k.getColumnQualifier()).getMetadataPath());
        });
      }

      assertEquals(0, client.tableOperations().listSplits(table).size());
      assertEquals(filesBeforeMerge, filesAfterMerge);

      VerifyIngest.verifyIngest(client, params);
    }
  }

  private Scanner createMetadataFileScanner(AccumuloClient client, String table) throws Exception {
    var scanner = client.createScanner(AccumuloTable.METADATA.tableName());
    var tableId = TableId.of(client.tableOperations().tableIdMap().get(table));
    var range = new KeyExtent(tableId, null, null).toMetaRange();
    scanner.setRange(range);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    return scanner;
  }

  private void misnormalizeFiles(AccumuloClient client, String table) throws Exception {
    client.tableOperations().offline(table, true);

    client.securityOperations().grantTablePermission(getPrincipal(),
        AccumuloTable.METADATA.tableName(), TablePermission.WRITE);

    try (var scanner = createMetadataFileScanner(client, table);
        var writer = client.createBatchWriter(AccumuloTable.METADATA.tableName())) {
      scanner.forEach((k, v) -> {
        Mutation m = new Mutation(k.getRow());
        var qual = k.getColumnQualifierData().toString();
        assertTrue(qual.contains("/tables/"));
        var newQual = new Text(qual.replace("/tables/", "//tables//"));
        m.put(k.getColumnFamily(), newQual, v);
        m.putDelete(k.getColumnFamily(), k.getColumnQualifier());
        try {
          writer.addMutation(m);
        } catch (MutationsRejectedException e) {
          throw new RuntimeException(e);
        }
      });
    } finally {
      client.securityOperations().revokeTablePermission(getPrincipal(),
          AccumuloTable.METADATA.tableName(), TablePermission.WRITE);
      client.tableOperations().online(table, true);
    }
  }
}

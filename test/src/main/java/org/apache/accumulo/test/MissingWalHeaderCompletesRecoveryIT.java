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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class MissingWalHeaderCompletesRecoveryIT extends ConfigurableMacBase {
  private static final Logger log =
      LoggerFactory.getLogger(MissingWalHeaderCompletesRecoveryIT.class);

  private boolean rootHasWritePermission;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration conf) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.MANAGER_RECOVERY_DELAY, "1s");
    // Make sure the GC doesn't delete the file before the metadata reference is added
    cfg.setProperty(Property.GC_CYCLE_START, "999999s");
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @BeforeEach
  public void setupMetadataPermission() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      rootHasWritePermission = client.securityOperations().hasTablePermission("root",
          MetadataTable.NAME, TablePermission.WRITE);
      if (!rootHasWritePermission) {
        client.securityOperations().grantTablePermission("root", MetadataTable.NAME,
            TablePermission.WRITE);
        // Make sure it propagates through ZK
        Thread.sleep(5000);
      }
    }
  }

  @AfterEach
  public void resetMetadataPermission() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // Final state doesn't match the original
      if (rootHasWritePermission != client.securityOperations().hasTablePermission("root",
          MetadataTable.NAME, TablePermission.WRITE)) {
        if (rootHasWritePermission) {
          // root had write permission when starting, ensure root still does
          client.securityOperations().grantTablePermission("root", MetadataTable.NAME,
              TablePermission.WRITE);
        } else {
          // root did not have write permission when starting, ensure that it does not
          client.securityOperations().revokeTablePermission("root", MetadataTable.NAME,
              TablePermission.WRITE);
        }
      }
    }
  }

  @Test
  public void testEmptyWalRecoveryCompletes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      MiniAccumuloClusterImpl cluster = getCluster();
      FileSystem fs = cluster.getFileSystem();

      // Fake out something that looks like host:port, it's irrelevant
      String fakeServer = "127.0.0.1:12345";

      File walogs = new File(cluster.getConfig().getAccumuloDir(), Constants.WAL_DIR);
      File walogServerDir = new File(walogs, fakeServer.replace(':', '+'));
      File emptyWalog = new File(walogServerDir, UUID.randomUUID().toString());

      log.info("Created empty WAL at {}", emptyWalog.toURI());

      fs.create(new Path(emptyWalog.toURI())).close();

      assertTrue(client.securityOperations().hasTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE), "root user did not have write permission to metadata table");

      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      assertNotNull(tableId, "Table ID was null");

      LogEntry logEntry =
          new LogEntry(new KeyExtent(tableId, null, null), 0, emptyWalog.toURI().toString());

      log.info("Taking {} offline", tableName);
      client.tableOperations().offline(tableName, true);

      log.info("{} is offline", tableName);

      Text row = TabletsSection.encodeRow(tableId, null);
      Mutation m = new Mutation(row);
      m.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());

      try (BatchWriter bw = client.createBatchWriter(MetadataTable.NAME)) {
        bw.addMutation(m);
      }

      log.info("Bringing {} online", tableName);
      client.tableOperations().online(tableName, true);

      log.info("{} is online", tableName);

      // Reading the table implies that recovery completed successfully (the empty file was ignored)
      // otherwise the tablet will never come online and we won't be able to read it.
      try (Scanner s = client.createScanner(tableName, Authorizations.EMPTY)) {
        assertEquals(0, Iterables.size(s));
      }
    }
  }

  @Test
  public void testPartialHeaderWalRecoveryCompletes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      MiniAccumuloClusterImpl cluster = getCluster();
      FileSystem fs = getCluster().getFileSystem();

      // Fake out something that looks like host:port, it's irrelevant
      String fakeServer = "127.0.0.1:12345";

      File walogs = new File(cluster.getConfig().getAccumuloDir(), Constants.WAL_DIR);
      File walogServerDir = new File(walogs, fakeServer.replace(':', '+'));
      File partialHeaderWalog = new File(walogServerDir, UUID.randomUUID().toString());

      log.info("Created WAL with malformed header at {}", partialHeaderWalog.toURI());

      // Write half of the header
      FSDataOutputStream wal = fs.create(new Path(partialHeaderWalog.toURI()));
      wal.write(DfsLogger.LOG_FILE_HEADER_V4.getBytes(UTF_8), 0,
          DfsLogger.LOG_FILE_HEADER_V4.length() / 2);
      wal.close();

      assertTrue(client.securityOperations().hasTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE), "root user did not have write permission to metadata table");

      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      assertNotNull(tableId, "Table ID was null");

      LogEntry logEntry = new LogEntry(null, 0, partialHeaderWalog.toURI().toString());

      log.info("Taking {} offline", tableName);
      client.tableOperations().offline(tableName, true);

      log.info("{} is offline", tableName);

      Text row = TabletsSection.encodeRow(tableId, null);
      Mutation m = new Mutation(row);
      m.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());

      try (BatchWriter bw = client.createBatchWriter(MetadataTable.NAME)) {
        bw.addMutation(m);
      }

      log.info("Bringing {} online", tableName);
      client.tableOperations().online(tableName, true);

      log.info("{} is online", tableName);

      // Reading the table implies that recovery completed successfully (the empty file was ignored)
      // otherwise the tablet will never come online and we won't be able to read it.
      try (Scanner s = client.createScanner(tableName, Authorizations.EMPTY)) {
        assertEquals(0, Iterables.size(s));
      }
    }
  }

}

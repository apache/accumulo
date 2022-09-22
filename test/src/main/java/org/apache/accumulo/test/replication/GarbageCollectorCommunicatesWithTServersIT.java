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
package org.apache.accumulo.test.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ACCUMULO-3302 series of tests which ensure that a WAL is prematurely closed when a TServer may
 * still continue to use it. Checking that no tablet references a WAL is insufficient to determine
 * if a WAL will never be used in the future.
 */
@Disabled("Replication ITs are not stable and not currently maintained")
@Deprecated
public class GarbageCollectorCommunicatesWithTServersIT extends ConfigurableMacBase {
  private static final Logger log =
      LoggerFactory.getLogger(GarbageCollectorCommunicatesWithTServersIT.class);

  private final int GC_PERIOD_SECONDS = 1;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    cfg.setNumTservers(1);
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, GC_PERIOD_SECONDS + "s");
    // Wait longer to try to let the replication table come online before a cycle runs
    cfg.setProperty(Property.GC_CYCLE_START, "10s");
    cfg.setProperty(Property.REPLICATION_NAME, "manager");
    // Set really long delays for the manager to do stuff for replication. We don't need
    // it to be doing anything, so just let it sleep
    cfg.setProperty(Property.REPLICATION_WORK_PROCESSOR_DELAY, "240s");
    cfg.setProperty(Property.MANAGER_REPLICATION_SCAN_INTERVAL, "240s");
    cfg.setProperty(Property.REPLICATION_DRIVER_DELAY, "240s");
    // Pull down the maximum size of the wal so we can test close()'ing it.
    cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, "1M");
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  /**
   * Fetch all of the WALs referenced by tablets in the metadata table for this table
   */
  private Set<String> getWalsForTable(String tableName) throws Exception {
    final ServerContext context = getServerContext();
    final String tableId = context.tableOperations().tableIdMap().get(tableName);

    assertNotNull(tableId, "Could not determine table ID for " + tableName);

    WalStateManager wals = new WalStateManager(context);

    Set<String> result = new HashSet<>();
    for (Entry<Path,WalState> entry : wals.getAllState().entrySet()) {
      log.debug("Reading WALs: {}={}", entry.getKey(), entry.getValue());
      result.add(entry.getKey().toString());
    }
    return result;
  }

  /**
   * Fetch all of the rfiles referenced by tablets in the metadata table for this table
   */
  private Set<String> getFilesForTable(String tableName) throws Exception {
    final AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();
    final TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

    assertNotNull(tableId, "Could not determine table ID for " + tableName);

    Set<String> rfiles = new HashSet<>();
    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      Range r = TabletsSection.getRange(tableId);
      s.setRange(r);
      s.fetchColumnFamily(DataFileColumnFamily.NAME);

      for (Entry<Key,Value> entry : s) {
        log.debug("Reading RFiles: {}={}", entry.getKey().toStringNoTruncate(), entry.getValue());
        // uri://path/to/wal
        String cq = entry.getKey().getColumnQualifier().toString();
        String path = new Path(cq).toString();
        log.debug("Normalize path to rfile: {}", path);
        rfiles.add(path);
      }
    }
    return rfiles;
  }

  /**
   * Get the replication status messages for the given table that exist in the metadata table (~repl
   * entries)
   */
  private Map<String,Status> getMetadataStatusForTable(String tableName) throws Exception {
    final AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();
    final String tableId = client.tableOperations().tableIdMap().get(tableName);

    assertNotNull(tableId, "Could not determine table ID for " + tableName);

    Map<String,Status> fileToStatus = new HashMap<>();
    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      Range r = ReplicationSection.getRange();
      s.setRange(r);
      s.fetchColumn(ReplicationSection.COLF, new Text(tableId));

      for (Entry<Key,Value> entry : s) {
        Text file = new Text();
        ReplicationSection.getFile(entry.getKey(), file);
        Status status = Status.parseFrom(entry.getValue().get());
        log.info("Got status for {}: {}", file, ProtobufUtil.toString(status));
        fileToStatus.put(file.toString(), status);
      }
    }
    return fileToStatus;
  }

  @Test
  public void testActiveWalPrecludesClosing() throws Exception {
    final String table = getUniqueNames(1)[0];
    final AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();

    // Bring the replication table online first and foremost
    ReplicationTable.setOnline(client);

    log.info("Creating {}", table);
    client.tableOperations().create(table);

    client.tableOperations().setProperty(table, Property.TABLE_REPLICATION.getKey(), "true");

    log.info("Writing a few mutations to the table");

    try (BatchWriter bw = client.createBatchWriter(table)) {
      byte[] empty = new byte[0];
      for (int i = 0; i < 5; i++) {
        Mutation m = new Mutation(Integer.toString(i));
        m.put(empty, empty, empty);
        bw.addMutation(m);
      }
    }

    log.info(
        "Checking that metadata only has two WALs recorded for this table (inUse, and opened)");

    Set<String> wals = getWalsForTable(table);
    assertEquals(2, wals.size(), "Expected to only find two WALs for the table");

    // Flush our test table to remove the WAL references in it
    client.tableOperations().flush(table, null, null, true);
    // Flush the metadata table too because it will have a reference to the WAL
    client.tableOperations().flush(MetadataTable.NAME, null, null, true);

    log.info("Waiting for replication table to come online");

    log.info("Fetching replication statuses from metadata table");

    Map<String,Status> fileToStatus = getMetadataStatusForTable(table);

    assertEquals(1, fileToStatus.size(), "Expected to only find one replication status message");

    String walName = fileToStatus.keySet().iterator().next();
    wals.retainAll(fileToStatus.keySet());
    assertEquals(1, wals.size());

    Status status = fileToStatus.get(walName);

    assertFalse(status.getClosed(), "Expected Status for file to not be closed");

    Set<String> filesForTable = getFilesForTable(table);
    assertEquals(1, filesForTable.size(), "Expected to only find one rfile for table");
    log.info("Files for table before MajC: {}", filesForTable);

    // Issue a MajC to roll a new file in HDFS
    client.tableOperations().compact(table, null, null, false, true);

    Set<String> filesForTableAfterCompaction = getFilesForTable(table);

    log.info("Files for table after MajC: {}", filesForTableAfterCompaction);

    assertEquals(1, filesForTableAfterCompaction.size(),
        "Expected to only find one rfile for table");
    assertNotEquals(filesForTableAfterCompaction, filesForTable,
        "Expected the files before and after compaction to differ");

    // Use the rfile which was just replaced by the MajC to determine when the GC has ran
    Path fileToBeDeleted = new Path(filesForTable.iterator().next());
    FileSystem fs = getCluster().getFileSystem();

    boolean fileExists = fs.exists(fileToBeDeleted);
    while (fileExists) {
      log.info("File which should get deleted still exists: {}", fileToBeDeleted);
      Thread.sleep(2000);
      fileExists = fs.exists(fileToBeDeleted);
    }

    Map<String,Status> fileToStatusAfterMinc = getMetadataStatusForTable(table);
    assertEquals(1, fileToStatusAfterMinc.size(),
        "Expected to still find only one replication status message: " + fileToStatusAfterMinc);

    assertEquals(fileToStatus, fileToStatusAfterMinc,
        "Status before and after MinC should be identical");
  }

  @Test
  public void testUnreferencedWalInTserverIsClosed() throws Exception {
    final String[] names = getUniqueNames(2);
    // `table` will be replicated, `otherTable` is only used to roll the WAL on the tserver
    final String table = names[0], otherTable = names[1];
    final AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();

    // Bring the replication table online first and foremost
    ReplicationTable.setOnline(client);

    log.info("Creating {}", table);
    client.tableOperations().create(table);

    client.tableOperations().setProperty(table, Property.TABLE_REPLICATION.getKey(), "true");

    log.info("Writing a few mutations to the table");

    byte[] empty = new byte[0];
    try (BatchWriter bw = client.createBatchWriter(table)) {
      for (int i = 0; i < 5; i++) {
        Mutation m = new Mutation(Integer.toString(i));
        m.put(empty, empty, empty);
        bw.addMutation(m);
      }
      log.info("Flushing mutations to the server");
    }

    log.info("Checking that metadata only has one WAL recorded for this table");

    Set<String> wals = getWalsForTable(table);
    assertEquals(2, wals.size(), "Expected to only find two WAL for the table");

    log.info("Compacting the table which will remove all WALs from the tablets");

    // Flush our test table to remove the WAL references in it
    client.tableOperations().flush(table, null, null, true);
    // Flush the metadata table too because it will have a reference to the WAL
    client.tableOperations().flush(MetadataTable.NAME, null, null, true);

    log.info("Fetching replication statuses from metadata table");

    Map<String,Status> fileToStatus = getMetadataStatusForTable(table);

    assertEquals(1, fileToStatus.size(), "Expected to only find one replication status message");

    String walName = fileToStatus.keySet().iterator().next();
    assertTrue(wals.contains(walName),
        "Expected log file name from tablet to equal replication entry");

    Status status = fileToStatus.get(walName);

    assertFalse(status.getClosed(), "Expected Status for file to not be closed");

    Set<String> filesForTable = getFilesForTable(table);
    assertEquals(1, filesForTable.size(), "Expected to only find one rfile for table");
    log.info("Files for table before MajC: {}", filesForTable);

    // Issue a MajC to roll a new file in HDFS
    client.tableOperations().compact(table, null, null, false, true);

    Set<String> filesForTableAfterCompaction = getFilesForTable(table);

    log.info("Files for table after MajC: {}", filesForTableAfterCompaction);

    assertEquals(1, filesForTableAfterCompaction.size(),
        "Expected to only find one rfile for table");
    assertNotEquals(filesForTableAfterCompaction, filesForTable,
        "Expected the files before and after compaction to differ");

    // Use the rfile which was just replaced by the MajC to determine when the GC has ran
    Path fileToBeDeleted = new Path(filesForTable.iterator().next());
    FileSystem fs = getCluster().getFileSystem();

    boolean fileExists = fs.exists(fileToBeDeleted);
    while (fileExists) {
      log.info("File which should get deleted still exists: {}", fileToBeDeleted);
      Thread.sleep(2000);
      fileExists = fs.exists(fileToBeDeleted);
    }

    // At this point in time, we *know* that the GarbageCollector has run which means that the
    // Status
    // for our WAL should not be altered.

    Map<String,Status> fileToStatusAfterMinc = getMetadataStatusForTable(table);
    assertEquals(1, fileToStatusAfterMinc.size(),
        "Expected to still find only one replication status message: " + fileToStatusAfterMinc);

    /*
     * To verify that the WALs is still getting closed, we have to force the tserver to close the
     * existing WAL and open a new one instead. The easiest way to do this is to write a load of
     * data that will exceed the 1.33% full threshold that the logger keeps track of
     */

    client.tableOperations().create(otherTable);
    try (BatchWriter bw = client.createBatchWriter(otherTable)) {
      // 500k
      byte[] bigValue = new byte[1024 * 500];
      Arrays.fill(bigValue, (byte) 1);
      // 500k * 50
      for (int i = 0; i < 50; i++) {
        Mutation m = new Mutation(Integer.toString(i));
        m.put(empty, empty, bigValue);
        bw.addMutation(m);
        if (i % 10 == 0) {
          bw.flush();
        }
      }
    }

    client.tableOperations().flush(otherTable, null, null, true);

    // Get the tservers which the manager deems as active
    final ClientContext context = (ClientContext) client;
    List<String> tservers = ThriftClientTypes.MANAGER.execute(context,
        mgr -> mgr.getActiveTservers(TraceUtil.traceInfo(), context.rpcCreds()));

    assertEquals(1, tservers.size(), "Expected only one active tservers");

    HostAndPort tserver = HostAndPort.fromString(tservers.get(0));

    // Get the active WALs from that server
    log.info("Fetching active WALs from {}", tserver);

    Client cli = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, tserver, context);
    List<String> activeWalsForTserver =
        cli.getActiveLogs(TraceUtil.traceInfo(), context.rpcCreds());

    log.info("Active wals: {}", activeWalsForTserver);

    assertEquals(1, activeWalsForTserver.size(), "Expected to find only one active WAL");

    String activeWal = new Path(activeWalsForTserver.get(0)).toString();

    assertNotEquals("Current active WAL on tserver should not be the original WAL we saw", walName,
        activeWal);

    log.info("Ensuring that replication status does get closed after WAL is no"
        + " longer in use by Tserver");

    do {
      Map<String,Status> replicationStatuses = getMetadataStatusForTable(table);

      log.info("Got replication status messages {}", replicationStatuses);
      assertEquals(1, replicationStatuses.size(),
          "Did not expect to find additional status records");

      status = replicationStatuses.values().iterator().next();
      log.info("Current status: {}", ProtobufUtil.toString(status));

      if (status.getClosed()) {
        return;
      }

      log.info("Status is not yet closed, waiting for garbage collector to close it");

      Thread.sleep(2000);
    } while (true);
  }
}

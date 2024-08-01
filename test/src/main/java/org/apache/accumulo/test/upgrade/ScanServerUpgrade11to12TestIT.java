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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.upgrade.Upgrader11to12;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerUpgrade11to12TestIT extends SharedMiniClusterBase {

  public static final Logger log = LoggerFactory.getLogger(ScanServerUpgrade11to12TestIT.class);
  private static final Range META_RANGE =
      new Range(AccumuloTable.SCAN_REF.tableId() + ";", AccumuloTable.SCAN_REF.tableId() + "<");

  private static class ScanServerUpgradeITConfiguration
      implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(0);
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    ScanServerUpgradeITConfiguration c = new ScanServerUpgradeITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
  }

  @AfterAll
  public static void stop() throws Exception {
    stopMiniCluster();
  }

  private Stream<Entry<Key,Value>> getOldScanServerRefs(String tableName) {
    try {
      BatchScanner scanner =
          getCluster().getServerContext().createBatchScanner(tableName, Authorizations.EMPTY);
      scanner.setRanges(Upgrader11to12.OLD_SCAN_SERVERS_RANGES);
      return scanner.stream().onClose(scanner::close);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException("Unable to find table " + tableName);
    }
  }

  private void deleteScanServerRefTable() throws InterruptedException {
    ServerContext ctx = getCluster().getServerContext();
    // Remove the scan server table metadata in zk
    try {
      ctx.getTableManager().removeTable(AccumuloTable.SCAN_REF.tableId());
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Removal of scan ref table failed" + e);
    }

    // Read from the metadata table to find any existing scan ref tablets and remove them
    try (BatchWriter writer = ctx.createBatchWriter(AccumuloTable.METADATA.tableName())) {
      var refTablet = checkForScanRefTablets().iterator();
      while (refTablet.hasNext()) {
        var entry = refTablet.next();
        log.info("Entry:  {}", entry);
        var mutation = new Mutation(entry.getKey().getRow());
        mutation.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
        writer.addMutation(mutation);
      }
      writer.flush();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      throw new RuntimeException(e);
    }

    // Compact the metadata table to remove the tablet file for the scan ref table
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().compact(AccumuloTable.METADATA.tableName(), null, null, true, true);
    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
      log.error("Failed to compact metadata table");
      throw new RuntimeException(e);
    }

    log.info("Scan ref table is deleted, now shutting down the system");
    try {
      getCluster().getClusterControl().stop(ServerType.MANAGER);
      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
    } catch (IOException e) {
      log.info("Failed to stop cluster");
    }
    Thread.sleep(60_000);
  }

  private void testMetadataScanServerRefRemoval(String tableName) {

    HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
    UUID serverLockUUID = UUID.randomUUID();

    Set<ScanServerRefTabletFile> scanRefs = Stream.of("F0000070.rf", "F0000071.rf", "F0000072.rf")
        .map(f -> "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/" + f)
        .map(f -> new ScanServerRefTabletFile(serverLockUUID, server.toString(), f))
        .collect(Collectors.toSet());

    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().removeProperty(tableName,
          Property.TABLE_CONSTRAINT_PREFIX.getKey() + "1");
      log.info("Removed constraints from table {}", tableName);
      Thread.sleep(10_000);
    } catch (AccumuloException | AccumuloSecurityException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    try (BatchWriter writer = ctx.createBatchWriter(tableName)) {
      for (ScanServerRefTabletFile ref : scanRefs) {
        Mutation sservMutation = new Mutation("~sserv" + ref.getFilePath());
        sservMutation.put(ref.getServerAddress(), new Text(ref.getServerLockUUID().toString()),
            new Value(""));
        writer.addMutation(sservMutation);

        Mutation scanRefMutation = new Mutation("~scanref" + ref.getServerLockUUID().toString());
        scanRefMutation.put(ref.getServerAddress(), ref.getFilePath(), new Value(""));
        writer.addMutation(scanRefMutation);
      }
      writer.flush();
    } catch (TableNotFoundException | MutationsRejectedException e) {
      log.warn("Failed to write mutations to metadata table");
      throw new RuntimeException(e);
    }

    // Check that ample cannot find these scan server refs
    assertEquals(0, ctx.getAmple().scanServerRefs().list().count());

    // Ensure they exist on the metadata table
    assertEquals(scanRefs.size() * 2L, getOldScanServerRefs(tableName).count());

    var upgrader = new Upgrader11to12();
    upgrader.removeScanServerRange(ctx, tableName);

    // Ensure entries are now removed from the metadata table
    assertEquals(0, getOldScanServerRefs(tableName).count());
  }

  private Stream<Entry<Key,Value>> checkForScanRefTablets() {
    try {
      Scanner scanner = getCluster().getServerContext()
          .createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY);
      scanner.setRange(META_RANGE);
      return scanner.stream().onClose(scanner::close);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testScanRefTableCreation() throws InterruptedException {
    ServerContext ctx = getCluster().getServerContext();
    deleteScanServerRefTable();
    log.info("Attempt to start the system");
    try {
      getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().start(ServerType.MANAGER);
      Thread.sleep(10_000);
    } catch (IOException e) {
      log.info("Failed to start cluster");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    log.info("Attempting creation of the scan ref table");
    var upgrader = new Upgrader11to12();
    upgrader.createScanServerRefTable(ctx);
    assertEquals(TableState.ONLINE,
        ctx.getTableManager().getTableState(AccumuloTable.SCAN_REF.tableId()));

    while (checkForScanRefTablets().count() < 4) {
      log.info("Waiting for the table to be hosted");
      Thread.sleep(1_000);
    }

    log.info("Reading entries from the metadata table");
    try (Scanner scanner = getCluster().getServerContext()
        .createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
      var refTablet = scanner.stream().iterator();
      while (refTablet.hasNext()) {
        log.info("Metadata Entry: {}", refTablet.next());
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    log.info("Reading entries from the root table");
    try (Scanner scanner = getCluster().getServerContext()
        .createScanner(AccumuloTable.ROOT.tableName(), Authorizations.EMPTY)) {
      var refTablet = scanner.stream().iterator();
      while (refTablet.hasNext()) {
        log.info("Root Entry: {}", refTablet.next());
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    // Create some scanRefs to test table functionality
    assertEquals(0, ctx.getAmple().scanServerRefs().list().count());
    HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
    UUID serverLockUUID = UUID.randomUUID();

    Set<ScanServerRefTabletFile> scanRefs = Stream.of("F0000070.rf", "F0000071.rf")
        .map(f -> "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/" + f)
        .map(f -> new ScanServerRefTabletFile(f, server.toString(), serverLockUUID))
        .collect(Collectors.toSet());
    ctx.getAmple().scanServerRefs().put(scanRefs);
    assertEquals(2, ctx.getAmple().scanServerRefs().list().count());
  }

  @Test
  public void testMetadataScanServerRefs() {
    testMetadataScanServerRefRemoval(Ample.DataLevel.USER.metaTable());
  }

}

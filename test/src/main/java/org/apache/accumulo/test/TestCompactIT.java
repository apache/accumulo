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

import static org.apache.accumulo.test.functional.FunctionalTestUtils.getStoredTabletFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

public class TestCompactIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(TestCompactIT.class);
  private static final String SLOW_ITER_NAME = "CustomSlowIter";
  private AccumuloClient client;
  private TableOperations ops;
  private String userTable;

  public static class IsolatedCompactorsConfig implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // define a compaction service for the user table that will create the FATE op

      Map<String,String> siteConfig = cfg.getSiteConfig();

      // Configure a compaction service for the user table
      siteConfig.put(Property.COMPACTION_SERVICE_PREFIX.getKey() + "user.planner",
          SimpleCompactionDispatcher.class.getName());
      // Configure a compaction service for system tables
      siteConfig.put(Property.COMPACTION_SERVICE_PREFIX.getKey() + "system.planner",
          SimpleCompactionDispatcher.class.getName());

      // Configure the dispatcher to map services to resource groups
      siteConfig.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service.user",
          "user_compactors");
      siteConfig.put(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service.system",
          "system_compactors");

      // Start two compactors and assign them to the two resource groups at startup
      siteConfig.put(Property.COMPACTOR_PREFIX.getKey() + "user.group", "user_compactors");
      siteConfig.put(Property.COMPACTOR_PREFIX.getKey() + "system.group", "system_compactors");
      // cfg.setSiteConfig(siteConfig);

      cfg.getClusterServerConfiguration().setNumDefaultCompactors(3);
    }
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new IsolatedCompactorsConfig());
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    client = Accumulo.newClient().from(getClientProps()).build();
    ops = client.tableOperations();
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);

    // Configure the user table to use the 'user' compaction service
    // ops.setProperty(userTable, Property.TABLE_COMPACTION_DISPATCHER.getKey(),
    // SimpleCompactionDispatcher.class.getName());
    // ops.setProperty(userTable, Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service",
    // "user");
    //
    // // Configure system tables to use the 'system' compaction service
    // for (String table : SystemTables.tableNames()) {
    // ops.setProperty(table, Property.TABLE_COMPACTION_DISPATCHER.getKey(),
    // SimpleCompactionDispatcher.class.getName());
    // ops.setProperty(table, Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service",
    // "system");
    // }
  }

  @AfterEach
  public void afterEach() throws Exception {
    if (userTable != null) {
      cleanupFateTable();
      ops.delete(userTable);
      userTable = null;
    }
    cleanupScanRefTable();
    client.close();
  }

  @Test
  public void test_compact() throws Exception {
    // disable the GC to prevent removal of newly appeared compilations files due to compaction on
    // METADATA and ROOT tables
    getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);
    try {
      // create some RFiles for the METADATA and ROOT tables by creating some data in the user
      // table, flushing that table, then the METADATA table, then the ROOT table

      client.tableOperations().flush(SystemTables.METADATA.tableName(), null, null, true);
      client.tableOperations().flush(SystemTables.ROOT.tableName(), null, null, true);
      for (int i = 0; i < 3; i++) {
        try (var bw = client.createBatchWriter(userTable)) {
          var mut = new Mutation("r" + i);
          mut.put("cf", "cq", "v");
          bw.addMutation(mut);
        }
        ops.flush(userTable, null, null, true);
        ops.flush(SystemTables.METADATA.tableName(), null, null, true);
        ops.flush(SystemTables.ROOT.tableName(), null, null, true);
      }

      // Create a file for the scan ref table
      createScanRefTableRow();
      ops.flush(SystemTables.SCAN_REF.tableName(), null, null, true);

      createFateTableRow(userTable);
      ops.flush(SystemTables.FATE.tableName(), null, null, true);
      // ops.delete(userTable);

      // var sdf = List.of(SystemTables.FATE.tableName(),
      // SystemTables.SCAN_REF.tableName());

      for (var sysTable : SystemTables.tableNames()) {
        List<StoredTabletFile> stfsBeforeCompact = getStoredTabletFiles(client, sysTable);
        log.info("Before compacting {}: {}", sysTable, stfsBeforeCompact);

        log.info("Compacting " + sysTable);
        // This compaction will run in the 'system_compactors' group.
        // Set wait to false to avoid blocking on the FATE transaction, which can be slow.
        ops.compact(sysTable, null, null, true, false);
        log.info("Initiated compaction for " + sysTable);

        // RFiles resulting from a compaction begin with 'A'. Wait until we see an RFile beginning
        // with 'A' that was not present before the compaction.
        Wait.waitFor(() -> {
          var stfsAfterCompact = getStoredTabletFiles(client, sysTable);
          log.info("after compacting {} {}", sysTable, stfsAfterCompact);
          String regex = "^A.*\\.rf$";
          var aStfsBeforeCompaction = stfsBeforeCompact.stream()
              .filter(stf -> stf.getFileName().matches(regex)).collect(Collectors.toSet());
          log.info("A files before compaction: " + aStfsBeforeCompaction);
          var aStfsAfterCompaction = stfsAfterCompact.stream()
              .filter(stf -> stf.getFileName().matches(regex)).collect(Collectors.toSet());
          log.info("A files after compaction: " + aStfsAfterCompaction);
          return !Sets.difference(aStfsAfterCompaction, aStfsBeforeCompaction).isEmpty();
        }, TimeUnit.SECONDS.toMillis(90), TimeUnit.SECONDS.toMillis(2));
      }
    } finally {
      getCluster().getClusterControl().startAllServers(ServerType.GARBAGE_COLLECTOR);
    }
  }

  private Text createScanRefTableRow() {
    HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
    UUID serverLockUUID = UUID.randomUUID();
    Set<ScanServerRefTabletFile> scanRefs = Stream.of("F0000070.rf", "F0000071.rf")
        .map(f -> "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/" + f)
        .map(f -> new ScanServerRefTabletFile(f, server.toString(), serverLockUUID))
        .collect(Collectors.toSet());
    getCluster().getServerContext().getAmple().scanServerRefs().put(scanRefs);
    return new Text(getCluster().getServerContext().getAmple().scanServerRefs().list()
        .filter(tf -> tf.getServerLockUUID().equals(serverLockUUID)).findFirst().orElseThrow()
        .getServerLockUUID().toString());
  }

  private Text createFateTableRow(String table) throws Exception {
    attachSlowMajcIterator(table);
    ReadWriteIT.ingest(client, 5, 5, 5, 0, table);

    Set<Text> rowsSeenBeforeNewOp = new HashSet<>();
    try (var scanner = client.createScanner(SystemTables.FATE.tableName())) {
      for (var entry : scanner) {
        rowsSeenBeforeNewOp.add(entry.getKey().getRow());
      }
    }

    // start a very slow compaction to create a FATE op that will linger in the FATE table until
    // cancelled
    ops.compact(table, null, null, true, false);

    Set<Text> rowsSeenAfterNewOp = new HashSet<>();
    try (var scanner = client.createScanner(SystemTables.FATE.tableName())) {
      for (var entry : scanner) {
        rowsSeenAfterNewOp.add(entry.getKey().getRow());
      }
    }

    var newOp = Sets.difference(rowsSeenAfterNewOp, rowsSeenBeforeNewOp);
    assertEquals(1, newOp.size());
    return newOp.stream().findFirst().orElseThrow();
  }

  private void attachSlowMajcIterator(String table) throws Exception {
    if (!ops.listIterators(table).containsKey(SLOW_ITER_NAME)) {
      IteratorSetting is = new IteratorSetting(1, SLOW_ITER_NAME, SlowIterator.class);
      is.addOption("sleepTime", "60000");
      ops.attachIterator(table, is, EnumSet.of(IteratorUtil.IteratorScope.majc));
    }
  }

  /**
   * Cleans up the data in the SCAN_REF table that was created by calls to
   * {@link #createScanRefTableRow()}
   */
  private void cleanupScanRefTable() throws Exception {
    var scanServerRefs = getCluster().getServerContext().getAmple().scanServerRefs();
    scanServerRefs.delete(scanServerRefs.list().collect(Collectors.toSet()));
    assertTrue(
        client.createScanner(SystemTables.SCAN_REF.tableName()).stream().findAny().isEmpty());
  }

  /**
   * Cleans up the data in the FATE table that was created by calls to
   * {@link #createFateTableRow(String)}
   */
  private void cleanupFateTable() throws Exception {
    ops.cancelCompaction(userTable);
    // Wait for FATE table to be clear
    Wait.waitFor(() -> {
      try (var scanner = client.createScanner(SystemTables.FATE.tableName())) {
        return !scanner.iterator().hasNext();
      }
    });
  }
}

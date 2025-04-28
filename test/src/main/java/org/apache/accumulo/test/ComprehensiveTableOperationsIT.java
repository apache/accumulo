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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.functional.BasicSummarizer;
import org.apache.accumulo.test.functional.BulkNewIT;
import org.apache.accumulo.test.functional.CloneTestIT;
import org.apache.accumulo.test.functional.CompactionIT;
import org.apache.accumulo.test.functional.ConstraintIT;
import org.apache.accumulo.test.functional.DeleteRowsIT;
import org.apache.accumulo.test.functional.LocalityGroupIT;
import org.apache.accumulo.test.functional.ManagerAssignmentIT;
import org.apache.accumulo.test.functional.MergeTabletsIT;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.RenameIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.accumulo.test.functional.SummaryIT;
import org.apache.accumulo.test.functional.TabletAvailabilityIT;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.Path;
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

/**
 * A comprehensive IT of all table operations against user tables and all system tables while
 * avoiding duplicating existing testing. This does not test for edge cases, but rather tests for
 * basic expected functionality of all table operations against user tables and all system tables.
 */
public class ComprehensiveTableOperationsIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(ComprehensiveTableOperationsIT.class);
  private static final String SLOW_ITER_NAME = "CustomSlowIter";
  private AccumuloClient client;
  private TableOperations ops;
  private String userTable;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @BeforeEach
  public void beforeEach() {
    client = Accumulo.newClient().from(getClientProps()).build();
    ops = client.tableOperations();
  }

  @AfterEach
  public void afterEach() throws Exception {
    // ensure none of the FATE or SCAN_REF data we created persists between tests. Also ensure the
    // user table does not persist between tests
    if (userTable != null) {
      cleanupFateTable();
      ops.delete(userTable);
      userTable = null;
    }
    cleanupScanRefTable();
    client.close();
  }

  @Test
  public void testAllTested() {
    var allTableOps =
        Arrays.stream(TableOperations.class.getDeclaredMethods()).map(Method::getName);
    var testMethodNames = Arrays.stream(ComprehensiveTableOperationsIT.class.getDeclaredMethods())
        .map(Method::getName).collect(Collectors.toSet());
    var untestedOps =
        allTableOps.filter(op -> testMethodNames.stream().noneMatch(test -> test.contains(op)))
            .collect(Collectors.toSet());
    assertTrue(untestedOps.isEmpty(), "The table operations " + untestedOps + " are untested");
  }

  @Test
  public void testExpectedSystemTables() {
    var testedSystemTableIds = Set.of(SystemTables.ROOT.tableId(), SystemTables.METADATA.tableId(),
        SystemTables.FATE.tableId(), SystemTables.SCAN_REF.tableId());
    assertEquals(testedSystemTableIds, SystemTables.tableIds(),
        "There are newly added system tables that are untested in this IT. Ensure each "
            + "test has testing for the new system table");
  }

  @Test
  public void test_list_tableIdMap() throws Exception {
    // Thoroughly tested elsewhere, but simple enough to test here. Test that all system tables
    // and user tables are returned
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    var expected = new HashSet<>(SystemTables.tableNames());
    expected.add(userTable);
    assertEquals(expected, ops.list());
    assertEquals(expected, ops.tableIdMap().keySet());
    for (Map.Entry<String,String> entry : ops.tableIdMap().entrySet()) {
      var tableName = entry.getKey();
      var tableId = TableId.of(entry.getValue());
      if (tableName.equals(SystemTables.ROOT.tableName())) {
        assertEquals(SystemTables.ROOT.tableId(), tableId);
      } else if (tableName.equals(SystemTables.METADATA.tableName())) {
        assertEquals(SystemTables.METADATA.tableId(), tableId);
      } else if (tableName.equals(SystemTables.FATE.tableName())) {
        assertEquals(SystemTables.FATE.tableId(), tableId);
      } else if (tableName.equals(SystemTables.SCAN_REF.tableName())) {
        assertEquals(SystemTables.SCAN_REF.tableId(), tableId);
      } else if (tableName.equals(userTable)) {
        assertFalse(SystemTables.tableIds().contains(tableId));
      } else {
        throw new IllegalStateException("Unrecognized table: " + tableName);
      }
    }
  }

  @Test
  public void test_exists() throws Exception {
    // Thoroughly tested elsewhere, but simple enough to test here. Test that all system tables
    // and user tables exist
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    var expected = new HashSet<>(SystemTables.tableNames());
    expected.add(userTable);
    for (String table : expected) {
      assertTrue(ops.exists(table));
    }
  }

  @Test
  public void test_create() {
    // Creating user tables is thoroughly tested. Make sure we can't create any of the already
    // existing system tables, though.
    for (var systemTable : SystemTables.tableNames()) {
      assertThrows(AccumuloException.class, () -> ops.create(systemTable));
    }
  }

  @Test
  public void test_exportTable_importTable() throws Exception {
    // exportTable, importTable for user tables is tested in ImportExportIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(ImportExportIT.class.getName()));
    // exportTable, importTable untested for system tables. Test basic functionality here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    Path baseDir = ImportExportIT.createBaseDir(getCluster(), getClass());
    var fs = getCluster().getFileSystem();

    // export the user table, so we can test importing into the system tables
    ops.offline(userTable, true);
    Path exportUserDir;
    exportUserDir = new Path(baseDir, "export_userdir");
    fs.deleteOnExit(exportUserDir);
    ops.exportTable(userTable, exportUserDir.toString());

    for (var sysTableName : SystemTables.tableNames()) {
      Path exportDir = new Path(baseDir, "export_" + sysTableName);
      fs.deleteOnExit(exportDir);

      // not offline, can't export
      assertThrows(IllegalStateException.class,
          () -> ops.exportTable(sysTableName, exportDir.toString()));
      // can't offline, so will never be able to export
      assertThrows(AccumuloException.class, () -> ops.offline(sysTableName, true));
      assertThrows(AccumuloException.class,
          () -> ops.importTable(sysTableName, exportUserDir.toString()));
    }
  }

  @Test
  public void test_addSplits_putSplits_listSplits_splitRangeByTablets() throws Exception {
    // addSplits,listSplits,putSplits tested elsewhere for METADATA, ROOT, and user tables, but
    // testing here as well since this setup is needed to test for splitRangeByTablets anyway,
    // which is untested elsewhere

    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    // system and user tables
    var allTables = ops.list();

    for (String table : allTables) {
      SortedSet<Text> splits1 = new TreeSet<>();
      splits1.add(new Text("split1"));
      SortedSet<Text> splits2 = new TreeSet<>();
      splits2.add(new Text("split2"));
      SortedMap<Text,TabletMergeability> splits2Map =
          TabletMergeabilityUtil.userDefaultSplits(splits2);

      if (table.equals(SystemTables.ROOT.tableName())) {
        // cannot add splits to ROOT
        assertThrows(AccumuloException.class, () -> ops.addSplits(table, splits1));
        assertThrows(AccumuloException.class, () -> ops.putSplits(table, splits2Map));
        assertEquals(0, ops.listSplits(table).size());
      } else {
        ops.addSplits(table, splits1);
        ops.putSplits(table, splits2Map);
        var listSplits = ops.listSplits(table);
        assertTrue(listSplits.containsAll(splits1));
        assertTrue(listSplits.containsAll(splits2));
      }

      assertEquals(ops.splitRangeByTablets(table, new Range(), 99).size(),
          ops.listSplits(table).size() + 1);
    }
  }

  @Test
  public void test_locate() throws Exception {
    // locate for user tables is tested in LocatorIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(LocatorIT.class.getName()));
    // locate for METADATA and ROOT tables is tested in ManagerAssignmentIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(ManagerAssignmentIT.class.getName()));

    // basic functionality check for locate on FATE and SCAN_REF tables
    var fateLocations =
        ops.locate(SystemTables.FATE.tableName(), Collections.singletonList(new Range()));
    var fateGroupByTablet = fateLocations.groupByTablet().keySet();
    assertFalse(fateGroupByTablet.isEmpty());
    fateGroupByTablet.forEach(tid -> {
      var tabletLoc = fateLocations.getTabletLocation(tid);
      assertNotNull(tabletLoc);
      assertTrue(tabletLoc.contains(":"));
    });
    var scanRefLocations =
        ops.locate(SystemTables.SCAN_REF.tableName(), Collections.singletonList(new Range()));
    var scanRefGroupByTablet = scanRefLocations.groupByTablet().keySet();
    assertFalse(scanRefGroupByTablet.isEmpty());
    scanRefGroupByTablet.forEach(tid -> {
      var tabletLoc = scanRefLocations.getTabletLocation(tid);
      assertNotNull(tabletLoc);
      assertTrue(tabletLoc.contains(":"));
    });
  }

  @Test
  public void test_getMaxRow_deleteRows() throws Exception {
    // getMaxRow for user tables is tested in FindMaxIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(FindMaxIT.class.getName()));
    // getMaxRow not tested for system tables. Test basic functionality here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    createFateTableRow(userTable);
    createScanRefTableRow();
    for (var sysTable : SystemTables.tableNames()) {
      var maxRow =
          ops.getMaxRow(sysTable, Authorizations.EMPTY, null, true, null, true);
      log.info("Max row of {} : {}", sysTable, maxRow);
      assertNotNull(maxRow);
    }

    // deleteRows for user tables is tested in DeleteRowsIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(DeleteRowsIT.class.getName()));
    // deleteRows not tested for system tables. Test basic functionality here
    for (var sysTable : SystemTables.tableNames()) {
      // should not be able to delete rows of any system table
      assertThrows(AccumuloException.class, () -> ops.deleteRows(sysTable, null, null));
    }
  }

  @Test
  public void test_merge() throws Exception {
    // merge for user tables is tested in various ITs. One example is MergeTabletsIT. Ensure
    // test exists
    assertDoesNotThrow(() -> Class.forName(MergeTabletsIT.class.getName()));
    // merge for METADATA and ROOT system tables tested in MetaSplitIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(MetaSplitIT.class.getName()));
    // merge for FATE and SCAN_REF tables not tested. Test basic functionality here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    var fateRow1 = createFateTableRow(userTable);
    createFateTableRow(userTable); // fate row 2
    var scanRefRow1 = createScanRefTableRow();
    createScanRefTableRow(); // scan ref row 2

    SortedSet<Text> fateSplits = new TreeSet<>();
    fateSplits.add(new Text(fateRow1));
    ops.addSplits(SystemTables.FATE.tableName(), fateSplits);
    // there may be preexisting splits on the FATE table from a previous test
    assertTrue(ops.listSplits(SystemTables.FATE.tableName()).contains(fateRow1));
    ops.merge(SystemTables.FATE.tableName(), null, null);
    assertTrue(ops.listSplits(SystemTables.FATE.tableName()).isEmpty());

    SortedSet<Text> scanRefSplits = new TreeSet<>();
    scanRefSplits.add(new Text(scanRefRow1));
    ops.addSplits(SystemTables.SCAN_REF.tableName(), scanRefSplits);
    // there may be preexisting splits on the SCAN_REF table from a previous test
    assertTrue(ops.listSplits(SystemTables.SCAN_REF.tableName()).contains(scanRefRow1));
    ops.merge(SystemTables.SCAN_REF.tableName(), null, null);
    assertTrue(ops.listSplits(SystemTables.SCAN_REF.tableName()).isEmpty());
  }

  @Test
  public void test_compact() throws Exception {
    // compact for user tables is tested in various ITs. One example is CompactionIT. Ensure
    // test exists
    assertDoesNotThrow(() -> Class.forName(CompactionIT.class.getName()));
    // disable the GC to prevent automatic compactions on METADATA and ROOT tables
    getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);
    try {
      // test basic functionality for system tables
      userTable = getUniqueNames(1)[0];
      ops.create(userTable);

      // create some RFiles for the METADATA and ROOT tables by creating some data in the user
      // table, flushing that table, then the METADATA table, then the ROOT table
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

      for (var sysTable : List.of(SystemTables.ROOT, SystemTables.METADATA, SystemTables.SCAN_REF,
          SystemTables.FATE)) {
        // create some RFiles for FATE and SCAN_REF tables
        if (sysTable == SystemTables.SCAN_REF) {
          createScanRefTableRow();
          ops.flush(SystemTables.SCAN_REF.tableName(), null, null, true);
        } else if (sysTable == SystemTables.FATE) {
          createFateTableRow(userTable);
          ops.flush(SystemTables.FATE.tableName(), null, null, true);
        }

        Set<StoredTabletFile> stfsBeforeCompact = getStoredTabFiles(sysTable);

        log.info("Compacting " + sysTable);
        ops.compact(sysTable.tableName(), null, null, true, true);
        log.info("Finished compacting " + sysTable);

        // RFiles resulting from a compaction begin with 'A'. Wait until we see an RFile beginning
        // with 'A' that was not present before the compaction.
        Wait.waitFor(() -> {
          var stfsAfterCompact = getStoredTabFiles(sysTable);
          String regex = "^A.*\\.rf$";
          var A_stfsBeforeCompaction = stfsBeforeCompact.stream()
              .filter(stf -> stf.getFileName().matches(regex)).collect(Collectors.toSet());
          var A_stfsAfterCompaction = stfsAfterCompact.stream()
              .filter(stf -> stf.getFileName().matches(regex)).collect(Collectors.toSet());
          return !Sets.difference(A_stfsAfterCompaction, A_stfsBeforeCompaction).isEmpty();
        });
      }
    } finally {
      getCluster().getClusterControl().startAllServers(ServerType.GARBAGE_COLLECTOR);
    }
  }

  @Test
  public void test_cancelCompaction() throws Exception {
    // cancelCompaction for user tables is tested in various ITs. One example is TableOperationsIT.
    // Ensure test exists
    assertDoesNotThrow(() -> Class.forName(TableOperationsIT.class.getName()));
    // test basic functionality for system tables
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);

    // Need some data in all the system tables. This allows the slow iterator we attach to the
    // system table to work when we compact. ROOT and METADATA will already have data, so just need
    // to create some data for the other system tables
    createFateTableRow(userTable);
    createScanRefTableRow();

    for (var sysTable : SystemTables.tableNames()) {
      try {
        var zrw = getCluster().getServerContext().getZooSession().asReaderWriter();
        attachSlowMajcIterator(sysTable);

        var metaFatesBeforeCompact = new HashSet<>(zrw.getChildren(Constants.ZFATE));

        log.info("Compacting " + sysTable);
        ops.compact(sysTable, null, null, true, false);
        log.info("Initiated compaction for " + sysTable);

        // Wait for the compaction to be started
        Wait.waitFor(() -> {
          var metaFatesAfterCompact = new HashSet<>(zrw.getChildren(Constants.ZFATE));
          return !Sets.difference(metaFatesAfterCompact, metaFatesBeforeCompact).isEmpty();
        });

        log.info("Cancelling compaction " + sysTable);
        ops.cancelCompaction(sysTable);

        // We can be sure that the compaction has been cancelled once we see no FATE operations
        Wait.waitFor(() -> zrw.getChildren(Constants.ZFATE).isEmpty());
      } finally {
        removeSlowMajcIterator(sysTable);
      }
    }
  }

  @Test
  public void test_delete() {
    // delete for user tables is tested in TableOperationsIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(TableOperationsIT.class.getName()));
    // delete not tested for system tables. Test basic functionality here
    for (var sysTable : SystemTables.tableNames()) {
      assertThrows(AccumuloException.class, () -> ops.delete(sysTable));
    }
  }

  @Test
  public void test_clone() {
    // cloning user and system tables is tested in CloneTestIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(CloneTestIT.class.getName()));
  }

  @Test
  public void test_rename() throws Exception {
    // rename for user tables is tested in RenameIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(RenameIT.class.getName()));
    // rename not tested for system tables. Test basic functionality here
    String newTableName = "newTableName";
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);

    for (var sysTable : SystemTables.tableNames()) {
      assertThrows(AccumuloException.class, () -> ops.rename(sysTable, newTableName));
      assertFalse(ops.exists(newTableName));
      assertThrows(AccumuloException.class, () -> ops.rename(userTable, sysTable));
    }
  }

  @Test
  public void test_flush() throws Exception {
    // flush for user tables and the METADATA and ROOT tables is tested in CompactionIT. Ensure
    // test exists
    assertDoesNotThrow(() -> Class.forName(CompactionIT.class.getName()));
    // flush for FATE and SCAN_REF not tested for. Test basic functionality here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    createFateTableRow(userTable);
    createScanRefTableRow();

    for (var sysTable : Set.of(SystemTables.FATE, SystemTables.SCAN_REF)) {
      Set<StoredTabletFile> filesBeforeFlush = new HashSet<>();

      try (TabletsMetadata tabletsMetadata = getCluster().getServerContext().getAmple()
          .readTablets().forTable(sysTable.tableId()).build()) {
        for (var tm : tabletsMetadata) {
          filesBeforeFlush.addAll(tm.getFiles());
        }
      }

      ops.flush(sysTable.tableName(), null, null, true);

      // Wait until the set of files changes
      Wait.waitFor(() -> {
        Set<StoredTabletFile> filesAfterFlush = new HashSet<>();
        try (TabletsMetadata tabletsMetadata = getCluster().getServerContext().getAmple()
            .readTablets().forTable(sysTable.tableId()).build()) {
          for (var tm : tabletsMetadata) {
            filesAfterFlush.addAll(tm.getFiles());
          }
          return !filesAfterFlush.equals(filesBeforeFlush);
        }
      });
    }
  }

  @Test
  public void
      test_setProperty_modifyProperties_removeProperty_getProperties_getTableProperties_getConfiguration()
          throws Exception {
    // These may be tested elsewhere across several tests, but simpler to test all here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    // system and user tables
    Set<String> allTables = ops.list();
    for (String tableName : allTables) {
      // setProperty
      String propKey = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "prop";
      String propVal = "val";
      String newPropVal = "newval";
      // getProperties, getTableProperties, and getConfiguration
      assertFalse(propFound(tableName, propKey, propVal));
      assertNull(ops.getTableProperties(tableName).get(propKey));
      assertNull(ops.getConfiguration(tableName).get(propKey));

      ops.setProperty(tableName, propKey, propVal);

      // getProperties, getTableProperties, and getConfiguration
      assertTrue(propFound(tableName, propKey, propVal));
      assertEquals(propVal, ops.getTableProperties(tableName).get(propKey));
      assertEquals(propVal, ops.getConfiguration(tableName).get(propKey));

      // modifyProperties
      ops.modifyProperties(tableName, properties -> properties.put(propKey, newPropVal));

      // getProperties, getTableProperties, and getConfiguration
      assertTrue(propFound(tableName, propKey, newPropVal));
      assertEquals(newPropVal, ops.getTableProperties(tableName).get(propKey));
      assertEquals(newPropVal, ops.getConfiguration(tableName).get(propKey));

      // removeProperty
      ops.removeProperty(tableName, propKey);

      // getProperties, getTableProperties, and getConfiguration
      assertFalse(propFound(tableName, propKey, newPropVal));
      assertNull(ops.getTableProperties(tableName).get(propKey));
      assertNull(ops.getConfiguration(tableName).get(propKey));
    }
  }

  @Test
  public void test_setLocalityGroups_getLocalityGroups() throws Exception {
    // setLocalityGroups, getLocalityGroups for user tables is tested in LocalityGroupIT. Ensure
    // test exists
    assertDoesNotThrow(() -> Class.forName(LocalityGroupIT.class.getName()));
    // setLocalityGroups, getLocalityGroups for system tables not tested for. Test basic
    // functionality here
    for (var sysTable : SystemTables.tableNames()) {
      LocalityGroupIT.createAndSetLocalityGroups(client, sysTable);
      LocalityGroupIT.verifyLocalityGroupSet(client, sysTable);
    }
  }

  @Test
  public void test_importDirectory() throws Exception {
    // importDirectory for user tables is tested in BulkNewIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(BulkNewIT.class.getName()));
    // importDirectory for system tables not tested for. Test basic functionality here
    var rootPath = getCluster().getTemporaryPath().toString();
    var dir = rootPath + "/" + getUniqueNames(1)[0];
    BulkNewIT.writeData(getCluster().getFileSystem(), dir + "/f1.",
        getCluster().getServerContext().getConfiguration(), 0, 5, 0);

    for (var sysTable : SystemTables.tableNames()) {
      assertThrows(Exception.class, () -> ops.importDirectory(dir).to(sysTable).load());
    }
  }

  @Test
  public void test_offline_online_isOnline() throws Exception {
    // offline,online,isOnline for user tables is tested in ComprehensiveBaseIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(ComprehensiveBaseIT.class.getName()));
    // offline,online,isOnline not tested for system tables. Test basic functionality here
    for (var sysTable : SystemTables.tableNames()) {
      assertTrue(ops.isOnline(sysTable));
      assertThrows(AccumuloException.class, () -> ops.offline(sysTable, true));
      assertTrue(ops.isOnline(sysTable));
      ops.online(sysTable, true);
    }
  }

  @Test
  public void test_clearLocatorCache() throws Exception {
    // clearLocatorCache is tested elsewhere for user tables but not for system tables. Simple
    // enough to test all tables here
    var clientContext = (ClientContext) client;
    userTable = getUniqueNames(1)[0];

    ops.create(userTable);
    ReadWriteIT.ingest(clientContext, 5, 5, 5, 0, userTable);
    assertTrue(clientContext.getTabletLocationCache(TableId.of(ops.tableIdMap().get(userTable)))
        .getTabletHostingRequestCount() > 0);
    ops.clearLocatorCache(userTable);
    assertEquals(0,
        clientContext.getTabletLocationCache(TableId.of(ops.tableIdMap().get(userTable)))
            .getTabletHostingRequestCount());

    for (var sysTable : SystemTables.values()) {
      assertEquals(0,
          clientContext.getTabletLocationCache(sysTable.tableId()).getTabletHostingRequestCount());
      ops.clearLocatorCache(sysTable.tableName());
      assertEquals(0,
          clientContext.getTabletLocationCache(sysTable.tableId()).getTabletHostingRequestCount());
    }
  }

  @Test
  public void
      test_attachIterator_removeIterator_getIteratorSetting_listIterators_checkIteratorConflicts()
          throws Exception {
    // These may be tested elsewhere across several tests, but simpler to test all here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    String iterName = "iter_name";
    var iterSetting = new IteratorSetting(100, iterName, NoOpFilter.class);
    var scope = EnumSet.of(IteratorUtil.IteratorScope.majc, IteratorUtil.IteratorScope.minc);
    // system and user tables
    Set<String> allTables = ops.list();

    for (String table : allTables) {
      ops.attachIterator(table, iterSetting, scope);
      try {
        assertTrue(ops.listIterators(table).containsKey(iterName));
        assertNull(ops.getIteratorSetting(table, iterName, IteratorUtil.IteratorScope.scan));
        assertEquals(iterSetting,
            ops.getIteratorSetting(table, iterName, IteratorUtil.IteratorScope.majc));
        assertThrows(AccumuloException.class,
            () -> ops.checkIteratorConflicts(table, iterSetting, scope));
        ops.checkIteratorConflicts(table, iterSetting, EnumSet.of(IteratorUtil.IteratorScope.scan));
      } finally {
        ops.removeIterator(table, iterName, scope);
        assertFalse(ops.listIterators(table).containsKey(iterName));
      }
    }
  }

  @Test
  public void test_addConstraint_listConstraints_removeConstraint() throws Exception {
    // addConstraint, listConstraints, removeConstraint for user tables is tested in ConstraintIT.
    // Ensure test exists
    assertDoesNotThrow(() -> Class.forName(ConstraintIT.class.getName()));
    // addConstraint, listConstraints, removeConstraint not tested for system tables. Test basic
    // functionality here
    for (var sysTable : SystemTables.tableNames()) {
      var numExistingConstraints = ops.listConstraints(sysTable).size();
      String constraint = ComprehensiveBaseIT.TestConstraint.class.getName();

      var constraintNum = ops.addConstraint(sysTable, constraint);

      var listConstraints = ops.listConstraints(sysTable);
      assertEquals(numExistingConstraints + 1, listConstraints.size());
      assertEquals(constraintNum, listConstraints.get(constraint));

      ops.removeConstraint(sysTable, constraintNum);

      listConstraints = ops.listConstraints(sysTable);
      assertEquals(numExistingConstraints, listConstraints.size());
      assertNull(listConstraints.get(constraint));
    }
  }

  @Test
  public void test_getDiskUsage() throws Exception {
    // getDiskUsage for user tables is tested in TableOperationsIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(TableOperationsIT.class.getName()));
    // getDiskUsage not tested for system tables. Test basic functionality here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    createFateTableRow(userTable);
    createScanRefTableRow();
    for (var sysTable : SystemTables.tableNames()) {
      ops.flush(sysTable, null, null, true);
      var diskUsageList = ops.getDiskUsage(Set.of(sysTable));
      assertEquals(1, diskUsageList.size());
      var diskUsage = diskUsageList.get(0);
      log.info("table : {}, disk usage : {}", sysTable, diskUsage.getUsage());
      assertTrue(diskUsage.getUsage() > 0);
    }
  }

  @Test
  public void test_testClassLoad() throws Exception {
    // testClassLoad is untested. Test basic functionality here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    // system and user tables
    Set<String> allTables = ops.list();
    for (var table : allTables) {
      // VersioningIterator is a default iterator
      assertTrue(ops.testClassLoad(table, VersioningIterator.class.getName(),
          SortedKeyValueIterator.class.getName()));
      assertFalse(ops.testClassLoad(table, "foo", SortedKeyValueIterator.class.getName()));
    }
  }

  @Test
  public void test_setSamplerConfiguration_getSamplerConfiguration_clearSamplerConfiguration()
      throws Exception {
    // these are mostly untested. Test basic functionality here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);
    // system and user tables
    Set<String> allTables = ops.list();
    for (var table : allTables) {
      ops.setSamplerConfiguration(table, SampleIT.SC1);
      assertEquals(SampleIT.SC1, ops.getSamplerConfiguration(table));
      ops.clearSamplerConfiguration(table);
      assertNull(ops.getSamplerConfiguration(table));
    }
  }

  @Test
  public void test_summaries_addSummarizers_removeSummarizers_listSummarizers() throws Exception {
    // these are all tested for user tables in SummaryIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(SummaryIT.class.getName()));
    // these are not tested for system tables. Test basic functionality here
    for (var sysTable : SystemTables.tableNames()) {
      SummarizerConfiguration sc = SummarizerConfiguration.builder(BasicSummarizer.class).build();

      assertThrows(AccumuloSecurityException.class, () -> ops.summaries(sysTable).retrieve());
      ops.addSummarizers(sysTable, sc);
      assertEquals(List.of(sc), ops.listSummarizers(sysTable));
      ops.removeSummarizers(sysTable, sc1 -> true);
      assertTrue(ops.listSummarizers(sysTable).isEmpty());
    }
  }

  @Test
  public void test_getTimeType() throws Exception {
    // getTimeType is tested elsewhere but not for all system tables. Simple enough to test all
    // tables here
    userTable = getUniqueNames(1)[0];
    ops.create(userTable);

    assertEquals(TimeType.MILLIS, ops.getTimeType(userTable));
    for (var sysTable : SystemTables.tableNames()) {
      assertEquals(TimeType.LOGICAL, ops.getTimeType(sysTable));
    }
  }

  @Test
  public void test_setTabletAvailability_getTabletInformation() throws Exception {
    // these are tested for user tables in TabletAvailabilityIT. Ensure test exists
    assertDoesNotThrow(() -> Class.forName(TabletAvailabilityIT.class.getName()));
    // these are not tested for system tables. Test basic functionality here
    for (var sysTable : SystemTables.tableNames()) {
      // should not be able to unhost any system table
      assertThrows(IllegalArgumentException.class,
          () -> ops.setTabletAvailability(sysTable, new Range(), TabletAvailability.UNHOSTED));
      assertTrue(ops.getTabletInformation(sysTable, new Range()).findAny().isPresent());
      ops.getTabletInformation(sysTable, new Range())
          .forEach(ti -> assertEquals(TabletAvailability.HOSTED, ti.getTabletAvailability()));
    }
  }

  /**
   * Creates some data in the FATE table. Will create a single row each call. Does so by initiating
   * a very slow compaction on the given table.
   *
   * @param table the table the fate operation will operate on
   * @return the row created
   */
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

  private void removeSlowMajcIterator(String table) throws Exception {
    ops.removeIterator(table, SLOW_ITER_NAME, EnumSet.of(IteratorUtil.IteratorScope.majc));
  }

  /**
   * Creates some data in the SCAN_REF table. Will create a single row each call.
   *
   * @return the row created
   */
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

  private boolean propFound(String tableName, String key, String val) throws Exception {
    var propsIter = ops.getProperties(tableName).iterator();
    boolean propFound = false;
    while (propsIter.hasNext()) {
      var prop = propsIter.next();
      if (prop.getKey().equals(key) && prop.getValue().equals(val)) {
        propFound = true;
        break;
      }
    }
    return propFound;
  }

  private Set<StoredTabletFile> getStoredTabFiles(SystemTables sysTable) {
    var clientContext = (ClientContext) client;
    Set<StoredTabletFile> storedTabFiles = new HashSet<>();
    try (TabletsMetadata tabletsMetadata =
        clientContext.getAmple().readTablets().forTable(sysTable.tableId()).build()) {
      for (var tm : tabletsMetadata) {
        var stfs = tm.getFiles();
        storedTabFiles.addAll(stfs);
        stfs.forEach(f -> log.info("Found stored tablet file: {} for the system table: {}",
            f.getMetadataPath(), sysTable.tableName()));
      }
    }
    return storedTabFiles;
  }

  private static class NoOpFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      return true;
    }
  }
}

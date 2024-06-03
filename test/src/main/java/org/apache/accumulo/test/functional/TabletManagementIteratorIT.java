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

import static org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction.BAD_STATE;
import static org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction.NEEDS_LOCATION_UPDATE;
import static org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction.NEEDS_RECOVERY;
import static org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction.NEEDS_SPLITTING;
import static org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction.NEEDS_VOLUME_REPLACEMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.state.TabletManagementIterator;
import org.apache.accumulo.server.manager.state.TabletManagementParameters;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Test to ensure that the {@link TabletManagementIterator} properly skips over tablet information
 * in the metadata table when there is no work to be done on the tablet (see ACCUMULO-3580)
 */
public class TabletManagementIteratorIT extends AccumuloClusterHarness {
  private final static Logger log = LoggerFactory.getLogger(TabletManagementIteratorIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Test
  public void test() throws AccumuloException, AccumuloSecurityException, TableExistsException,
      TableNotFoundException, IOException {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tables = getUniqueNames(10);
      final String t1 = tables[0];
      final String t2 = tables[1];
      final String t3 = tables[2];
      final String t4 = tables[3];
      final String metaCopy1 = tables[4];
      final String metaCopy2 = tables[5];
      final String metaCopy3 = tables[6];
      final String metaCopy4 = tables[7];
      final String metaCopy5 = tables[8];
      final String metaCopy6 = tables[9];

      // create some metadata
      createTable(client, t1, true);
      createTable(client, t2, false);
      createTable(client, t3, true);
      createTable(client, t4, true);

      // Scan table t3 which will cause it's tablets
      // to be hosted. Then, remove the location.
      Scanner s = client.createScanner(t3);
      s.setRange(new Range());
      @SuppressWarnings("unused")
      var unused = Iterables.size(s); // consume all the data

      // examine a clone of the metadata table, so we can manipulate it
      copyTable(client, AccumuloTable.METADATA.tableName(), metaCopy1);

      var tableId1 = getServerContext().getTableId(t1);
      var tableId3 = getServerContext().getTableId(t3);
      var tableId4 = getServerContext().getTableId(t4);

      // Create expected KeyExtents to test output of findTabletsNeedingAttention
      KeyExtent endR1 = new KeyExtent(tableId1, new Text("some split"), null);
      KeyExtent endR3 = new KeyExtent(tableId3, new Text("some split"), null);
      KeyExtent endR4 = new KeyExtent(tableId4, new Text("some split"), null);
      KeyExtent prevR1 = new KeyExtent(tableId1, null, new Text("some split"));
      KeyExtent prevR3 = new KeyExtent(tableId3, null, new Text("some split"));
      KeyExtent prevR4 = new KeyExtent(tableId4, null, new Text("some split"));
      Map<KeyExtent,Set<TabletManagement.ManagementAction>> expected;

      TabletManagementParameters tabletMgmtParams = createParameters(client);
      Map<KeyExtent,Set<TabletManagement.ManagementAction>> tabletsInFlux =
          findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams);
      while (!tabletsInFlux.isEmpty()) {
        log.debug("Waiting for {} tablets for {}", tabletsInFlux, metaCopy1);
        UtilWaitThread.sleep(500);
        copyTable(client, AccumuloTable.METADATA.tableName(), metaCopy1);
        tabletsInFlux = findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams);
      }
      expected = Map.of();
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams),
          "No tables should need attention");

      // The metadata table stabilized and metaCopy1 contains a copy suitable for testing. Before
      // metaCopy1 is modified, copy it for subsequent test.
      copyTable(client, metaCopy1, metaCopy2);
      copyTable(client, metaCopy1, metaCopy3);
      copyTable(client, metaCopy1, metaCopy4);
      copyTable(client, metaCopy1, metaCopy5);
      copyTable(client, metaCopy1, metaCopy6);

      // t1 is unassigned, setting to always will generate a change to host tablets
      setTabletAvailability(client, metaCopy1, t1, TabletAvailability.HOSTED.name());
      // t3 is hosted, setting to never will generate a change to unhost tablets
      setTabletAvailability(client, metaCopy1, t3, TabletAvailability.UNHOSTED.name());
      tabletMgmtParams = createParameters(client);
      expected = Map.of(endR1, Set.of(NEEDS_LOCATION_UPDATE), prevR1, Set.of(NEEDS_LOCATION_UPDATE),
          endR3, Set.of(NEEDS_LOCATION_UPDATE), prevR3, Set.of(NEEDS_LOCATION_UPDATE));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams),
          "Should have four tablets with hosting availability changes");

      // test continue scan functionality, this test needs a table and tablet mgmt params that will
      // return more than one tablet
      testContinueScan(client, metaCopy1, tabletMgmtParams);

      // test the assigned case (no location)
      removeLocation(client, metaCopy1, t3);
      expected =
          Map.of(endR1, Set.of(NEEDS_LOCATION_UPDATE), prevR1, Set.of(NEEDS_LOCATION_UPDATE));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams),
          "Should have two tablets without a loc");

      // Test setting the operation id on one of the tablets in table t1. Table t1 has two tablets
      // w/o a location. Only one should need attention because of the operation id.
      setOperationId(client, metaCopy1, t1, new Text("some split"), TabletOperationType.SPLITTING);
      expected = Map.of(prevR1, Set.of(NEEDS_LOCATION_UPDATE));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams),
          "Should have tablets needing attention because of operation id");

      // test the cases where the assignment is to a dead tserver
      reassignLocation(client, metaCopy2, t3);
      expected = Map.of(endR3, Set.of(NEEDS_LOCATION_UPDATE));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy2, tabletMgmtParams),
          "Only 1 of 2 tablets in table t1 should be returned");

      // Test the recovery cases
      createLogEntry(client, metaCopy5, t1);
      setTabletAvailability(client, metaCopy5, t1, TabletAvailability.UNHOSTED.name());
      expected = Map.of(endR1, Set.of(NEEDS_LOCATION_UPDATE, NEEDS_RECOVERY));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy5, tabletMgmtParams),
          "Only 1 of 2 tablets in table t1 should be returned");
      setTabletAvailability(client, metaCopy5, t1, TabletAvailability.ONDEMAND.name());
      expected = Map.of(endR1, Set.of(NEEDS_LOCATION_UPDATE, NEEDS_RECOVERY));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy5, tabletMgmtParams),
          "Only 1 of 2 tablets in table t1 should be returned");
      setTabletAvailability(client, metaCopy5, t1, TabletAvailability.HOSTED.name());
      expected = Map.of(endR1, Set.of(NEEDS_LOCATION_UPDATE, NEEDS_RECOVERY), prevR1,
          Set.of(NEEDS_LOCATION_UPDATE));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy5, tabletMgmtParams),
          "2 tablets in table t1 should be returned");

      // Remove location and set merge operation id on both tablets
      // These tablets should not need attention as they have no WALs
      setTabletAvailability(client, metaCopy4, t4, TabletAvailability.HOSTED.name());
      removeLocation(client, metaCopy4, t4);
      expected =
          Map.of(endR4, Set.of(NEEDS_LOCATION_UPDATE), prevR4, Set.of(NEEDS_LOCATION_UPDATE));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy4, tabletMgmtParams),
          "Tablets have no location and a tablet availability of hosted, so they should need attention");

      // Test MERGING and SPLITTING do not need attention with no location or wals
      setOperationId(client, metaCopy4, t4, null, TabletOperationType.MERGING);
      expected = Map.of();
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy4, tabletMgmtParams),
          "Should have no tablets needing attention for merge as they have no location");
      setOperationId(client, metaCopy4, t4, null, TabletOperationType.SPLITTING);
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy4, tabletMgmtParams),
          "Should have no tablets needing attention for merge as they have no location");

      // Create a log entry for one of the tablets, this tablet will now need attention
      // for both MERGING and SPLITTING
      setOperationId(client, metaCopy4, t4, null, TabletOperationType.MERGING);
      createLogEntry(client, metaCopy4, t4);
      expected = Map.of(endR4, Set.of(NEEDS_LOCATION_UPDATE, NEEDS_RECOVERY));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy4, tabletMgmtParams),
          "Should have a tablet needing attention because of wals");
      // Switch op to SPLITTING which should also need attention like MERGING
      setOperationId(client, metaCopy4, t4, null, TabletOperationType.SPLITTING);
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy4, tabletMgmtParams),
          "Should have a tablet needing attention because of wals");

      // Switch op to delete, no tablets should need attention even with WALs
      setOperationId(client, metaCopy4, t4, null, TabletOperationType.DELETING);
      expected = Map.of();
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy4, tabletMgmtParams),
          "Should have no tablets needing attention for delete");

      // test the bad tablet location state case (inconsistent metadata)
      tabletMgmtParams = createParameters(client);
      addDuplicateLocation(client, metaCopy3, t3);
      expected = Map.of(prevR3, Set.of(NEEDS_LOCATION_UPDATE, BAD_STATE));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy3, tabletMgmtParams),
          "Should have 1 tablet that needs a metadata repair");

      // test the volume replacements case. Need to insert some files into
      // the metadata for t4, then run the TabletManagementIterator with
      // volume replacements
      addFiles(client, metaCopy4, t4);
      Map<Path,Path> replacements =
          Map.of(new Path("file:/vol1/accumulo/inst_id"), new Path("file:/vol2/accumulo/inst_id"));
      tabletMgmtParams = createParameters(client, replacements);
      expected = Map.of(prevR4, Set.of(NEEDS_VOLUME_REPLACEMENT));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy4, tabletMgmtParams),
          "Should have one tablet that needs a volume replacement");

      // In preparation for split an offline testing ensure nothing needs attention
      tabletMgmtParams = createParameters(client);
      addFiles(client, metaCopy6, t4);
      expected = Map.of();
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy6, tabletMgmtParams),
          "No tablets should need attention");
      // Lower the split threshold for the table, should cause the files added to need attention.
      client.tableOperations().setProperty(tables[3], Property.TABLE_SPLIT_THRESHOLD.getKey(),
          "1K");
      expected = Map.of(prevR4, Set.of(NEEDS_SPLITTING));
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy6, tabletMgmtParams),
          "Should have one tablet that needs splitting");

      // Take the table offline which should prevent the tablet from being returned for needing to
      // split
      client.tableOperations().offline(tables[3], false);
      tabletMgmtParams = createParameters(client);
      expected = Map.of();
      assertEquals(expected, findTabletsNeedingAttention(client, metaCopy6, tabletMgmtParams),
          "No tablets should need attention");

      // clean up
      dropTables(client, t1, t2, t3, t4, metaCopy1, metaCopy2, metaCopy3, metaCopy4, metaCopy5,
          metaCopy6);
    }
  }

  private void setTabletAvailability(AccumuloClient client, String table, String tableNameToModify,
      String state) throws TableNotFoundException, MutationsRejectedException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));

    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(new KeyExtent(tableIdToModify, null, null).toMetaRange());
      for (Entry<Key,Value> entry : scanner) {
        Mutation m = new Mutation(entry.getKey().getRow());
        m.put(TabletColumnFamily.AVAILABILITY_COLUMN.getColumnFamily(),
            TabletColumnFamily.AVAILABILITY_COLUMN.getColumnQualifier(),
            entry.getKey().getTimestamp() + 1, new Value(state));
        try (BatchWriter bw = client.createBatchWriter(table)) {
          bw.addMutation(m);
        }
      }
    }
  }

  private void addDuplicateLocation(AccumuloClient client, String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    Mutation m = new Mutation(new KeyExtent(tableIdToModify, null, null).toMetaRow());
    m.put(CurrentLocationColumnFamily.NAME, new Text("1234567"), new Value("fake:9005"));
    try (BatchWriter bw = client.createBatchWriter(table)) {
      bw.addMutation(m);
    }
  }

  private void addFiles(AccumuloClient client, String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    Mutation m = new Mutation(new KeyExtent(tableIdToModify, null, null).toMetaRow());
    m.put(DataFileColumnFamily.NAME,
        new Text(StoredTabletFile
            .serialize("file:/vol1/accumulo/inst_id/tables/2a/default_tablet/F0000072.rf")),
        new Value(new DataFileValue(1000000, 100000, 0).encode()));
    try (BatchWriter bw = client.createBatchWriter(table)) {
      bw.addMutation(m);
    }
    try {
      client.createScanner(table).iterator()
          .forEachRemaining(e -> System.out.println(e.getKey() + "-> " + e.getValue()));
    } catch (TableNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (AccumuloSecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (AccumuloException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void reassignLocation(AccumuloClient client, String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(new KeyExtent(tableIdToModify, null, null).toMetaRange());
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
      Entry<Key,Value> entry = scanner.iterator().next();
      Mutation m = new Mutation(entry.getKey().getRow());
      m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(),
          entry.getKey().getTimestamp());
      m.put(entry.getKey().getColumnFamily(), new Text("1234567"),
          entry.getKey().getTimestamp() + 1, new Value("fake:9005"));
      try (BatchWriter bw = client.createBatchWriter(table)) {
        bw.addMutation(m);
      }
    }
  }

  // Sets an operation type on all tablets up to the end row
  private void setOperationId(AccumuloClient client, String table, String tableNameToModify,
      Text end, TabletOperationType opType) throws TableNotFoundException {
    FateInstanceType instanceType = FateInstanceType.fromNamespaceOrTableName(table);
    var opid = TabletOperationId.from(opType, FateId.from(instanceType, UUID.randomUUID()));
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    try (TabletsMetadata tabletsMetadata =
        getServerContext().getAmple().readTablets().forTable(tableIdToModify)
            .overlapping(null, end != null ? TabletsSection.encodeRow(tableIdToModify, end) : null)
            .fetch(ColumnType.PREV_ROW).build()) {
      for (TabletMetadata tabletMetadata : tabletsMetadata) {
        Mutation m = new Mutation(tabletMetadata.getExtent().toMetaRow());
        MetadataSchema.TabletsSection.ServerColumnFamily.OPID_COLUMN.put(m,
            new Value(opid.canonical()));
        try (BatchWriter bw = client.createBatchWriter(table)) {
          bw.addMutation(m);
        } catch (MutationsRejectedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void removeLocation(AccumuloClient client, String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    BatchDeleter deleter = client.createBatchDeleter(table, Authorizations.EMPTY, 1);
    deleter
        .setRanges(Collections.singleton(new KeyExtent(tableIdToModify, null, null).toMetaRange()));
    deleter.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
    deleter.delete();
    deleter.close();
  }

  private Map<KeyExtent,Set<TabletManagement.ManagementAction>> findTabletsNeedingAttention(
      AccumuloClient client, String table, TabletManagementParameters tabletMgmtParams)
      throws TableNotFoundException, IOException {
    Map<KeyExtent,Set<TabletManagement.ManagementAction>> results = new HashMap<>();
    List<KeyExtent> resultList = new ArrayList<>();
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      TabletManagementIterator.configureScanner(scanner, tabletMgmtParams);
      scanner.updateScanIteratorOption("tabletChange", "debug", "1");
      for (Entry<Key,Value> e : scanner) {
        if (e != null) {
          TabletManagement mti = TabletManagementIterator.decode(e);
          results.put(mti.getTabletMetadata().getExtent(), mti.getActions());
          log.debug("Found tablets that changed state: {}", mti.getTabletMetadata().getExtent());
          log.debug("actions : {}", mti.getActions());
          log.debug("metadata: {}", mti.getTabletMetadata());
          resultList.add(mti.getTabletMetadata().getExtent());
        }
      }
    }
    log.debug("Tablets in flux: {}", resultList);
    return results;
  }

  // Multiple places in the accumulo code will read a batch of keys and then take the last key and
  // make it non inclusive to get the next batch. This test code simulates that and ensures the
  // tablet mgmt iterator works with that.
  private void testContinueScan(AccumuloClient client, String table,
      TabletManagementParameters tabletMgmtParams) throws TableNotFoundException {
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      TabletManagementIterator.configureScanner(scanner, tabletMgmtParams);
      List<Entry<Key,Value>> entries1 = new ArrayList<>();
      scanner.forEach(e -> entries1.add(e));
      assertTrue(entries1.size() > 1);

      // Create a range that does not include the first key from the last scan.
      var range = new Range(entries1.get(0).getKey(), false, null, true);
      scanner.setRange(range);

      // Ensure the first key excluded from the scan
      List<Entry<Key,Value>> entries2 = new ArrayList<>();
      scanner.forEach(e -> entries2.add(e));
      assertEquals(entries1.size() - 1, entries2.size());
      assertEquals(entries1.get(1).getKey(), entries2.get(0).getKey());
    }
  }

  private void createTable(AccumuloClient client, String t, boolean online)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException,
      TableExistsException {
    SortedSet<Text> partitionKeys = new TreeSet<>();
    partitionKeys.add(new Text("some split"));
    NewTableConfiguration ntc = new NewTableConfiguration().withSplits(partitionKeys);
    client.tableOperations().create(t, ntc);
    client.tableOperations().online(t);
    if (!online) {
      client.tableOperations().offline(t, true);
    }
  }

  /**
   * Create a copy of the source table by first gathering all the rows of the source in a list of
   * mutations. Then create the copy of the table and apply the mutations to the copy.
   */
  private void copyTable(AccumuloClient client, String source, String copy)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException {
    try {
      dropTables(client, copy);
    } catch (TableNotFoundException ex) {
      // ignored
    }

    log.info("Gathering rows to copy {} ", source);
    List<Mutation> mutations = new ArrayList<>();

    try (Scanner scanner = client.createScanner(source, Authorizations.EMPTY)) {
      RowIterator rows = new RowIterator(new IsolatedScanner(scanner));

      while (rows.hasNext()) {
        Iterator<Entry<Key,Value>> row = rows.next();
        Mutation m = null;

        while (row.hasNext()) {
          Entry<Key,Value> entry = row.next();
          Key k = entry.getKey();

          if (m == null) {
            m = new Mutation(k.getRow());
          }
          m.put(k.getColumnFamily(), k.getColumnQualifier(), k.getColumnVisibilityParsed(),
              k.getTimestamp(), entry.getValue());
        }

        mutations.add(m);
      }
    }

    // metadata should be stable with only 9 rows (2 for each table) + 1 for the FateTable
    log.debug("Gathered {} rows to create copy {}", mutations.size(), copy);
    assertEquals(9, mutations.size(),
        "Metadata should have 8 rows (2 for each table) + one row for "
            + AccumuloTable.FATE.tableId().canonical());
    client.tableOperations().create(copy);

    try (BatchWriter writer = client.createBatchWriter(copy)) {
      for (Mutation m : mutations) {
        writer.addMutation(m);
      }
    }

    log.info("Finished creating copy " + copy);
  }

  private void dropTables(AccumuloClient client, String... tables)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    for (String t : tables) {
      client.tableOperations().delete(t);
    }
  }

  // Creates a log entry on the "some split" extent, this could be modified easily to support
  // other extents
  private void createLogEntry(AccumuloClient client, String table, String tableNameToModify)
      throws MutationsRejectedException, TableNotFoundException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    KeyExtent extent = new KeyExtent(tableIdToModify, new Text("some split"), null);
    Mutation m = new Mutation(extent.toMetaRow());
    String fileName = "file:/accumulo/wal/localhost+9997/" + UUID.randomUUID().toString();
    LogEntry logEntry = LogEntry.fromPath(fileName);
    logEntry.addToMutation(m);
    try (BatchWriter bw = client.createBatchWriter(table)) {
      bw.addMutation(m);
    }
  }

  private static TabletManagementParameters createParameters(AccumuloClient client) {
    return createParameters(client, Map.of());
  }

  private static TabletManagementParameters createParameters(AccumuloClient client,
      Map<Path,Path> replacements) {
    var context = (ClientContext) client;
    Set<TableId> onlineTables = Sets.filter(context.getTableIdToNameMap().keySet(),
        tableId -> context.getTableState(tableId) == TableState.ONLINE);

    HashSet<TServerInstance> tservers = new HashSet<>();
    for (String tserver : context.instanceOperations().getTabletServers()) {
      try {
        var zPath = ServiceLock.path(ZooUtil.getRoot(context.instanceOperations().getInstanceId())
            + Constants.ZTSERVERS + "/" + tserver);
        long sessionId = ServiceLock.getSessionId(context.getZooCache(), zPath);
        tservers.add(new TServerInstance(tserver, sessionId));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return new TabletManagementParameters(ManagerState.NORMAL,
        Map.of(
            Ample.DataLevel.ROOT, true, Ample.DataLevel.USER, true, Ample.DataLevel.METADATA, true),
        onlineTables,
        new LiveTServerSet.LiveTServersSnapshot(tservers,
            Map.of(Constants.DEFAULT_RESOURCE_GROUP_NAME, tservers)),
        Set.of(), Map.of(), Ample.DataLevel.USER, Map.of(), true, replacements,
        SteadyTime.from(10000, TimeUnit.NANOSECONDS));
  }
}

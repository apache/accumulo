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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.manager.state.TabletManagementIterator;
import org.apache.accumulo.server.manager.state.TabletManagementParameters;
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

      String[] tables = getUniqueNames(6);
      final String t1 = tables[0];
      final String t2 = tables[1];
      final String t3 = tables[2];
      final String metaCopy1 = tables[3];
      final String metaCopy2 = tables[4];
      final String metaCopy3 = tables[5];

      // create some metadata
      createTable(client, t1, true);
      createTable(client, t2, false);
      createTable(client, t3, true);

      // Scan table t3 which will cause it's tablets
      // to be hosted. Then, remove the location.
      Scanner s = client.createScanner(t3);
      s.setRange(new Range());
      @SuppressWarnings("unused")
      var unused = Iterables.size(s); // consume all the data

      // examine a clone of the metadata table, so we can manipulate it
      copyTable(client, MetadataTable.NAME, metaCopy1);

      TabletManagementParameters tabletMgmtParams = createParameters(client);
      int tabletsInFlux = findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams);
      while (tabletsInFlux > 0) {
        log.debug("Waiting for {} tablets for {}", tabletsInFlux, metaCopy1);
        UtilWaitThread.sleep(500);
        copyTable(client, MetadataTable.NAME, metaCopy1);
        tabletsInFlux = findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams);
      }
      assertEquals(0, findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams),
          "No tables should need attention");

      // The metadata table stabilized and metaCopy1 contains a copy suitable for testing. Before
      // metaCopy1 is modified, copy it for subsequent test.
      copyTable(client, metaCopy1, metaCopy2);
      copyTable(client, metaCopy1, metaCopy3);

      // t1 is unassigned, setting to always will generate a change to host tablets
      setTabletHostingGoal(client, metaCopy1, t1, TabletHostingGoal.ALWAYS.name());
      // t3 is hosted, setting to never will generate a change to unhost tablets
      setTabletHostingGoal(client, metaCopy1, t3, TabletHostingGoal.NEVER.name());
      tabletMgmtParams = createParameters(client);
      assertEquals(4, findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams),
          "Should have four tablets with hosting goal changes");

      // test the assigned case (no location)
      removeLocation(client, metaCopy1, t3);
      assertEquals(2, findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams),
          "Should have two tablets without a loc");

      // Test setting the operation id on one of the tablets in table t1. Table t1 has two tablets
      // w/o a location. Only one should need attention because of the operation id.
      setOperationId(client, metaCopy1, t1);
      assertEquals(1, findTabletsNeedingAttention(client, metaCopy1, tabletMgmtParams),
          "Should have tablets needing attention because of operation id");

      // test the cases where the assignment is to a dead tserver
      reassignLocation(client, metaCopy2, t3);
      assertEquals(1, findTabletsNeedingAttention(client, metaCopy2, tabletMgmtParams),
          "Only 1 of 2 tablets in table t1 should be returned");

      // test the bad tablet location state case (inconsistent metadata)
      tabletMgmtParams = createParameters(client);
      addDuplicateLocation(client, metaCopy3, t3);
      assertEquals(1, findTabletsNeedingAttention(client, metaCopy3, tabletMgmtParams),
          "Should have 1 tablet that needs a metadata repair");

      // clean up
      dropTables(client, t1, t2, t3, metaCopy1, metaCopy2, metaCopy3);
    }
  }

  private void setTabletHostingGoal(AccumuloClient client, String table, String tableNameToModify,
      String state) throws TableNotFoundException, MutationsRejectedException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));

    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(new KeyExtent(tableIdToModify, null, null).toMetaRange());
      for (Entry<Key,Value> entry : scanner) {
        Mutation m = new Mutation(entry.getKey().getRow());
        m.put(HostingColumnFamily.GOAL_COLUMN.getColumnFamily(),
            HostingColumnFamily.GOAL_COLUMN.getColumnQualifier(), entry.getKey().getTimestamp() + 1,
            new Value(state));
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

  private void setOperationId(AccumuloClient client, String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, 42L);
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(new KeyExtent(tableIdToModify, null, null).toMetaRange());
      Entry<Key,Value> entry = scanner.iterator().next();
      Mutation m = new Mutation(entry.getKey().getRow());
      MetadataSchema.TabletsSection.ServerColumnFamily.OPID_COLUMN.put(m,
          new Value(opid.canonical()));
      try (BatchWriter bw = client.createBatchWriter(table)) {
        bw.addMutation(m);
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

  private int findTabletsNeedingAttention(AccumuloClient client, String table,
      TabletManagementParameters tabletMgmtParams) throws TableNotFoundException, IOException {
    int results = 0;
    List<KeyExtent> resultList = new ArrayList<>();
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      TabletManagementIterator.configureScanner(scanner, tabletMgmtParams);
      scanner.updateScanIteratorOption("tabletChange", "debug", "1");
      for (Entry<Key,Value> e : scanner) {
        if (e != null) {
          TabletManagement mti = TabletManagementIterator.decode(e);
          results++;
          log.debug("Found tablets that changed state: {}", mti.getTabletMetadata().getExtent());
          resultList.add(mti.getTabletMetadata().getExtent());
        }
      }
    }
    log.debug("Tablets in flux: {}", resultList);
    return results;
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

    // metadata should be stable with only 6 rows (2 for each table)
    log.debug("Gathered {} rows to create copy {}", mutations.size(), copy);
    assertEquals(6, mutations.size(), "Metadata should have 6 rows (2 for each table)");
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

  private static TabletManagementParameters createParameters(AccumuloClient client) {
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
        Map.of(Ample.DataLevel.ROOT, true, Ample.DataLevel.USER, true, Ample.DataLevel.METADATA,
            true),
        onlineTables, tservers, Set.of(), Map.of(), Map.of(), Ample.DataLevel.USER, Map.of());
  }
}

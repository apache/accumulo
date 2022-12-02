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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.manager.state.CurrentState;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MetaDataTableScanner;
import org.apache.accumulo.server.manager.state.TabletStateChangeIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Test to ensure that the {@link TabletStateChangeIterator} properly skips over tablet information
 * in the metadata table when there is no work to be done on the tablet (see ACCUMULO-3580)
 */
public class TabletStateChangeIteratorIT extends AccumuloClusterHarness {
  private final static Logger log = LoggerFactory.getLogger(TabletStateChangeIteratorIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @Test
  public void test() throws AccumuloException, AccumuloSecurityException, TableExistsException,
      TableNotFoundException {

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

      // examine a clone of the metadata table, so we can manipulate it
      copyTable(client, MetadataTable.NAME, metaCopy1);

      State state = new State(client);
      int tabletsInFlux = findTabletsNeedingAttention(client, metaCopy1, state);
      while (tabletsInFlux > 0) {
        log.debug("Waiting for {} tablets for {}", tabletsInFlux, metaCopy1);
        UtilWaitThread.sleep(500);
        copyTable(client, MetadataTable.NAME, metaCopy1);
        tabletsInFlux = findTabletsNeedingAttention(client, metaCopy1, state);
      }
      assertEquals(0, findTabletsNeedingAttention(client, metaCopy1, state),
          "No tables should need attention");

      // The metadata table stabilized and metaCopy1 contains a copy suitable for testing. Before
      // metaCopy1 is modified, copy it for subsequent test.
      copyTable(client, metaCopy1, metaCopy2);
      copyTable(client, metaCopy1, metaCopy3);

      // test the assigned case (no location)
      removeLocation(client, metaCopy1, t3);
      assertEquals(2, findTabletsNeedingAttention(client, metaCopy1, state),
          "Should have two tablets without a loc");

      // test the cases where the assignment is to a dead tserver
      reassignLocation(client, metaCopy2, t3);
      assertEquals(1, findTabletsNeedingAttention(client, metaCopy2, state),
          "Should have one tablet that needs to be unassigned");

      // test the cases where there is ongoing merges
      state = new State(client) {
        @Override
        public Collection<MergeInfo> merges() {
          TableId tableIdToModify = TableId.of(client.tableOperations().tableIdMap().get(t3));
          return Collections.singletonList(
              new MergeInfo(new KeyExtent(tableIdToModify, null, null), MergeInfo.Operation.MERGE));
        }
      };
      assertEquals(1, findTabletsNeedingAttention(client, metaCopy2, state),
          "Should have 2 tablets that need to be chopped or unassigned");

      // test the bad tablet location state case (inconsistent metadata)
      state = new State(client);
      addDuplicateLocation(client, metaCopy3, t3);
      assertEquals(1, findTabletsNeedingAttention(client, metaCopy3, state),
          "Should have 1 tablet that needs a metadata repair");

      // clean up
      dropTables(client, t1, t2, t3, metaCopy1, metaCopy2, metaCopy3);
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

  private int findTabletsNeedingAttention(AccumuloClient client, String table, State state)
      throws TableNotFoundException {
    int results = 0;
    List<Key> resultList = new ArrayList<>();
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      MetaDataTableScanner.configureScanner(scanner, state);
      log.debug("Current state = {}", state);
      scanner.updateScanIteratorOption("tabletChange", "debug", "1");
      for (Entry<Key,Value> e : scanner) {
        if (e != null) {
          results++;
          resultList.add(e.getKey());
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
    client.tableOperations().online(t, true);
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

  private static class State implements CurrentState {

    final ClientContext context;

    State(AccumuloClient client) {
      this.context = (ClientContext) client;
    }

    private Set<TServerInstance> tservers;
    private Set<TableId> onlineTables;

    @Override
    public Set<TServerInstance> onlineTabletServers() {
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
      this.tservers = Collections.unmodifiableSet(tservers);
      return tservers;
    }

    @Override
    public Set<TableId> onlineTables() {
      Set<TableId> onlineTables = context.getTableIdToNameMap().keySet();
      this.onlineTables =
          Sets.filter(onlineTables, tableId -> context.getTableState(tableId) == TableState.ONLINE);
      return this.onlineTables;
    }

    @Override
    public Collection<MergeInfo> merges() {
      return Collections.emptySet();
    }

    @Override
    public Set<KeyExtent> migrationsSnapshot() {
      return Collections.emptySet();
    }

    @Override
    public Set<TServerInstance> shutdownServers() {
      return Collections.emptySet();
    }

    @Override
    public ManagerState getManagerState() {
      return ManagerState.NORMAL;
    }

    @Override
    public String toString() {
      return "tservers: " + tservers + " onlineTables: " + onlineTables;
    }
  }

}

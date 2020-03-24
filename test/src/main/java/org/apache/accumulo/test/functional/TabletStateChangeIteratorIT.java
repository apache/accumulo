/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletStateChangeIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Test to ensure that the {@link TabletStateChangeIterator} properly skips over tablet information
 * in the metadata table when there is no work to be done on the tablet (see ACCUMULO-3580)
 */
public class TabletStateChangeIteratorIT extends AccumuloClusterHarness {

  @Override
  public int defaultTimeoutSeconds() {
    return 3 * 60;
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
      while (findTabletsNeedingAttention(client, metaCopy1, state) > 0) {
        UtilWaitThread.sleep(500);
        copyTable(client, MetadataTable.NAME, metaCopy1);
      }
      assertEquals("No tables should need attention", 0,
          findTabletsNeedingAttention(client, metaCopy1, state));

      // The metadata table stabilized and metaCopy1 contains a copy suitable for testing. Before
      // metaCopy1 is modified, copy it for subsequent test.
      copyTable(client, metaCopy1, metaCopy2);
      copyTable(client, metaCopy1, metaCopy3);

      // test the assigned case (no location)
      removeLocation(client, metaCopy1, t3);
      assertEquals("Should have two tablets without a loc", 2,
          findTabletsNeedingAttention(client, metaCopy1, state));

      // test the cases where the assignment is to a dead tserver
      reassignLocation(client, metaCopy2, t3);
      assertEquals("Should have one tablet that needs to be unassigned", 1,
          findTabletsNeedingAttention(client, metaCopy2, state));

      // test the cases where there is ongoing merges
      state = new State(client) {
        @Override
        public Collection<MergeInfo> merges() {
          TableId tableIdToModify = TableId.of(client.tableOperations().tableIdMap().get(t3));
          return Collections.singletonList(
              new MergeInfo(new KeyExtent(tableIdToModify, null, null), MergeInfo.Operation.MERGE));
        }
      };
      assertEquals("Should have 2 tablets that need to be chopped or unassigned", 1,
          findTabletsNeedingAttention(client, metaCopy2, state));

      // test the bad tablet location state case (inconsistent metadata)
      state = new State(client);
      addDuplicateLocation(client, metaCopy3, t3);
      assertEquals("Should have 1 tablet that needs a metadata repair", 1,
          findTabletsNeedingAttention(client, metaCopy3, state));

      // clean up
      dropTables(client, t1, t2, t3, metaCopy1, metaCopy2, metaCopy3);
    }
  }

  private void addDuplicateLocation(AccumuloClient client, String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    Mutation m = new Mutation(new KeyExtent(tableIdToModify, null, null).getMetadataEntry());
    m.put(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME, new Text("1234567"),
        new Value("fake:9005"));
    try (BatchWriter bw = client.createBatchWriter(table)) {
      bw.addMutation(m);
    }
  }

  private void reassignLocation(AccumuloClient client, String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    TableId tableIdToModify =
        TableId.of(client.tableOperations().tableIdMap().get(tableNameToModify));
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(new KeyExtent(tableIdToModify, null, null).toMetadataRange());
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
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
    BatchDeleter deleter =
        client.createBatchDeleter(table, Authorizations.EMPTY, 1, new BatchWriterConfig());
    deleter.setRanges(
        Collections.singleton(new KeyExtent(tableIdToModify, null, null).toMetadataRange()));
    deleter.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    deleter.delete();
    deleter.close();
  }

  private int findTabletsNeedingAttention(AccumuloClient client, String table, State state)
      throws TableNotFoundException {
    int results = 0;
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      MetaDataTableScanner.configureScanner(scanner, state);
      scanner.updateScanIteratorOption("tabletChange", "debug", "1");
      for (Entry<Key,Value> e : scanner) {
        if (e != null)
          results++;
      }
    }

    return results;
  }

  private void createTable(AccumuloClient client, String t, boolean online)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException,
      TableExistsException {
    client.tableOperations().create(t);
    client.tableOperations().online(t, true);
    SortedSet<Text> partitionKeys = new TreeSet<>();
    partitionKeys.add(new Text("some split"));
    client.tableOperations().addSplits(t, partitionKeys);
    if (!online) {
      client.tableOperations().offline(t, true);
    }
  }

  private void copyTable(AccumuloClient client, String source, String copy)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException {
    try {
      dropTables(client, copy);
    } catch (TableNotFoundException ex) {
      // ignored
    }

    client.tableOperations().create(copy);

    try (Scanner scanner = client.createScanner(source, Authorizations.EMPTY);
        BatchWriter writer = client.createBatchWriter(copy, new BatchWriterConfig())) {
      RowIterator rows = new RowIterator(new IsolatedScanner(scanner));

      while (rows.hasNext()) {
        Iterator<Entry<Key,Value>> row = rows.next();
        Mutation m = null;

        while (row.hasNext()) {
          Entry<Key,Value> entry = row.next();
          Key k = entry.getKey();
          if (m == null)
            m = new Mutation(k.getRow());

          m.put(k.getColumnFamily(), k.getColumnQualifier(), k.getColumnVisibilityParsed(),
              k.getTimestamp(), entry.getValue());
        }

        writer.addMutation(m);
      }
    }
  }

  private void dropTables(AccumuloClient client, String... tables)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    for (String t : tables) {
      client.tableOperations().delete(t);
    }
  }

  private class State implements CurrentState {

    final AccumuloClient client;

    State(AccumuloClient client) {
      this.client = client;
    }

    @Override
    public Set<TServerInstance> onlineTabletServers() {
      HashSet<TServerInstance> tservers = new HashSet<>();
      for (String tserver : client.instanceOperations().getTabletServers()) {
        try {
          String zPath = ZooUtil.getRoot(client.instanceOperations().getInstanceID())
              + Constants.ZTSERVERS + "/" + tserver;
          ClientInfo info = getClientInfo();
          long sessionId = ZooLock.getSessionId(
              new ZooCache(info.getZooKeepers(), info.getZooKeepersSessionTimeOut()), zPath);
          tservers.add(new TServerInstance(tserver, sessionId));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return tservers;
    }

    @Override
    public Set<TableId> onlineTables() {
      ClientContext context = (ClientContext) client;
      Set<TableId> onlineTables = Tables.getIdToNameMap(context).keySet();
      return Sets.filter(onlineTables,
          tableId -> Tables.getTableState(context, tableId) == TableState.ONLINE);
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
    public MasterState getMasterState() {
      return MasterState.NORMAL;
    }
  }

}

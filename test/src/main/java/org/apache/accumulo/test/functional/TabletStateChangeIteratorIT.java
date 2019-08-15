/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
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
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletStateChangeIterator;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.base.Predicate;
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
    String[] tables = getUniqueNames(6);
    final String t1 = tables[0];
    final String t2 = tables[1];
    final String t3 = tables[2];
    final String metaCopy1 = tables[3];
    final String metaCopy2 = tables[4];
    final String metaCopy3 = tables[5];

    // create some metadata
    createTable(t1, true);
    createTable(t2, false);
    createTable(t3, true);

    // examine a clone of the metadata table, so we can manipulate it
    copyTable(MetadataTable.NAME, metaCopy1);

    State state = new State();
    while (findTabletsNeedingAttention(metaCopy1, state) > 0) {
      UtilWaitThread.sleep(500);
      copyTable(MetadataTable.NAME, metaCopy1);
    }
    assertEquals("No tables should need attention", 0,
        findTabletsNeedingAttention(metaCopy1, state));

    // The metadata table stabilized and metaCopy1 contains a copy suitable for testing. Before
    // metaCopy1 is modified, copy it for subsequent test.
    copyTable(metaCopy1, metaCopy2);
    copyTable(metaCopy1, metaCopy3);

    // test the assigned case (no location)
    removeLocation(metaCopy1, t3);
    assertEquals("Should have two tablets without a loc", 2,
        findTabletsNeedingAttention(metaCopy1, state));

    // test the cases where the assignment is to a dead tserver
    reassignLocation(metaCopy2, t3);
    assertEquals("Should have one tablet that needs to be unassigned", 1,
        findTabletsNeedingAttention(metaCopy2, state));

    // test the cases where there is ongoing merges
    state = new State() {
      @Override
      public Collection<MergeInfo> merges() {
        String tableIdToModify = getConnector().tableOperations().tableIdMap().get(t3);
        return Collections.singletonList(
            new MergeInfo(new KeyExtent(tableIdToModify, null, null), MergeInfo.Operation.MERGE));
      }
    };
    assertEquals("Should have 2 tablets that need to be chopped or unassigned", 1,
        findTabletsNeedingAttention(metaCopy2, state));

    // test the bad tablet location state case (inconsistent metadata)
    state = new State();
    addDuplicateLocation(metaCopy3, t3);
    assertEquals("Should have 1 tablet that needs a metadata repair", 1,
        findTabletsNeedingAttention(metaCopy3, state));

    // clean up
    dropTables(t1, t2, t3, metaCopy1, metaCopy2, metaCopy3);
  }

  private void addDuplicateLocation(String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    String tableIdToModify = getConnector().tableOperations().tableIdMap().get(tableNameToModify);
    Mutation m = new Mutation(new KeyExtent(tableIdToModify, null, null).getMetadataEntry());
    m.put(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME, new Text("1234567"),
        new Value("fake:9005".getBytes(UTF_8)));
    BatchWriter bw = getConnector().createBatchWriter(table, null);
    bw.addMutation(m);
    bw.close();
  }

  private void reassignLocation(String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    String tableIdToModify = getConnector().tableOperations().tableIdMap().get(tableNameToModify);
    Scanner scanner = getConnector().createScanner(table, Authorizations.EMPTY);
    scanner.setRange(new KeyExtent(tableIdToModify, null, null).toMetadataRange());
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    Entry<Key,Value> entry = scanner.iterator().next();
    Mutation m = new Mutation(entry.getKey().getRow());
    m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(),
        entry.getKey().getTimestamp());
    m.put(entry.getKey().getColumnFamily(), new Text("1234567"), entry.getKey().getTimestamp() + 1,
        new Value("fake:9005".getBytes(UTF_8)));
    scanner.close();
    BatchWriter bw = getConnector().createBatchWriter(table, null);
    bw.addMutation(m);
    bw.close();
  }

  private void removeLocation(String table, String tableNameToModify)
      throws TableNotFoundException, MutationsRejectedException {
    String tableIdToModify = getConnector().tableOperations().tableIdMap().get(tableNameToModify);
    BatchDeleter deleter =
        getConnector().createBatchDeleter(table, Authorizations.EMPTY, 1, new BatchWriterConfig());
    deleter.setRanges(
        Collections.singleton(new KeyExtent(tableIdToModify, null, null).toMetadataRange()));
    deleter.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    deleter.delete();
    deleter.close();
  }

  private int findTabletsNeedingAttention(String table, State state) throws TableNotFoundException {
    int results = 0;
    Scanner scanner = getConnector().createScanner(table, Authorizations.EMPTY);
    MetaDataTableScanner.configureScanner(scanner, state);
    scanner.updateScanIteratorOption("tabletChange", "debug", "1");
    for (Entry<Key,Value> e : scanner) {
      if (e != null)
        results++;
    }

    return results;
  }

  private void createTable(String t, boolean online) throws AccumuloSecurityException,
      AccumuloException, TableNotFoundException, TableExistsException {
    Connector conn = getConnector();
    conn.tableOperations().create(t);
    conn.tableOperations().online(t, true);
    SortedSet<Text> partitionKeys = new TreeSet<>();
    partitionKeys.add(new Text("some split"));
    conn.tableOperations().addSplits(t, partitionKeys);
    if (!online) {
      conn.tableOperations().offline(t, true);
    }
  }

  private void copyTable(String source, String copy) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, TableExistsException {
    try {
      dropTables(copy);
    } catch (TableNotFoundException ex) {
      // ignored
    }

    getConnector().tableOperations().create(copy);

    try (Scanner scanner = getConnector().createScanner(source, Authorizations.EMPTY);
        BatchWriter writer = getConnector().createBatchWriter(copy, new BatchWriterConfig())) {
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

  private void dropTables(String... tables)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    for (String t : tables) {
      getConnector().tableOperations().delete(t);
    }
  }

  private class State implements CurrentState {

    @Override
    public Set<TServerInstance> onlineTabletServers() {
      HashSet<TServerInstance> tservers = new HashSet<>();
      for (String tserver : getConnector().instanceOperations().getTabletServers()) {
        try {
          String zPath =
              ZooUtil.getRoot(getConnector().getInstance()) + Constants.ZTSERVERS + "/" + tserver;
          long sessionId = ZooLock.getSessionId(new ZooCache(getCluster().getZooKeepers(),
              getConnector().getInstance().getZooKeepersSessionTimeOut()), zPath);
          tservers.add(new TServerInstance(tserver, sessionId));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return tservers;
    }

    @Override
    public Set<String> onlineTables() {
      HashSet<String> onlineTables =
          new HashSet<>(getConnector().tableOperations().tableIdMap().values());
      return Sets.filter(onlineTables, new Predicate<String>() {
        @Override
        public boolean apply(String tableId) {
          return Tables.getTableState(getConnector().getInstance(), tableId) == TableState.ONLINE;
        }
      });
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

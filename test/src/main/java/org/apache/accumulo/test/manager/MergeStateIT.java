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
package org.apache.accumulo.test.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.manager.state.MergeStats;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.state.Assignment;
import org.apache.accumulo.server.manager.state.CurrentState;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MergeState;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MergeStateIT extends ConfigurableMacBase {

  private static class MockCurrentState implements CurrentState {

    TServerInstance someTServer =
        new TServerInstance(HostAndPort.fromParts("127.0.0.1", 1234), 0x123456);
    MergeInfo mergeInfo;

    MockCurrentState(MergeInfo info) {
      this.mergeInfo = info;
    }

    @Override
    public Set<TableId> onlineTables() {
      return Collections.singleton(TableId.of("t"));
    }

    @Override
    public Set<TServerInstance> onlineTabletServers() {
      return Collections.singleton(someTServer);
    }

    @Override
    public Collection<MergeInfo> merges() {
      return Collections.singleton(mergeInfo);
    }

    @Override
    public Set<KeyExtent> migrationsSnapshot() {
      return Collections.emptySet();
    }

    @Override
    public ManagerState getManagerState() {
      return ManagerState.NORMAL;
    }

    @Override
    public Set<TServerInstance> shutdownServers() {
      return Collections.emptySet();
    }
  }

  private static void update(AccumuloClient c, Mutation m)
      throws TableNotFoundException, MutationsRejectedException {
    try (BatchWriter bw = c.createBatchWriter(MetadataTable.NAME)) {
      bw.addMutation(m);
    }
  }

  @Test
  public void test() throws Exception {
    ServerContext context = getServerContext();
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProperties()).build()) {
      accumuloClient.securityOperations().grantTablePermission(accumuloClient.whoami(),
          MetadataTable.NAME, TablePermission.WRITE);
      BatchWriter bw = accumuloClient.createBatchWriter(MetadataTable.NAME);

      // Create a fake METADATA table with these splits
      String[] splits = {"a", "e", "j", "o", "t", "z"};
      // create metadata for a table "t" with the splits above
      TableId tableId = TableId.of("t");
      Text pr = null;
      for (String s : splits) {
        Text split = new Text(s);
        Mutation prevRow =
            TabletColumnFamily.createPrevRowMutation(new KeyExtent(tableId, split, pr));
        prevRow.put(CurrentLocationColumnFamily.NAME, new Text("123456"),
            new Value("127.0.0.1:1234"));
        ChoppedColumnFamily.CHOPPED_COLUMN.put(prevRow, new Value("junk"));
        bw.addMutation(prevRow);
        pr = split;
      }
      // Add the default tablet
      Mutation defaultTablet =
          TabletColumnFamily.createPrevRowMutation(new KeyExtent(tableId, null, pr));
      defaultTablet.put(CurrentLocationColumnFamily.NAME, new Text("123456"),
          new Value("127.0.0.1:1234"));
      bw.addMutation(defaultTablet);
      bw.close();

      // Read out the TabletLocationStates
      MockCurrentState state =
          new MockCurrentState(new MergeInfo(new KeyExtent(tableId, new Text("p"), new Text("e")),
              MergeInfo.Operation.MERGE));

      // Verify the tablet state: hosted, and count
      TabletStateStore metaDataStateStore =
          TabletStateStore.getStoreForLevel(DataLevel.USER, context, state);
      int count = 0;
      for (TabletLocationState tss : metaDataStateStore) {
        if (tss != null) {
          count++;
        }
      }
      assertEquals(0, count); // the normal case is to skip tablets in a good state

      // Create the hole
      // Split the tablet at one end of the range
      Mutation m = TabletColumnFamily
          .createPrevRowMutation(new KeyExtent(tableId, new Text("t"), new Text("p")));
      TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value("0.5"));
      TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m,
          TabletColumnFamily.encodePrevEndRow(new Text("o")));
      update(accumuloClient, m);

      // do the state check
      MergeStats stats = scan(state, metaDataStateStore);
      MergeState newState = stats.nextMergeState(accumuloClient, state);
      assertEquals(MergeState.WAITING_FOR_OFFLINE, newState);

      // unassign the tablets
      try (BatchDeleter deleter =
          accumuloClient.createBatchDeleter(MetadataTable.NAME, Authorizations.EMPTY, 1000)) {
        deleter.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
        deleter.setRanges(Collections.singletonList(new Range()));
        deleter.delete();
      }

      // now we should be ready to merge but, we have inconsistent metadata
      stats = scan(state, metaDataStateStore);
      assertEquals(MergeState.WAITING_FOR_OFFLINE, stats.nextMergeState(accumuloClient, state));

      // finish the split
      KeyExtent tablet = new KeyExtent(tableId, new Text("p"), new Text("o"));
      m = TabletColumnFamily.createPrevRowMutation(tablet);
      TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value("0.5"));
      update(accumuloClient, m);
      metaDataStateStore
          .setLocations(Collections.singletonList(new Assignment(tablet, state.someTServer)));

      // onos... there's a new tablet online
      stats = scan(state, metaDataStateStore);
      assertEquals(MergeState.WAITING_FOR_CHOPPED, stats.nextMergeState(accumuloClient, state));

      // chop it
      m = TabletColumnFamily.createPrevRowMutation(tablet);
      ChoppedColumnFamily.CHOPPED_COLUMN.put(m, new Value("junk"));
      update(accumuloClient, m);

      stats = scan(state, metaDataStateStore);
      assertEquals(MergeState.WAITING_FOR_OFFLINE, stats.nextMergeState(accumuloClient, state));

      // take it offline
      m = TabletColumnFamily.createPrevRowMutation(tablet);
      Collection<Collection<String>> walogs = Collections.emptyList();
      metaDataStateStore.unassign(
          Collections.singletonList(
              new TabletLocationState(tablet, null, state.someTServer, null, null, walogs, false)),
          null);

      // now we can split
      stats = scan(state, metaDataStateStore);
      assertEquals(MergeState.MERGING, stats.nextMergeState(accumuloClient, state));
    }
  }

  private MergeStats scan(MockCurrentState state, TabletStateStore metaDataStateStore) {
    MergeStats stats = new MergeStats(state.mergeInfo);
    stats.getMergeInfo().setState(MergeState.WAITING_FOR_OFFLINE);
    for (TabletLocationState tss : metaDataStateStore) {
      stats.update(tss.extent, tss.getState(state.onlineTabletServers()), tss.chopped, false);
    }
    return stats;
  }
}

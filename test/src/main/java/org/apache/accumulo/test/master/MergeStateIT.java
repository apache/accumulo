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
package org.apache.accumulo.test.master;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.master.state.MergeStats;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class MergeStateIT extends ConfigurableMacBase {

  private static class MockCurrentState implements CurrentState {

    TServerInstance someTServer = new TServerInstance(HostAndPort.fromParts("127.0.0.1", 1234), 0x123456);
    MergeInfo mergeInfo;

    MockCurrentState(MergeInfo info) {
      this.mergeInfo = info;
    }

    @Override
    public Set<Table.ID> onlineTables() {
      return Collections.singleton(Table.ID.of("t"));
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
    public MasterState getMasterState() {
      return MasterState.NORMAL;
    }

    @Override
    public Set<TServerInstance> shutdownServers() {
      return Collections.emptySet();
    }
  }

  private static void update(Connector c, Mutation m) throws TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = c.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    bw.addMutation(m);
    bw.close();
  }

  @Test
  public void test() throws Exception {
    AccumuloServerContext context = EasyMock.createMock(AccumuloServerContext.class);
    Connector connector = getConnector();
    EasyMock.expect(context.getConnector()).andReturn(connector).anyTimes();
    EasyMock.replay(context);
    connector.securityOperations().grantTablePermission(connector.whoami(), MetadataTable.NAME, TablePermission.WRITE);
    BatchWriter bw = connector.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());

    // Create a fake METADATA table with these splits
    String splits[] = {"a", "e", "j", "o", "t", "z"};
    // create metadata for a table "t" with the splits above
    Table.ID tableId = Table.ID.of("t");
    Text pr = null;
    for (String s : splits) {
      Text split = new Text(s);
      Mutation prevRow = KeyExtent.getPrevRowUpdateMutation(new KeyExtent(tableId, split, pr));
      prevRow.put(TabletsSection.CurrentLocationColumnFamily.NAME, new Text("123456"), new Value("127.0.0.1:1234".getBytes()));
      ChoppedColumnFamily.CHOPPED_COLUMN.put(prevRow, new Value("junk".getBytes()));
      bw.addMutation(prevRow);
      pr = split;
    }
    // Add the default tablet
    Mutation defaultTablet = KeyExtent.getPrevRowUpdateMutation(new KeyExtent(tableId, null, pr));
    defaultTablet.put(TabletsSection.CurrentLocationColumnFamily.NAME, new Text("123456"), new Value("127.0.0.1:1234".getBytes()));
    bw.addMutation(defaultTablet);
    bw.close();

    // Read out the TabletLocationStates
    MockCurrentState state = new MockCurrentState(new MergeInfo(new KeyExtent(tableId, new Text("p"), new Text("e")), MergeInfo.Operation.MERGE));

    // Verify the tablet state: hosted, and count
    MetaDataStateStore metaDataStateStore = new MetaDataStateStore(context, state);
    int count = 0;
    for (TabletLocationState tss : metaDataStateStore) {
      if (tss != null)
        count++;
    }
    Assert.assertEquals(0, count); // the normal case is to skip tablets in a good state

    // Create the hole
    // Split the tablet at one end of the range
    Mutation m = new KeyExtent(tableId, new Text("t"), new Text("p")).getPrevRowUpdateMutation();
    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value("0.5".getBytes()));
    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m, KeyExtent.encodePrevEndRow(new Text("o")));
    update(connector, m);

    // do the state check
    MergeStats stats = scan(state, metaDataStateStore);
    MergeState newState = stats.nextMergeState(connector, state);
    Assert.assertEquals(MergeState.WAITING_FOR_OFFLINE, newState);

    // unassign the tablets
    BatchDeleter deleter = connector.createBatchDeleter(MetadataTable.NAME, Authorizations.EMPTY, 1000, new BatchWriterConfig());
    deleter.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
    deleter.setRanges(Collections.singletonList(new Range()));
    deleter.delete();

    // now we should be ready to merge but, we have inconsistent metadata
    stats = scan(state, metaDataStateStore);
    Assert.assertEquals(MergeState.WAITING_FOR_OFFLINE, stats.nextMergeState(connector, state));

    // finish the split
    KeyExtent tablet = new KeyExtent(tableId, new Text("p"), new Text("o"));
    m = tablet.getPrevRowUpdateMutation();
    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value("0.5".getBytes()));
    update(connector, m);
    metaDataStateStore.setLocations(Collections.singletonList(new Assignment(tablet, state.someTServer)));

    // onos... there's a new tablet online
    stats = scan(state, metaDataStateStore);
    Assert.assertEquals(MergeState.WAITING_FOR_CHOPPED, stats.nextMergeState(connector, state));

    // chop it
    m = tablet.getPrevRowUpdateMutation();
    ChoppedColumnFamily.CHOPPED_COLUMN.put(m, new Value("junk".getBytes()));
    update(connector, m);

    stats = scan(state, metaDataStateStore);
    Assert.assertEquals(MergeState.WAITING_FOR_OFFLINE, stats.nextMergeState(connector, state));

    // take it offline
    m = tablet.getPrevRowUpdateMutation();
    Collection<Collection<String>> walogs = Collections.emptyList();
    metaDataStateStore.unassign(Collections.singletonList(new TabletLocationState(tablet, null, state.someTServer, null, null, walogs, false)), null);

    // now we can split
    stats = scan(state, metaDataStateStore);
    Assert.assertEquals(MergeState.MERGING, stats.nextMergeState(connector, state));

  }

  private MergeStats scan(MockCurrentState state, MetaDataStateStore metaDataStateStore) {
    MergeStats stats = new MergeStats(state.mergeInfo);
    stats.getMergeInfo().setState(MergeState.WAITING_FOR_OFFLINE);
    for (TabletLocationState tss : metaDataStateStore) {
      stats.update(tss.extent, tss.getState(state.onlineTabletServers()), tss.chopped, false);
    }
    return stats;
  }
}

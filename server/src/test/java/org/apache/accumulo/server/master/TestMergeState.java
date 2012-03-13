/**
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
package org.apache.accumulo.server.master;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.MergeStats;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TestMergeState {
  
  class MockCurrentState implements CurrentState {
    
    TServerInstance someTServer = new TServerInstance(new InetSocketAddress("127.0.0.1", 1234), 0x123456);
    MergeInfo mergeInfo;
    
    MockCurrentState(MergeInfo info) {
      this.mergeInfo = info;
    }
    
    @Override
    public Set<String> onlineTables() {
      return Collections.singleton("t");
    }
    
    @Override
    public Set<TServerInstance> onlineTabletServers() {
      return Collections.singleton(someTServer);
    }
    
    @Override
    public Collection<MergeInfo> merges() {
      return Collections.singleton(mergeInfo);
    }
  }
  
  private static void update(Connector c, Mutation m) throws TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = c.createBatchWriter(Constants.METADATA_TABLE_NAME, 1000l, 1000l, 1);
    bw.addMutation(m);
    bw.close();
  }

  @Test
  public void test() throws Exception {
    Instance instance = new MockInstance();
    Connector connector = instance.getConnector("root", "secret");
    BatchWriter bw = connector.createBatchWriter("!METADATA", 1000l, 1000l, 1);
    
    // Create a fake METADATA table with these splits
    String splits[] = {"a", "e", "j", "o", "t", "z"};
    // create metadata for a table "t" with the splits above
    Text tableId = new Text("t");
    Text pr = null;
    for (String s : splits) {
      Text split = new Text(s);
      Mutation prevRow = KeyExtent.getPrevRowUpdateMutation(new KeyExtent(tableId, split, pr));
      prevRow.put(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, new Text("123456"), new Value("127.0.0.1:1234".getBytes()));
      ColumnFQ.put(prevRow, Constants.METADATA_CHOPPED_COLUMN, new Value("junk".getBytes()));
      bw.addMutation(prevRow);
      pr = split;
    }
    // Add the default tablet
    Mutation defaultTablet = KeyExtent.getPrevRowUpdateMutation(new KeyExtent(tableId, null, pr));
    defaultTablet.put(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, new Text("123456"), new Value("127.0.0.1:1234".getBytes()));
    bw.addMutation(defaultTablet);
    bw.close();
    
    // Read out the TabletLocationStates
    MockCurrentState state = new MockCurrentState(new MergeInfo(new KeyExtent(tableId, new Text("p"), new Text("e")), MergeInfo.Operation.MERGE));
    AuthInfo auths = new AuthInfo("root", ByteBuffer.wrap("secret".getBytes()), "instance");
    
    // Verify the tablet state: hosted, and count
    MetaDataStateStore metaDataStateStore = new MetaDataStateStore(instance, auths, state);
    int count = 0;
    for (TabletLocationState tss : metaDataStateStore) {
      Assert.assertEquals(TabletState.HOSTED, tss.getState(state.onlineTabletServers()));
      count++;
    }
    Assert.assertEquals(splits.length + 1, count);
    
    // Create the hole
    // Split the tablet at one end of the range
    Mutation m = new KeyExtent(tableId, new Text("t"), new Text("p")).getPrevRowUpdateMutation();
    ColumnFQ.put(m, Constants.METADATA_SPLIT_RATIO_COLUMN, new Value("0.5".getBytes()));
    ColumnFQ.put(m, Constants.METADATA_OLD_PREV_ROW_COLUMN, KeyExtent.encodePrevEndRow(new Text("o")));
    update(connector, m);
    
    // do the state check
    MergeStats stats = scan(state, metaDataStateStore);
    MergeState newState = stats.nextMergeState(connector, state);
    Assert.assertEquals(MergeState.WAITING_FOR_OFFLINE, newState);
    
    // unassign the tablets
    BatchDeleter deleter = connector.createBatchDeleter("!METADATA", Constants.NO_AUTHS, 1000, 1000l, 1000l, 1);
    deleter.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
    deleter.setRanges(Collections.singletonList(new Range()));
    deleter.delete();
    
    // now we should be ready to merge but, we have an inconsistent !METADATA table
    stats = scan(state, metaDataStateStore);
    Assert.assertEquals(MergeState.WAITING_FOR_OFFLINE, stats.nextMergeState(connector, state));
    
    // finish the split
    KeyExtent tablet = new KeyExtent(tableId, new Text("p"), new Text("o"));
    m = tablet.getPrevRowUpdateMutation();
    ColumnFQ.put(m, Constants.METADATA_SPLIT_RATIO_COLUMN, new Value("0.5".getBytes()));
    update(connector, m);
    metaDataStateStore.setLocations(Collections.singletonList(new Assignment(tablet, state.someTServer)));
    
    // onos... there's a new tablet online
    stats = scan(state, metaDataStateStore);
    Assert.assertEquals(MergeState.WAITING_FOR_CHOPPED, stats.nextMergeState(connector, state));
    
    // chop it
    m = tablet.getPrevRowUpdateMutation();
    ColumnFQ.put(m, Constants.METADATA_CHOPPED_COLUMN, new Value("junk".getBytes()));
    update(connector, m);

    stats = scan(state, metaDataStateStore);
    Assert.assertEquals(MergeState.WAITING_FOR_OFFLINE, stats.nextMergeState(connector, state));

    // take it offline
    m = tablet.getPrevRowUpdateMutation();
    Collection<Collection<String>> walogs = Collections.emptyList();
    metaDataStateStore.unassign(Collections.singletonList(new TabletLocationState(tablet, null, state.someTServer, null, walogs, false)));
    
    // now we can split
    stats = scan(state, metaDataStateStore);
    Assert.assertEquals(MergeState.MERGING, stats.nextMergeState(connector, state));

  }

  /**
   * @param state
   * @param metaDataStateStore
   * @param locations
   * @return
   */
  private MergeStats scan(MockCurrentState state, MetaDataStateStore metaDataStateStore) {
    MergeStats stats = new MergeStats(state.mergeInfo);
    stats.getMergeInfo().setState(MergeState.WAITING_FOR_OFFLINE);
    for (TabletLocationState tss : metaDataStateStore) {
      stats.update(tss.extent, tss.getState(state.onlineTabletServers()), tss.chopped, false);
    }
    return stats;
  }
}

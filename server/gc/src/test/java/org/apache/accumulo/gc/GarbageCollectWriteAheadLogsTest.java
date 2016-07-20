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
package org.apache.accumulo.gc;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.replication.ReplicationSchema;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.easymock.EasyMock;
import org.junit.Test;

public class GarbageCollectWriteAheadLogsTest {

  private final TServerInstance server1 = new TServerInstance("localhost:1234[SESSION]");
  private final TServerInstance server2 = new TServerInstance("localhost:1234[OTHERSESS]");
  private final UUID id = UUID.randomUUID();
  private final Map<TServerInstance,List<UUID>> markers = Collections.singletonMap(server1, Collections.singletonList(id));
  private final Map<TServerInstance,List<UUID>> markers2 = Collections.singletonMap(server2, Collections.singletonList(id));
  private final Path path = new Path("hdfs://localhost:9000/accumulo/wal/localhost+1234/" + id);
  private final KeyExtent extent = new KeyExtent(new Text("1<"), new Text(new byte[] {0}));
  private final Collection<Collection<String>> walogs = Collections.emptyList();
  private final TabletLocationState tabletAssignedToServer1;
  private final TabletLocationState tabletAssignedToServer2;
  {
    try {
      tabletAssignedToServer1 = new TabletLocationState(extent, (TServerInstance) null, server1, (TServerInstance) null, null, walogs, false);
      tabletAssignedToServer2 = new TabletLocationState(extent, (TServerInstance) null, server2, (TServerInstance) null, null, walogs, false);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  private final Iterable<TabletLocationState> tabletOnServer1List = Collections.singletonList(tabletAssignedToServer1);
  private final Iterable<TabletLocationState> tabletOnServer2List = Collections.singletonList(tabletAssignedToServer2);
  private final List<Entry<Key,Value>> emptyList = Collections.emptyList();
  private final Iterator<Entry<Key,Value>> emptyKV = emptyList.iterator();

  @Test
  public void testRemoveUnusedLog() throws Exception {
    AccumuloServerContext context = EasyMock.createMock(AccumuloServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers).once();
    EasyMock.expect(marker.state(server1, id)).andReturn(new Pair<>(WalState.UNREFERENCED, path));
    EasyMock.expect(fs.deleteRecursively(path)).andReturn(true).once();
    marker.removeWalMarker(server1, id);
    EasyMock.expectLastCall().once();
    EasyMock.replay(context, fs, marker, tserverSet);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false, tserverSet, marker, tabletOnServer1List) {
      @Override
      protected int removeReplicationEntries(Map<UUID,TServerInstance> candidates) throws IOException, KeeperException, InterruptedException {
        return 0;
      }
    };
    gc.collect(status);
    EasyMock.verify(context, fs, marker, tserverSet);
  }

  @Test
  public void testKeepClosedLog() throws Exception {
    AccumuloServerContext context = EasyMock.createMock(AccumuloServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));
    EasyMock.expect(marker.getAllMarkers()).andReturn(markers).once();
    EasyMock.expect(marker.state(server1, id)).andReturn(new Pair<>(WalState.CLOSED, path));
    EasyMock.replay(context, marker, tserverSet, fs);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false, tserverSet, marker, tabletOnServer1List) {
      @Override
      protected int removeReplicationEntries(Map<UUID,TServerInstance> candidates) throws IOException, KeeperException, InterruptedException {
        return 0;
      }
    };
    gc.collect(status);
    EasyMock.verify(context, marker, tserverSet, fs);
  }

  @Test
  public void deleteUnreferenceLogOnDeadServer() throws Exception {
    AccumuloServerContext context = EasyMock.createMock(AccumuloServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);
    Connector conn = EasyMock.createMock(Connector.class);
    Scanner mscanner = EasyMock.createMock(Scanner.class);
    Scanner rscanner = EasyMock.createMock(Scanner.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));
    EasyMock.expect(marker.getAllMarkers()).andReturn(markers2).once();
    EasyMock.expect(marker.state(server2, id)).andReturn(new Pair<>(WalState.OPEN, path));
    EasyMock.expect(context.getConnector()).andReturn(conn);

    EasyMock.expect(conn.createScanner(ReplicationTable.NAME, Authorizations.EMPTY)).andReturn(rscanner);
    rscanner.fetchColumnFamily(ReplicationSchema.StatusSection.NAME);
    EasyMock.expectLastCall().once();
    EasyMock.expect(rscanner.iterator()).andReturn(emptyKV);

    EasyMock.expect(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)).andReturn(mscanner);
    mscanner.fetchColumnFamily(MetadataSchema.ReplicationSection.COLF);
    EasyMock.expectLastCall().once();
    mscanner.setRange(MetadataSchema.ReplicationSection.getRange());
    EasyMock.expectLastCall().once();
    EasyMock.expect(mscanner.iterator()).andReturn(emptyKV);
    EasyMock.expect(fs.deleteRecursively(path)).andReturn(true).once();
    marker.removeWalMarker(server2, id);
    EasyMock.expectLastCall().once();
    marker.forget(server2);
    EasyMock.expectLastCall().once();
    EasyMock.replay(context, fs, marker, tserverSet, conn, rscanner, mscanner);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false, tserverSet, marker, tabletOnServer1List);
    gc.collect(status);
    EasyMock.verify(context, fs, marker, tserverSet, conn, rscanner, mscanner);
  }

  @Test
  public void ignoreReferenceLogOnDeadServer() throws Exception {
    AccumuloServerContext context = EasyMock.createMock(AccumuloServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);
    Connector conn = EasyMock.createMock(Connector.class);
    Scanner mscanner = EasyMock.createMock(Scanner.class);
    Scanner rscanner = EasyMock.createMock(Scanner.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));
    EasyMock.expect(marker.getAllMarkers()).andReturn(markers2).once();
    EasyMock.expect(marker.state(server2, id)).andReturn(new Pair<>(WalState.OPEN, path));
    EasyMock.expect(context.getConnector()).andReturn(conn);

    EasyMock.expect(conn.createScanner(ReplicationTable.NAME, Authorizations.EMPTY)).andReturn(rscanner);
    rscanner.fetchColumnFamily(ReplicationSchema.StatusSection.NAME);
    EasyMock.expectLastCall().once();
    EasyMock.expect(rscanner.iterator()).andReturn(emptyKV);

    EasyMock.expect(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)).andReturn(mscanner);
    mscanner.fetchColumnFamily(MetadataSchema.ReplicationSection.COLF);
    EasyMock.expectLastCall().once();
    mscanner.setRange(MetadataSchema.ReplicationSection.getRange());
    EasyMock.expectLastCall().once();
    EasyMock.expect(mscanner.iterator()).andReturn(emptyKV);
    EasyMock.replay(context, fs, marker, tserverSet, conn, rscanner, mscanner);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false, tserverSet, marker, tabletOnServer2List);
    gc.collect(status);
    EasyMock.verify(context, fs, marker, tserverSet, conn, rscanner, mscanner);
  }

  @Test
  public void replicationDelaysFileCollection() throws Exception {
    AccumuloServerContext context = EasyMock.createMock(AccumuloServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);
    Connector conn = EasyMock.createMock(Connector.class);
    Scanner mscanner = EasyMock.createMock(Scanner.class);
    Scanner rscanner = EasyMock.createMock(Scanner.class);
    String row = MetadataSchema.ReplicationSection.getRowPrefix() + path.toString();
    String colf = MetadataSchema.ReplicationSection.COLF.toString();
    String colq = "1";
    Map<Key,Value> replicationWork = Collections.singletonMap(new Key(row, colf, colq), new Value(new byte[0]));

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));
    EasyMock.expect(marker.getAllMarkers()).andReturn(markers).once();
    EasyMock.expect(marker.state(server1, id)).andReturn(new Pair<>(WalState.UNREFERENCED, path));
    EasyMock.expect(context.getConnector()).andReturn(conn);

    EasyMock.expect(conn.createScanner(ReplicationTable.NAME, Authorizations.EMPTY)).andReturn(rscanner);
    rscanner.fetchColumnFamily(ReplicationSchema.StatusSection.NAME);
    EasyMock.expectLastCall().once();
    EasyMock.expect(rscanner.iterator()).andReturn(emptyKV);

    EasyMock.expect(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)).andReturn(mscanner);
    mscanner.fetchColumnFamily(MetadataSchema.ReplicationSection.COLF);
    EasyMock.expectLastCall().once();
    mscanner.setRange(MetadataSchema.ReplicationSection.getRange());
    EasyMock.expectLastCall().once();
    EasyMock.expect(mscanner.iterator()).andReturn(replicationWork.entrySet().iterator());
    EasyMock.replay(context, fs, marker, tserverSet, conn, rscanner, mscanner);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false, tserverSet, marker, tabletOnServer1List);
    gc.collect(status);
    EasyMock.verify(context, fs, marker, tserverSet, conn, rscanner, mscanner);
  }
}

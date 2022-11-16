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
package org.apache.accumulo.gc;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class GarbageCollectWriteAheadLogsTest {

  private final TServerInstance server1 = new TServerInstance("localhost:1234[SESSION]");
  private final TServerInstance server2 = new TServerInstance("localhost:1234[OTHERSESS]");
  private final UUID id = UUID.randomUUID();
  private final Map<TServerInstance,List<UUID>> markers =
      Collections.singletonMap(server1, Collections.singletonList(id));
  private final Map<TServerInstance,List<UUID>> markers2 =
      Collections.singletonMap(server2, Collections.singletonList(id));
  private final Path path = new Path("hdfs://localhost:9000/accumulo/wal/localhost+1234/" + id);
  private final KeyExtent extent = KeyExtent.fromMetaRow(new Text("1<"));
  private final Collection<Collection<String>> walogs = Collections.emptyList();
  private final TabletLocationState tabletAssignedToServer1;
  private final TabletLocationState tabletAssignedToServer2;

  {
    try {
      tabletAssignedToServer1 =
          new TabletLocationState(extent, null, server1, null, null, walogs, false);
      tabletAssignedToServer2 =
          new TabletLocationState(extent, null, server2, null, null, walogs, false);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private final Iterable<TabletLocationState> tabletOnServer1List =
      Collections.singletonList(tabletAssignedToServer1);
  private final Iterable<TabletLocationState> tabletOnServer2List =
      Collections.singletonList(tabletAssignedToServer2);

  @Test
  public void testRemoveUnusedLog() throws Exception {
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    tserverSet.scanServers();
    EasyMock.expectLastCall();
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers).once();
    EasyMock.expect(marker.state(server1, id)).andReturn(new Pair<>(WalState.UNREFERENCED, path));
    EasyMock.expect(fs.deleteRecursively(path)).andReturn(true).once();
    marker.removeWalMarker(server1, id);
    EasyMock.expectLastCall().once();
    EasyMock.replay(context, fs, marker, tserverSet);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false,
        tserverSet, marker, tabletOnServer1List) {
      @Override
      protected Map<UUID,Path> getSortedWALogs() {
        return Collections.emptyMap();
      }
    };
    gc.collect(status);
    EasyMock.verify(context, fs, marker, tserverSet);
  }

  @Test
  public void testKeepClosedLog() throws Exception {
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    tserverSet.scanServers();
    EasyMock.expectLastCall();
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers).once();
    EasyMock.expect(marker.state(server1, id)).andReturn(new Pair<>(WalState.CLOSED, path));
    EasyMock.replay(context, marker, tserverSet, fs);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false,
        tserverSet, marker, tabletOnServer1List) {
      @Override
      protected Map<UUID,Path> getSortedWALogs() {
        return Collections.emptyMap();
      }
    };
    gc.collect(status);
    EasyMock.verify(context, marker, tserverSet, fs);
  }

  @Test
  public void deleteUnreferenceLogOnDeadServer() throws Exception {
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    tserverSet.scanServers();
    EasyMock.expectLastCall();
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers2).once();
    EasyMock.expect(marker.state(server2, id)).andReturn(new Pair<>(WalState.OPEN, path));

    EasyMock.expect(fs.deleteRecursively(path)).andReturn(true).once();
    marker.removeWalMarker(server2, id);
    EasyMock.expectLastCall().once();
    marker.forget(server2);
    EasyMock.expectLastCall().once();
    EasyMock.replay(context, fs, marker, tserverSet);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false,
        tserverSet, marker, tabletOnServer1List) {
      @Override
      protected Map<UUID,Path> getSortedWALogs() {
        return Collections.emptyMap();
      }
    };
    gc.collect(status);
    EasyMock.verify(context, fs, marker, tserverSet);
  }

  @Test
  public void ignoreReferenceLogOnDeadServer() throws Exception {
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    tserverSet.scanServers();
    EasyMock.expectLastCall();
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers2).once();
    EasyMock.expect(marker.state(server2, id)).andReturn(new Pair<>(WalState.OPEN, path));

    EasyMock.replay(context, fs, marker, tserverSet);
    GarbageCollectWriteAheadLogs gc = new GarbageCollectWriteAheadLogs(context, fs, false,
        tserverSet, marker, tabletOnServer2List) {
      @Override
      protected Map<UUID,Path> getSortedWALogs() {
        return Collections.emptyMap();
      }
    };
    gc.collect(status);
    EasyMock.verify(context, fs, marker, tserverSet);
  }

}

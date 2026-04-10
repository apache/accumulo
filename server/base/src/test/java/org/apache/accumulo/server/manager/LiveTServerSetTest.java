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
package org.apache.accumulo.server.manager;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.rpc.RpcService;
import org.apache.accumulo.core.zookeeper.ZcStat;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerInfo;
import org.junit.jupiter.api.Test;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;

public class LiveTServerSetTest {

  @Test
  public void testSessionIds() {
    Map<ServiceLockPath,TServerInfo> servers = new HashMap<>();
    TServerConnection mockConn = createMock(TServerConnection.class);

    var hostPort = HostAndPort.fromParts("localhost", 1234);

    TServerInfo server1 =
        new TServerInfo(new TServerInstance(hostPort, "5555"), mockConn, ResourceGroupId.DEFAULT);

    ServiceLockPaths lockPaths = new ServiceLockPaths(createMock(ZooCache.class));

    servers.put(lockPaths.createTabletServerPath(ResourceGroupId.DEFAULT, hostPort), server1);

    assertEquals(server1.instance, LiveTServerSet.find(servers, "localhost:1234"));
    assertNull(LiveTServerSet.find(servers, "localhost:4321"));
    assertEquals(server1.instance, LiveTServerSet.find(servers, "localhost:1234[5555]"));
    assertNull(LiveTServerSet.find(servers, "localhost:1234[55755]"));
  }

  record LockInfo(ServiceLockData sld, long sessionId) {
  }

  static class TestLiveTserverSet extends LiveTServerSet {
    private final Set<ServiceLockPath> paths;
    private final Map<ServiceLockPath,LockInfo> locks;

    public TestLiveTserverSet(Set<ServiceLockPath> paths, Map<ServiceLockPath,LockInfo> locks,
        Listener listener) {
      super(createStrictMock(ServerContext.class));
      this.paths = paths;
      this.locks = locks;
      this.cback.set(listener);
    }

    @VisibleForTesting
    protected Set<ServiceLockPath> getTserverPaths() {
      return paths;
    }

    @VisibleForTesting
    protected Optional<ServiceLockData> getLockData(ServiceLockPath tserverPath, ZcStat stat) {
      var lockInfo = locks.get(tserverPath);
      if (lockInfo == null) {
        return Optional.empty();
      }

      stat.setEphemeralOwner(lockInfo.sessionId);

      return Optional.of(lockInfo.sld);
    }
  }

  @Test
  public void testSameHostPortInDiffResourceGroups() {
    // This tests two tservers with the same host port in different resource groups where only one
    // has a lock. There was a bug where LiveTserverSet would add and remove that host port from its
    // set and pass both add/remove to the callback listener.

    ServiceLockPaths lockPaths = new ServiceLockPaths(createMock(ZooCache.class));

    var hp1 = HostAndPort.fromParts("host1", 9800);
    var g1 = ResourceGroupId.of("tg1");
    var path1 = lockPaths.createTabletServerPath(ResourceGroupId.DEFAULT, hp1);

    var path2 = lockPaths.createTabletServerPath(g1, hp1);

    var paths = new HashSet<ServiceLockPath>();
    paths.add(path1);
    paths.add(path2);

    var locks = new HashMap<ServiceLockPath,LockInfo>();
    locks.put(path2, new LockInfo(
        new ServiceLockData(UUID.randomUUID(), hp1.toString(), RpcService.TSERV, g1), 123456));

    var deletedSeen = new HashSet<TServerInstance>();
    var addedSeen = new HashSet<TServerInstance>();
    LiveTServerSet tservers = new TestLiveTserverSet(paths, locks, ((current, deleted, added) -> {
      deletedSeen.addAll(deleted);
      addedSeen.addAll(added);
    }));
    tservers.scanServers();

    var expected1 = Set.of(new TServerInstance(hp1, 123456));
    assertEquals(expected1, addedSeen);
    assertEquals(Set.of(), deletedSeen);
    assertEquals(expected1, tservers.getSnapshot().getTservers());
    assertEquals(Map.of(g1, expected1), tservers.getSnapshot().getTserverGroups());

    // change which tserver has the lock
    locks.clear();
    locks.put(path1, new LockInfo(new ServiceLockData(UUID.randomUUID(), hp1.toString(),
        RpcService.TSERV, ResourceGroupId.DEFAULT), 654321));

    addedSeen.clear();
    tservers.scanServers();

    var expected2 = Set.of(new TServerInstance(hp1, 654321));
    assertEquals(expected2, addedSeen);
    assertEquals(expected1, deletedSeen);
    assertEquals(expected2, tservers.getSnapshot().getTservers());
    assertEquals(Map.of(ResourceGroupId.DEFAULT, expected2),
        tservers.getSnapshot().getTserverGroups());

    // test when nothing has changed since last scan
    addedSeen.clear();
    deletedSeen.clear();
    tservers.scanServers();
    // Should not see any add/removes
    assertEquals(Set.of(), addedSeen);
    assertEquals(Set.of(), deletedSeen);
    assertEquals(expected2, tservers.getSnapshot().getTservers());
    assertEquals(Map.of(ResourceGroupId.DEFAULT, expected2),
        tservers.getSnapshot().getTserverGroups());

  }
}

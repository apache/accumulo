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
package org.apache.accumulo.core.zookeeper;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooCache.ZooCacheWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZooCacheTest {

  /**
   * Test class that extends ZooCache to suppress the creation of the persistent recursive watchers
   * that are created in the constructor and to provide access to the watcher.
   */
  private static class TestZooCache extends ZooCache {

    public TestZooCache(ZooSession zk, Set<String> pathsToWatch) {
      super(zk, pathsToWatch);
    }

    @Override
    protected void setupWatchers() {
      clear();
    }

    public void executeWatcher(WatchedEvent event) {
      // simulate ZooKeeper calling our Watcher
      watcher.process(event);
    }

    @Override
    long getZKClientObjectVersion() {
      return 1L;
    }

  }

  private static final String root =
      Constants.ZROOT + "/" + UUID.randomUUID().toString() + Constants.ZTSERVERS;
  private static final String ZPATH = root + "/testPath";
  private static final byte[] DATA = {(byte) 1, (byte) 2, (byte) 3, (byte) 4};
  private static final List<String> CHILDREN = java.util.Arrays.asList("huey", "dewey", "louie");

  private ZooSession zk;
  private TestZooCache zc;

  @BeforeEach
  public void setUp() {
    zk = createStrictMock(ZooSession.class);
    zc = new TestZooCache(zk, Set.of(root));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOverlappingPaths() throws Exception {
    expect(zk.getConnectionCounter()).andReturn(2L).times(2);
    zk.addPersistentRecursiveWatchers(isA(Set.class), isA(Watcher.class));
    replay(zk);
    assertThrows(IllegalArgumentException.class,
        () -> new ZooCache(zk, Set.of(root, root + "/localhost:9995")));

    Set<String> goodPaths = Set.of("/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/compactors",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/dead/tservers",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/gc/lock",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/managers/lock",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/namespaces",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/recovery",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/root_tablet",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/sservers",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/tables",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/tservers",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/users",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/mini",
        "/accumulo/8247eee6-a176-4e19-baf7-e3da965fe050/monitor/lock");
    new ZooCache(zk, goodPaths);
    verify(zk);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUnwatchedPaths() throws Exception {
    expect(zk.getConnectionCounter()).andReturn(2L).anyTimes();
    zk.addPersistentRecursiveWatchers(isA(Set.class), isA(Watcher.class));
    replay(zk);
    ZooCache cache = new ZooCache(zk, Set.of(root));
    assertThrows(IllegalStateException.class, () -> cache.get("/some/unknown/path"));
    assertThrows(IllegalStateException.class, () -> cache.getChildren("/some/unknown/path"));
    verify(zk);
  }

  @Test
  public void testGet() throws Exception {

    expect(zk.getData(eq(ZPATH), isNull(), isA(Stat.class))).andAnswer(() -> {
      ((Stat) EasyMock.getCurrentArguments()[2]).setEphemeralOwner(123456789);
      return DATA;
    });
    replay(zk);

    assertFalse(zc.dataCached(ZPATH));
    ZcStat zcStat = new ZcStat();
    assertSame(DATA, zc.get(ZPATH, zcStat));
    assertEquals(123456789, zcStat.getEphemeralOwner());
    assertSame(DATA, zc.get(ZPATH));
    verify(zk);
  }

  @Test
  public void testGet_NonExistent() throws Exception {
    expect(zk.getData(ZPATH, null, new Stat()))
        .andThrow(new KeeperException.NoNodeException(ZPATH));
    replay(zk);

    assertNull(zc.get(ZPATH));
    verify(zk);
  }

  @Test
  public void testGet_Retry_NoNode() throws Exception {
    testGet_Retry(new KeeperException.NoNodeException(ZPATH));
  }

  @Test
  public void testGet_Retry_ConnectionLoss() throws Exception {
    testGet_Retry(new KeeperException.ConnectionLossException());
  }

  @Test
  public void testGet_Retry_Interrupted() throws Exception {
    testGet_Retry(new InterruptedException());
  }

  private void testGet_Retry(Exception e) throws Exception {
    Stat existsStat = new Stat();
    expect(zk.getData(ZPATH, null, existsStat)).andThrow(e);
    if (!(e instanceof KeeperException.NoNodeException)) {
      // Retry will not happen on NoNodeException or BadVersionException
      expect(zk.getData(ZPATH, null, existsStat)).andReturn(DATA);
    }
    replay(zk);

    if (e instanceof KeeperException.NoNodeException
        || e instanceof KeeperException.BadVersionException) {
      assertNull(zc.get(ZPATH));
    } else {
      assertArrayEquals(DATA, zc.get(ZPATH));
    }
    verify(zk);
  }

  @Test
  public void testGetChildren() throws Exception {
    expect(zk.getChildren(ZPATH, null)).andReturn(CHILDREN);
    replay(zk);

    assertFalse(zc.childrenCached(ZPATH));
    assertEquals(CHILDREN, zc.getChildren(ZPATH));
    verify(zk);

    assertTrue(zc.childrenCached(ZPATH));
    // cannot check for sameness, return value is wrapped each time
    assertEquals(CHILDREN, zc.getChildren(ZPATH)); // cache hit
  }

  @Test
  public void testGetChildren_NoKids() throws Exception {
    expect(zk.getChildren(ZPATH, null)).andReturn(List.of());
    replay(zk);

    assertEquals(List.of(), zc.getChildren(ZPATH));
    verify(zk);

    assertEquals(List.of(), zc.getChildren(ZPATH)); // cache hit
  }

  @Test
  public void testGetChildren_Retry() throws Exception {
    expect(zk.getChildren(ZPATH, null)).andThrow(new KeeperException.BadVersionException(ZPATH));
    expect(zk.getChildren(ZPATH, null)).andReturn(CHILDREN);
    replay(zk);

    assertEquals(CHILDREN, zc.getChildren(ZPATH));
    verify(zk);
    assertEquals(CHILDREN, zc.getChildren(ZPATH));
  }

  @Test
  public void testGetChildren_NoNode() throws Exception {
    assertFalse(zc.childrenCached(ZPATH));
    assertFalse(zc.dataCached(ZPATH));
    expect(zk.getChildren(ZPATH, null)).andThrow(new KeeperException.NoNodeException(ZPATH));
    replay(zk);

    assertNull(zc.getChildren(ZPATH));
    verify(zk);
    assertNull(zc.getChildren(ZPATH));
    // when its discovered a node does not exists in getChildren then its also known it does not
    // exists for getData
    assertNull(zc.get(ZPATH));
    assertTrue(zc.childrenCached(ZPATH));
    assertTrue(zc.dataCached(ZPATH));
  }

  private static class TestWatcher implements ZooCacheWatcher {
    private final WatchedEvent expectedEvent;
    private boolean wasCalled;

    TestWatcher(WatchedEvent event) {
      expectedEvent = event;
      wasCalled = false;
    }

    @Override
    public void accept(WatchedEvent event) {
      assertSame(expectedEvent, event);
      wasCalled = true;
    }

    boolean wasCalled() {
      return wasCalled;
    }
  }

  @Test
  public void testWatchDataNode_Deleted() throws Exception {
    testWatchDataNode(DATA, Watcher.Event.EventType.NodeDeleted, false);
  }

  @Test
  public void testWatchDataNode_DataChanged() throws Exception {
    testWatchDataNode(DATA, Watcher.Event.EventType.NodeDataChanged, false);
  }

  @Test
  public void testWatchDataNode_Created() throws Exception {
    testWatchDataNode(null, Watcher.Event.EventType.NodeCreated, false);
  }

  @Test
  public void testWatchDataNode_NoneSyncConnected() throws Exception {
    testWatchDataNode(null, Watcher.Event.EventType.None, true);
  }

  private void testWatchDataNode(byte[] initialData, Watcher.Event.EventType eventType,
      boolean stillCached) throws Exception {
    WatchedEvent event =
        new WatchedEvent(eventType, Watcher.Event.KeeperState.SyncConnected, ZPATH);
    TestWatcher exw = new TestWatcher(event);
    zc = new TestZooCache(zk, Set.of(root));
    zc.addZooCacheWatcher(exw);

    watchData(initialData, stillCached);
    zc.executeWatcher(event);
    assertTrue(exw.wasCalled());
    assertEquals(stillCached, zc.dataCached(ZPATH));
  }

  private void watchData(byte[] initialData, boolean cached) throws Exception {
    Stat existsStat = new Stat();
    if (initialData != null) {
      expect(zk.getData(ZPATH, null, existsStat)).andReturn(initialData);
    } else {
      expect(zk.getData(ZPATH, null, existsStat))
          .andThrow(new KeeperException.NoNodeException(ZPATH));
    }
    replay(zk);
    zc.get(ZPATH);
    assertTrue(zc.dataCached(ZPATH));
  }

  @Test
  public void testWatchDataNode_Disconnected() throws Exception {
    testWatchDataNode_Clear(Watcher.Event.KeeperState.Disconnected);
  }

  @Test
  public void testWatchDataNode_Expired() throws Exception {
    testWatchDataNode_Clear(Watcher.Event.KeeperState.Expired);
  }

  @Test
  public void testGetDataThenChildren() throws Exception {
    testGetBoth(true);
  }

  @Test
  public void testGetChildrenThenDate() throws Exception {
    testGetBoth(false);
  }

  private void testGetBoth(boolean getDataFirst) throws Exception {
    assertFalse(zc.childrenCached(ZPATH));
    assertFalse(zc.dataCached(ZPATH));

    var uc1 = zc.getUpdateCount();

    if (getDataFirst) {
      expect(zk.getData(ZPATH, null, new Stat())).andReturn(DATA);
      expect(zk.getChildren(ZPATH, null)).andReturn(CHILDREN);
    } else {
      expect(zk.getChildren(ZPATH, null)).andReturn(CHILDREN);
      expect(zk.getData(ZPATH, null, new Stat())).andReturn(DATA);
    }

    replay(zk);

    if (getDataFirst) {
      var zcStat = new ZcStat();
      var data = zc.get(ZPATH, zcStat);
      assertArrayEquals(DATA, data);
    } else {
      var children = zc.getChildren(ZPATH);
      assertEquals(CHILDREN, children);
    }
    var uc2 = zc.getUpdateCount();
    assertTrue(uc1 < uc2);

    if (getDataFirst) {
      var children = zc.getChildren(ZPATH);
      assertEquals(CHILDREN, children);
    } else {
      var zcStat = new ZcStat();
      var data = zc.get(ZPATH, zcStat);
      assertArrayEquals(DATA, data);
    }
    var uc3 = zc.getUpdateCount();
    assertTrue(uc2 < uc3);

    verify(zk);

    var zcStat = new ZcStat();
    var data = zc.get(ZPATH, zcStat);
    // the stat is associated with the data so should aways see the one returned by the call to get
    // data and not get children
    assertArrayEquals(DATA, data);
    var children = zc.getChildren(ZPATH);
    assertEquals(CHILDREN, children);
    // everything is cached so the get calls on the cache should not change the update count
    assertEquals(uc3, zc.getUpdateCount());
  }

  private void testWatchDataNode_Clear(Watcher.Event.KeeperState state) throws Exception {
    WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.None, state, null);
    TestWatcher exw = new TestWatcher(event);
    zc = new TestZooCache(zk, Set.of(root));
    zc.addZooCacheWatcher(exw);

    watchData(DATA, true);
    assertTrue(zc.dataCached(ZPATH));
    zc.executeWatcher(event);
    assertTrue(exw.wasCalled());
    assertFalse(zc.dataCached(ZPATH));
  }

  @Test
  public void testWatchChildrenNode_Deleted() throws Exception {
    testWatchChildrenNode(CHILDREN, Watcher.Event.EventType.NodeDeleted, false);
  }

  @Test
  public void testWatchChildrenNode_ChildrenChanged() throws Exception {
    testWatchChildrenNode(CHILDREN, Watcher.Event.EventType.NodeChildrenChanged, true);
  }

  @Test
  public void testWatchChildrenNode_Created() throws Exception {
    testWatchChildrenNode(null, Watcher.Event.EventType.NodeCreated, false);
  }

  @Test
  public void testWatchChildrenNode_NoneSyncConnected() throws Exception {
    testWatchChildrenNode(CHILDREN, Watcher.Event.EventType.None, true);
  }

  private void testWatchChildrenNode(List<String> initialChildren,
      Watcher.Event.EventType eventType, boolean stillCached) throws Exception {
    WatchedEvent event =
        new WatchedEvent(eventType, Watcher.Event.KeeperState.SyncConnected, ZPATH);
    TestWatcher exw = new TestWatcher(event);
    zc = new TestZooCache(zk, Set.of(root));
    zc.addZooCacheWatcher(exw);

    watchChildren(initialChildren);
    zc.executeWatcher(event);
    assertTrue(exw.wasCalled());
    assertEquals(stillCached, zc.childrenCached(ZPATH));
  }

  private void watchChildren(List<String> initialChildren) throws Exception {
    if (initialChildren == null) {
      expect(zk.getChildren(ZPATH, null)).andReturn(List.of());
    } else {
      expect(zk.getChildren(ZPATH, null)).andReturn(initialChildren);
    }
    replay(zk);
    zc.getChildren(ZPATH);
    assertTrue(zc.childrenCached(ZPATH));
  }
}

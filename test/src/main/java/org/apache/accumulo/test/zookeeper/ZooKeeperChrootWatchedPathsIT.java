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
package org.apache.accumulo.test.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeCreated;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooSession.ZKUtil;
import org.apache.accumulo.harness.WithTestNames;
import org.apache.accumulo.test.util.Wait;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies the behavior of ZooKeeper to ensure that the watched event paths are those
 * relative to the chroot connect string, and not the full path in ZooKeeper. It tests this by
 * creating one non-chrooted client to modify ZooKeeper contents, and a second chrooted client to
 * watch the triggered paths.
 */
@Tag(ZOOKEEPER_TESTING_SERVER)
public class ZooKeeperChrootWatchedPathsIT extends WithTestNames {

  private static Logger log = LoggerFactory.getLogger(ZooKeeperChrootWatchedPathsIT.class);

  private static final String ROOT = "/" + UUID.randomUUID();
  private static MyZkTestingServer testZk = null;
  private static ZooSession rootZk;
  private static ZooSession chrootZk;

  private List<EventType> exactPathEvents;
  private List<EventType> childPathEvents;
  private Watcher watcher;
  private String relativeNodePath;
  private String absoluteNodePath;

  private static class MyZkTestingServer extends ZooKeeperTestingServer {
    MyZkTestingServer(File tmpDir) {
      super(tmpDir);
    }

    ZooKeeper createDirectZk(String chrootPath) throws IOException {
      var zk = new ZooKeeper(zkServer.getConnectString() + chrootPath, 30_000, null);
      zk.addAuthInfo("digest", ("accumulo:" + SECRET).getBytes(UTF_8));
      return zk;
    }
  }

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setupZk() throws Exception {
    testZk = new MyZkTestingServer(tempDir);
    rootZk = testZk.newClient();
    rootZk.create(ROOT, null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
    // create a chrooted client for the tests to use
    chrootZk = testZk.newClient(ROOT);
  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    try {
      chrootZk.close();
    } finally {
      try {
        rootZk.close();
      } finally {
        testZk.close();
      }
    }
  }

  @BeforeEach
  public void setup() {
    relativeNodePath = "/" + testName();
    absoluteNodePath = ROOT + relativeNodePath;
    exactPathEvents = Collections.synchronizedList(new ArrayList<>(8));
    childPathEvents = Collections.synchronizedList(new ArrayList<>(8));
    watcher = event -> {
      log.debug("{}", event);
      var path = event.getPath();
      var type = event.getType();
      if (path != null) {
        // should never see the root path, since the watcher is only used for chroot clients
        assertFalse(path.contains(ROOT));
        if (path.equals(relativeNodePath)) {
          exactPathEvents.add(type);
        } else if (path.startsWith(relativeNodePath + "/")) {
          childPathEvents.add(type);
        }
      }
    };
  }

  @AfterEach
  public void teardown() {
    exactPathEvents.clear();
  }

  @Test
  public void testDataWatches() throws Exception {
    var stat = chrootZk.exists(relativeNodePath, watcher); // register watcher
    assertNull(stat);

    // create and watch for the event
    rootZk.create(absoluteNodePath, "something".getBytes(UTF_8), ZooUtil.PUBLIC,
        CreateMode.PERSISTENT);
    Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeCreated)));

    // prepare for data changed event
    stat = chrootZk.exists(relativeNodePath, watcher); // re-register fired watcher for data changed
    assertNotNull(stat);

    // change data
    stat = rootZk.setData(absoluteNodePath, "somethingElse".getBytes(UTF_8), stat.getVersion());
    assertNotNull(stat);
    Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeCreated, NodeDataChanged)));

    // prepare for delete event
    stat = chrootZk.exists(relativeNodePath, watcher); // re-register fired watcher for delete
    assertNotNull(stat);

    // delete and watch for the event
    rootZk.delete(absoluteNodePath, stat.getVersion());
    Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeCreated, NodeDataChanged, NodeDeleted)));
    stat = chrootZk.exists(relativeNodePath, null);
    assertNull(stat);
  }

  @Test
  public void testChildWatches() throws Exception {
    // create parent to watch
    rootZk.create(absoluteNodePath, "something".getBytes(UTF_8), ZooUtil.PUBLIC,
        CreateMode.PERSISTENT);

    var children = chrootZk.getChildren(relativeNodePath, watcher);
    assertTrue(children.isEmpty());

    rootZk.create(absoluteNodePath + "/child1", "itsagirl".getBytes(UTF_8), ZooUtil.PUBLIC,
        CreateMode.PERSISTENT);
    Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeChildrenChanged)));

    // check children without re-registering the watcher
    children = chrootZk.getChildren(relativeNodePath, null);
    assertEquals(List.of("child1"), children);

    rootZk.create(absoluteNodePath + "/child2", "itsaboy".getBytes(UTF_8), ZooUtil.PUBLIC,
        CreateMode.PERSISTENT);
    Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeChildrenChanged)));

    // check children and re-register the watcher
    children = chrootZk.getChildren(relativeNodePath, watcher);
    children.sort(String::compareTo); // sort order is not guaranteed from ZK
    assertEquals(List.of("child1", "child2"), children);

    rootZk.create(absoluteNodePath + "/pet", "itsacat".getBytes(UTF_8), ZooUtil.PUBLIC,
        CreateMode.PERSISTENT);
    Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeChildrenChanged, NodeChildrenChanged)));

    // check children and re-register the watcher for deletes
    children = chrootZk.getChildren(relativeNodePath, watcher);
    children.sort(String::compareTo); // sort order is not guaranteed from ZK
    assertEquals(List.of("child1", "child2", "pet"), children);

    // check delete
    rootZk.delete(absoluteNodePath + "/pet", 0);
    Wait.waitFor(() -> exactPathEvents
        .equals(List.of(NodeChildrenChanged, NodeChildrenChanged, NodeChildrenChanged)));

    // check children and re-register the watcher for more deletes
    children = chrootZk.getChildren(relativeNodePath, watcher);
    children.sort(String::compareTo); // sort order is not guaranteed from ZK
    assertEquals(List.of("child1", "child2"), children);

    // delete another node, but do it recursively to ensure the watched event fires on the parent
    ZKUtil.deleteRecursive(rootZk, absoluteNodePath + "/child2");
    Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeChildrenChanged, NodeChildrenChanged,
        NodeChildrenChanged, NodeChildrenChanged)));

    // check children and re-register the watcher for more deletes
    children = chrootZk.getChildren(relativeNodePath, watcher);
    assertEquals(List.of("child1"), children);

    // delete the parent recursively to ensure the watcher still fires
    ZKUtil.deleteRecursive(rootZk, absoluteNodePath);
    Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeChildrenChanged, NodeChildrenChanged,
        NodeChildrenChanged, NodeChildrenChanged, NodeChildrenChanged)));

    var stat = chrootZk.exists(relativeNodePath, null);
    assertNull(stat);
  }

  @Test
  public void testPersistentRecursiveWatches() throws Exception {
    // this test does not cover the edge case described in ZOOKEEPER-4475, which was fixed in 3.9.0;
    // the extra events received in that edge case are of the same type tested in the
    // testChildWatches test method elsewhere in this test class; we have no reason to believe that
    // they would somehow contain extra path information when sent to the persistent recursive
    // watcher that was not supposed to receive them; a correctly implemented persistent recursive
    // watcher should ignore those extra events anyway, so even if they did contain extra path
    // information, it shouldn't matter
    try (var directChrootZk = testZk.createDirectZk(ROOT)) {

      // watcher set on node, but it doesn't exist yet
      directChrootZk.addWatch(relativeNodePath, watcher, AddWatchMode.PERSISTENT_RECURSIVE);

      // create node
      rootZk.create(absoluteNodePath, null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
      Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeCreated)) && childPathEvents.isEmpty());

      // change data
      var stat = rootZk.setData(absoluteNodePath, "somethingElse".getBytes(UTF_8), 0);
      assertNotNull(stat);
      Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeCreated, NodeDataChanged))
          && childPathEvents.isEmpty());

      // add a child
      rootZk.create(absoluteNodePath + "/child", null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
      Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeCreated, NodeDataChanged))
          && childPathEvents.equals(List.of(NodeCreated)));

      // change child data
      var childStat =
          rootZk.setData(absoluteNodePath + "/child", "somethingElse".getBytes(UTF_8), 0);
      assertNotNull(childStat);
      Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeCreated, NodeDataChanged))
          && childPathEvents.equals(List.of(NodeCreated, NodeDataChanged)));

      // delete child
      ZKUtil.deleteRecursive(rootZk, absoluteNodePath);
      Wait.waitFor(() -> exactPathEvents.equals(List.of(NodeCreated, NodeDataChanged, NodeDeleted))
          && childPathEvents.equals(List.of(NodeCreated, NodeDataChanged, NodeDeleted)));
    }
  }

}

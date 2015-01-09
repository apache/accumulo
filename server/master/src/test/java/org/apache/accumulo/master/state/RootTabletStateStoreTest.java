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
package org.apache.accumulo.master.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.DistributedStore;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.net.HostAndPort;

public class RootTabletStateStoreTest {

  static class Node {
    Node(String name) {
      this.name = name;
    }

    List<Node> children = new ArrayList<Node>();
    String name;
    byte[] value = new byte[] {};

    Node find(String name) {
      for (Node node : children)
        if (node.name.equals(name))
          return node;
      return null;
    }
  };

  static class FakeZooStore implements DistributedStore {

    Node root = new Node("/");

    private Node recurse(Node root, String[] path, int depth) {
      if (depth == path.length)
        return root;
      Node child = root.find(path[depth]);
      if (child == null)
        return null;
      return recurse(child, path, depth + 1);
    }

    private Node navigate(String path) {
      path = path.replaceAll("/$", "");
      return recurse(root, path.split("/"), 1);
    }

    @Override
    public List<String> getChildren(String path) throws DistributedStoreException {
      Node node = navigate(path);
      if (node == null)
        return Collections.emptyList();
      List<String> children = new ArrayList<String>(node.children.size());
      for (Node child : node.children)
        children.add(child.name);
      return children;
    }

    @Override
    public void put(String path, byte[] bs) throws DistributedStoreException {
      create(path).value = bs;
    }

    private Node create(String path) {
      String[] parts = path.split("/");
      return recurseCreate(root, parts, 1);
    }

    private Node recurseCreate(Node root, String[] path, int index) {
      if (path.length == index)
        return root;
      Node node = root.find(path[index]);
      if (node == null) {
        node = new Node(path[index]);
        root.children.add(node);
      }
      return recurseCreate(node, path, index + 1);
    }

    @Override
    public void remove(String path) throws DistributedStoreException {
      String[] parts = path.split("/");
      String[] parentPath = Arrays.copyOf(parts, parts.length - 1);
      Node parent = recurse(root, parentPath, 1);
      if (parent == null)
        return;
      Node child = parent.find(parts[parts.length - 1]);
      if (child != null)
        parent.children.remove(child);
    }

    @Override
    public byte[] get(String path) throws DistributedStoreException {
      Node node = navigate(path);
      if (node != null)
        return node.value;
      return null;
    }
  }

  @Test
  public void testFakeZoo() throws DistributedStoreException {
    DistributedStore store = new FakeZooStore();
    store.put("/a/b/c", "abc".getBytes());
    byte[] abc = store.get("/a/b/c");
    assertArrayEquals(abc, "abc".getBytes());
    byte[] empty = store.get("/a/b");
    assertArrayEquals(empty, "".getBytes());
    store.put("/a/b", "ab".getBytes());
    assertArrayEquals(store.get("/a/b"), "ab".getBytes());
    store.put("/a/b/b", "abb".getBytes());
    List<String> children = store.getChildren("/a/b");
    assertEquals(new HashSet<String>(children), new HashSet<String>(Arrays.asList("b", "c")));
    store.remove("/a/b/c");
    children = store.getChildren("/a/b");
    assertEquals(new HashSet<String>(children), new HashSet<String>(Arrays.asList("b")));
  }

  @Test
  public void testRootTabletStateStore() throws DistributedStoreException {
    ZooTabletStateStore tstore = new ZooTabletStateStore(new FakeZooStore());
    KeyExtent root = RootTable.EXTENT;
    String sessionId = "this is my unique session data";
    TServerInstance server = new TServerInstance(HostAndPort.fromParts("127.0.0.1", 10000), sessionId);
    List<Assignment> assignments = Collections.singletonList(new Assignment(root, server));
    tstore.setFutureLocations(assignments);
    int count = 0;
    for (TabletLocationState location : tstore) {
      assertEquals(location.extent, root);
      assertEquals(location.future, server);
      assertNull(location.current);
      count++;
    }
    assertEquals(count, 1);
    tstore.setLocations(assignments);
    count = 0;
    for (TabletLocationState location : tstore) {
      assertEquals(location.extent, root);
      assertNull(location.future);
      assertEquals(location.current, server);
      count++;
    }
    assertEquals(count, 1);
    TabletLocationState assigned = null;
    try {
      assigned = new TabletLocationState(root, server, null, null, null, false);
    } catch (BadLocationStateException e) {
      fail("Unexpected error " + e);
    }
    tstore.unassign(Collections.singletonList(assigned));
    count = 0;
    for (TabletLocationState location : tstore) {
      assertEquals(location.extent, root);
      assertNull(location.future);
      assertNull(location.current);
      count++;
    }
    assertEquals(count, 1);

    KeyExtent notRoot = new KeyExtent(new Text("0"), null, null);
    try {
      tstore.setLocations(Collections.singletonList(new Assignment(notRoot, server)));
      Assert.fail("should not get here");
    } catch (IllegalArgumentException ex) {}

    try {
      tstore.setFutureLocations(Collections.singletonList(new Assignment(notRoot, server)));
      Assert.fail("should not get here");
    } catch (IllegalArgumentException ex) {}

    TabletLocationState broken = null;
    try {
      broken = new TabletLocationState(notRoot, server, null, null, null, false);
    } catch (BadLocationStateException e) {
      fail("Unexpected error " + e);
    }
    try {
      tstore.unassign(Collections.singletonList(broken));
      Assert.fail("should not get here");
    } catch (IllegalArgumentException ex) {}
  }

  // @Test
  // public void testMetaDataStore() { } // see functional test
}

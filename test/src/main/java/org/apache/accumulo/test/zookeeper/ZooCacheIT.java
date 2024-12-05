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

import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ZooCacheIT {

  private ZooKeeperTestingServer szk = null;
  private ZooReaderWriter zk = null;

  @TempDir
  private File tempDir;

  @BeforeEach
  public void setup() throws Exception {
    szk = new ZooKeeperTestingServer(tempDir);
    zk = szk.getZooReaderWriter();
  }

  @AfterEach
  public void teardown() throws Exception {
    szk.close();
  }

  @Test
  public void testGetChildren() throws Exception {
    ZooCache zooCache = new ZooCache(zk, null);

    zk.mkdirs("/test2");
    zk.mkdirs("/test3");
    zk.mkdirs("/test3/c1");
    zk.mkdirs("/test3/c2");

    // cache non-existence of /test1 and existence of /test2 and /test3
    long uc1 = zooCache.getUpdateCount();
    assertNull(zooCache.getChildren("/test1"));
    long uc2 = zooCache.getUpdateCount();
    assertTrue(uc1 < uc2);
    assertEquals(List.of(), zooCache.getChildren("/test2"));
    long uc3 = zooCache.getUpdateCount();
    assertTrue(uc2 < uc3);
    assertEquals(Set.of("c1", "c2"), Set.copyOf(zooCache.getChildren("/test3")));
    long uc4 = zooCache.getUpdateCount();
    assertTrue(uc3 < uc4);

    // The cache should be stable now and new accesses should not change the update count
    assertNull(zooCache.getChildren("/test1"));
    // once getChildren discovers that a node does not exists, then get data will also know this
    assertNull(zooCache.get("/test1"));
    assertEquals(List.of(), zooCache.getChildren("/test2"));
    assertEquals(Set.of("c1", "c2"), Set.copyOf(zooCache.getChildren("/test3")));
    assertEquals(uc4, zooCache.getUpdateCount());

    // Had cached non-existence of "/test1", should get a notification that it was created
    zk.mkdirs("/test1");

    Wait.waitFor(() -> {
      var children = zooCache.getChildren("/test1");
      return children != null && children.isEmpty();
    });

    long uc5 = zooCache.getUpdateCount();
    assertTrue(uc4 < uc5);
    assertEquals(List.of(), zooCache.getChildren("/test1"));
    assertEquals(List.of(), zooCache.getChildren("/test2"));
    assertEquals(Set.of("c1", "c2"), Set.copyOf(zooCache.getChildren("/test3")));
    assertEquals(uc5, zooCache.getUpdateCount());

    // add a child to /test3, should get a notification of the change
    zk.mkdirs("/test3/c3");
    Wait.waitFor(() -> {
      var children = zooCache.getChildren("/test3");
      return children != null && children.size() == 3;
    });
    long uc6 = zooCache.getUpdateCount();
    assertTrue(uc5 < uc6);
    assertEquals(List.of(), zooCache.getChildren("/test1"));
    assertEquals(List.of(), zooCache.getChildren("/test2"));
    assertEquals(Set.of("c1", "c2", "c3"), Set.copyOf(zooCache.getChildren("/test3")));
    assertEquals(uc6, zooCache.getUpdateCount());

    // remove a child from /test3
    zk.delete("/test3/c2");
    Wait.waitFor(() -> {
      var children = zooCache.getChildren("/test3");
      return children != null && children.size() == 2;
    });
    long uc7 = zooCache.getUpdateCount();
    assertTrue(uc6 < uc7);
    assertEquals(List.of(), zooCache.getChildren("/test1"));
    assertEquals(List.of(), zooCache.getChildren("/test2"));
    assertEquals(Set.of("c1", "c3"), Set.copyOf(zooCache.getChildren("/test3")));
    assertEquals(uc7, zooCache.getUpdateCount());

    // remove /test2, should start caching that it does not exist
    zk.delete("/test2");
    Wait.waitFor(() -> zooCache.getChildren("/test2") == null);
    long uc8 = zooCache.getUpdateCount();
    assertTrue(uc7 < uc8);
    assertEquals(List.of(), zooCache.getChildren("/test1"));
    assertNull(zooCache.getChildren("/test2"));
    assertEquals(Set.of("c1", "c3"), Set.copyOf(zooCache.getChildren("/test3")));
    assertEquals(uc8, zooCache.getUpdateCount());

    // add /test2 back, should update
    zk.mkdirs("/test2");
    Wait.waitFor(() -> zooCache.getChildren("/test2") != null);
    long uc9 = zooCache.getUpdateCount();
    assertTrue(uc8 < uc9);
    assertEquals(List.of(), zooCache.getChildren("/test1"));
    assertEquals(List.of(), zooCache.getChildren("/test2"));
    assertEquals(Set.of("c1", "c3"), Set.copyOf(zooCache.getChildren("/test3")));
    assertEquals(uc9, zooCache.getUpdateCount());

    // make multiple changes. the cache should see all of these
    zk.delete("/test1");
    zk.mkdirs("/test2/ca");
    zk.delete("/test3/c1");
    zk.mkdirs("/test3/c4");
    zk.delete("/test3/c4");
    zk.mkdirs("/test3/c5");

    Wait.waitFor(() -> {
      var children1 = zooCache.getChildren("/test1");
      var children2 = zooCache.getChildren("/test2");
      var children3 = zooCache.getChildren("/test3");
      return children1 == null && children2 != null && children2.size() == 1 && children3 != null
          && Set.copyOf(children3).equals(Set.of("c3", "c5"));
    });
    long uc10 = zooCache.getUpdateCount();
    assertTrue(uc9 < uc10);
    assertNull(zooCache.getChildren("/test1"));
    assertEquals(List.of("ca"), zooCache.getChildren("/test2"));
    assertEquals(Set.of("c3", "c5"), Set.copyOf(zooCache.getChildren("/test3")));
    assertEquals(uc10, zooCache.getUpdateCount());
  }
}

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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.test.util.Wait;
import org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.benmanes.caffeine.cache.Ticker;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ZooCacheIT {

  public static class ZooCacheTicker implements Ticker {

    private int advanceCounter = 0;

    @Override
    public long read() {
      return System.nanoTime() + (advanceCounter * ZooCache.CACHE_DURATION.toNanos());
    }

    public void advance() {
      advanceCounter++;
    }

    public void reset() {
      advanceCounter = 0;
    }

  }

  private ZooKeeperTestingServer szk = null;
  private ZooReaderWriter zk = null;
  private ZooCacheTicker ticker = new ZooCacheTicker();

  @TempDir
  private File tempDir;

  @BeforeEach
  @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "setting ticker in test for eviction test")
  public void setup() throws Exception {
    szk = new ZooKeeperTestingServer(tempDir);
    zk = szk.getZooReaderWriter();
    ZooCache.ticker = ticker;
  }

  @AfterEach
  public void teardown() throws Exception {
    szk.close();
  }

  @Test
  public void testGetChildren() throws Exception {

    Set<String> watchesRemoved = Collections.synchronizedSet(new HashSet<>());
    Watcher watcher = event -> {
      if (event.getType() == Watcher.Event.EventType.ChildWatchRemoved
          || event.getType() == Watcher.Event.EventType.DataWatchRemoved) {
        watchesRemoved.add(event.getPath());
      }
    };

    final String root = Constants.ZROOT + UUID.randomUUID().toString();

    ZooCache zooCache = new ZooCache(root, zk, watcher);

    final String base = root + Constants.ZTSERVERS;

    zk.mkdirs(base + "/test2");
    zk.mkdirs(base + "/test3/c1");
    zk.mkdirs(base + "/test3/c2");

    // cache non-existence of /test1 and existence of /test2 and /test3
    long uc1 = zooCache.getUpdateCount();
    assertNull(zooCache.getChildren(base + "/test1"));
    long uc2 = zooCache.getUpdateCount();
    assertTrue(uc1 < uc2);
    assertEquals(List.of(), zooCache.getChildren(base + "/test2"));
    long uc3 = zooCache.getUpdateCount();
    assertTrue(uc2 < uc3);
    assertEquals(Set.of("c1", "c2"), Set.copyOf(zooCache.getChildren(base + "/test3")));
    long uc4 = zooCache.getUpdateCount();
    assertTrue(uc3 < uc4);

    // The cache should be stable now and new accesses should not change the update count
    assertNull(zooCache.getChildren(base + "/test1"));
    // once getChildren discovers that a node does not exists, then get data will also know this
    assertNull(zooCache.get(base + "/test1"));
    assertEquals(List.of(), zooCache.getChildren(base + "/test2"));
    assertEquals(Set.of("c1", "c2"), Set.copyOf(zooCache.getChildren(base + "/test3")));
    assertEquals(uc4, zooCache.getUpdateCount());

    // Had cached non-existence of "/test1", should get a notification that it was created
    zk.mkdirs(base + "/test1");

    Wait.waitFor(() -> {
      var children = zooCache.getChildren(base + "/test1");
      return children != null && children.isEmpty();
    });

    long uc5 = zooCache.getUpdateCount();
    assertTrue(uc4 < uc5);
    assertEquals(List.of(), zooCache.getChildren(base + "/test1"));
    assertEquals(List.of(), zooCache.getChildren(base + "/test2"));
    assertEquals(Set.of("c1", "c2"), Set.copyOf(zooCache.getChildren(base + "/test3")));
    long uc5b = zooCache.getUpdateCount();
    assertTrue(uc5 < uc5b);

    // add a child to /test3, should get a notification of the change
    zk.mkdirs(base + "/test3/c3");
    Wait.waitFor(() -> {
      var children = zooCache.getChildren(base + "/test3");
      System.out.println("children: " + children);
      return children != null && children.size() == 3;
    });
    long uc6 = zooCache.getUpdateCount();
    assertTrue(uc5b < uc6);
    assertEquals(List.of(), zooCache.getChildren(base + "/test1"));
    assertEquals(List.of(), zooCache.getChildren(base + "/test2"));
    assertEquals(Set.of("c1", "c2", "c3"), Set.copyOf(zooCache.getChildren(base + "/test3")));
    assertEquals(uc6, zooCache.getUpdateCount());

    // remove a child from /test3
    zk.delete(base + "/test3/c2");
    Wait.waitFor(() -> {
      var children = zooCache.getChildren(base + "/test3");
      return children != null && children.size() == 2;
    });
    long uc7 = zooCache.getUpdateCount();
    assertTrue(uc6 < uc7);
    assertEquals(List.of(), zooCache.getChildren(base + "/test1"));
    assertEquals(List.of(), zooCache.getChildren(base + "/test2"));
    assertEquals(Set.of("c1", "c3"), Set.copyOf(zooCache.getChildren(base + "/test3")));
    assertEquals(uc7, zooCache.getUpdateCount());

    // remove /test2, should start caching that it does not exist
    zk.delete(base + "/test2");
    Wait.waitFor(() -> zooCache.getChildren(base + "/test2") == null);
    long uc8 = zooCache.getUpdateCount();
    assertTrue(uc7 < uc8);
    assertEquals(List.of(), zooCache.getChildren(base + "/test1"));
    assertNull(zooCache.getChildren(base + "/test2"));
    assertEquals(Set.of("c1", "c3"), Set.copyOf(zooCache.getChildren(base + "/test3")));
    long uc8b = zooCache.getUpdateCount();
    assertTrue(uc8 < uc8b);

    // add /test2 back, should update
    zk.mkdirs(base + "/test2");
    Wait.waitFor(() -> zooCache.getChildren(base + "/test2") != null);
    long uc9 = zooCache.getUpdateCount();
    assertTrue(uc8b < uc9);
    assertEquals(List.of(), zooCache.getChildren(base + "/test1"));
    assertEquals(List.of(), zooCache.getChildren(base + "/test2"));
    assertEquals(Set.of("c1", "c3"), Set.copyOf(zooCache.getChildren(base + "/test3")));
    long uc9b = zooCache.getUpdateCount();
    assertTrue(uc9 < uc9b);

    // make multiple changes. the cache should see all of these
    zk.delete(base + "/test1");
    zk.mkdirs(base + "/test2/ca");
    zk.delete(base + "/test3/c1");
    zk.mkdirs(base + "/test3/c4");
    zk.delete(base + "/test3/c4");
    zk.mkdirs(base + "/test3/c5");

    Wait.waitFor(() -> {
      var children1 = zooCache.getChildren(base + "/test1");
      var children2 = zooCache.getChildren(base + "/test2");
      var children3 = zooCache.getChildren(base + "/test3");
      return children1 == null && children2 != null && children2.size() == 1 && children3 != null
          && Set.copyOf(children3).equals(Set.of("c3", "c5"));
    });
    long uc10 = zooCache.getUpdateCount();
    assertTrue(uc9b < uc10);
    assertNull(zooCache.getChildren(base + "/test1"));
    assertEquals(List.of("ca"), zooCache.getChildren(base + "/test2"));
    assertEquals(Set.of("c3", "c5"), Set.copyOf(zooCache.getChildren(base + "/test3")));
    assertEquals(uc10, zooCache.getUpdateCount());

    // wait for the cache to evict and clear watches
    ticker.advance();
    Wait.waitFor(() -> {
      // the cache will not run its eviction handler unless accessed, so access something that is
      // not expected to be evicted
      zooCache.getChildren(base + "/test4");
      return zooCache.childrenCached(base + "/test1") == false
          && zooCache.childrenCached(base + "/test2") == false
          && zooCache.childrenCached(base + "/test3") == false;
    });
  }
}

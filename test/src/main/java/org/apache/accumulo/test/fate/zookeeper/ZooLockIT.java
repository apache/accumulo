/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.AsyncLockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZooLockIT extends SharedMiniClusterBase {

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  static class ConnectedWatcher implements Watcher {
    volatile boolean connected = false;

    @Override
    public synchronized void process(WatchedEvent event) {
      connected = event.getState() == KeeperState.SyncConnected;
    }

    public synchronized boolean isConnected() {
      return connected;
    }
  }

  static class TestALW implements AsyncLockWatcher {

    LockLossReason reason = null;
    boolean locked = false;
    Exception exception = null;
    int changes = 0;

    @Override
    public synchronized void lostLock(LockLossReason reason) {
      this.reason = reason;
      changes++;
      this.notifyAll();
    }

    @Override
    public synchronized void acquiredLock() {
      this.locked = true;
      changes++;
      this.notifyAll();
    }

    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      this.exception = e;
      changes++;
      this.notifyAll();
    }

    public synchronized void waitForChanges(int numExpected) throws InterruptedException {
      while (changes < numExpected) {
        this.wait();
      }
    }

    @Override
    public synchronized void unableToMonitorLockNode(Throwable e) {
      changes++;
      this.notifyAll();
    }
  }

  private static final AtomicInteger pdCount = new AtomicInteger(0);

  @Test(timeout = 10000)
  public void testDeleteParent() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    assertFalse(zl.isLocked());

    ZooReaderWriter zk = new ZooReaderWriter(getCluster().getZooKeepers(), 30000, "secret");

    // intentionally created parent after lock
    zk.mkdirs(parent);

    zk.delete(parent, -1);

    zk.mkdirs(parent);

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    zl.unlock();
  }

  @Test(timeout = 10000)
  public void testNoParent() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertFalse(lw.locked);
    assertFalse(zl.isLocked());
    assertNotNull(lw.exception);
    assertNull(lw.reason);
  }

  @Test(timeout = 10000)
  public void testDeleteLock() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooReaderWriter zk = new ZooReaderWriter(getCluster().getZooKeepers(), 30000, "secret");
    zk.mkdirs(parent);

    ZooLock zl = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    zk.delete(zl.getLockPath(), -1);

    lw.waitForChanges(2);

    assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
    assertNull(lw.exception);

  }

  @Test(timeout = 10000)
  public void testDeleteWaiting() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooReaderWriter zk = new ZooReaderWriter(getCluster().getZooKeepers(), 30000, "secret");
    zk.mkdirs(parent);

    ZooLock zl = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    ZooLock zl2 = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    TestALW lw2 = new TestALW();

    zl2.lockAsync(lw2, "test2".getBytes(UTF_8));

    assertFalse(lw2.locked);
    assertFalse(zl2.isLocked());

    ZooLock zl3 = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    TestALW lw3 = new TestALW();

    zl3.lockAsync(lw3, "test3".getBytes(UTF_8));

    List<String> children = zk.getChildren(parent);
    Collections.sort(children);

    zk.delete(parent + "/" + children.get(1), -1);

    lw2.waitForChanges(1);

    assertFalse(lw2.locked);
    assertNotNull(lw2.exception);
    assertNull(lw2.reason);

    zk.delete(parent + "/" + children.get(0), -1);

    lw.waitForChanges(2);

    assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
    assertNull(lw.exception);

    lw3.waitForChanges(1);

    assertTrue(lw3.locked);
    assertTrue(zl3.isLocked());
    assertNull(lw3.exception);
    assertNull(lw3.reason);

    zl3.unlock();

  }

  @Test(timeout = 10000)
  public void testUnexpectedEvent() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeper zk = new ZooKeeper(getCluster().getZooKeepers(), 30000, watcher)) {
      zk.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      ZooLock zl = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

      assertFalse(zl.isLocked());

      // would not expect data to be set on this node, but it should not cause problems.....
      zk.setData(parent, "foo".getBytes(UTF_8), -1);

      TestALW lw = new TestALW();

      zl.lockAsync(lw, "test1".getBytes(UTF_8));

      lw.waitForChanges(1);

      assertTrue(lw.locked);
      assertTrue(zl.isLocked());
      assertNull(lw.exception);
      assertNull(lw.reason);

      // would not expect data to be set on this node either
      zk.setData(zl.getLockPath(), "bar".getBytes(UTF_8), -1);

      zk.delete(zl.getLockPath(), -1);

      lw.waitForChanges(2);

      assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
      assertNull(lw.exception);
    }

  }

  @Test(timeout = 10000)
  public void testTryLock() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeper zk = new ZooKeeper(getCluster().getZooKeepers(), 30000, watcher)) {
      zk.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      for (int i = 0; i < 10; i++) {
        zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.delete(parent, -1);
      }

      zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      TestALW lw = new TestALW();

      boolean ret = zl.tryLock(lw, "test1".getBytes(UTF_8));

      assertTrue(ret);

      // make sure still watching parent even though a lot of events occurred for the parent
      synchronized (zl) {
        Field field = zl.getClass().getDeclaredField("watchingParent");
        field.setAccessible(true);
        assertTrue((Boolean) field.get(zl));
      }

      zl.unlock();
    }
  }

  @Test(timeout = 10000)
  public void testChangeData() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();
    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeper zk = new ZooKeeper(getCluster().getZooKeepers(), 30000, watcher)) {
      zk.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      ZooLock zl = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

      TestALW lw = new TestALW();

      zl.lockAsync(lw, "test1".getBytes(UTF_8));
      assertEquals("test1", new String(zk.getData(zl.getLockPath(), null, null)));

      zl.replaceLockData("test2".getBytes(UTF_8));
      assertEquals("test2", new String(zk.getData(zl.getLockPath(), null, null)));
    }
  }

}

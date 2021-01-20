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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.AccumuloLockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
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

  static class ZooKeeperWrapper extends ZooKeeper {

    public ZooKeeperWrapper(String connectString, int sessionTimeout, Watcher watcher)
        throws IOException {
      super(connectString, sessionTimeout, watcher);
    }

    public String createOnce(String path, byte[] data, List<ACL> acl, CreateMode createMode)
        throws KeeperException, InterruptedException {
      return super.create(path, data, acl, createMode);
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
        throws KeeperException, InterruptedException {
      // Let's simulate that the first call succeeded but the client didn't get the message,
      // so the ZooKeeper client retries.
      super.create(path, data, acl, createMode);
      return super.create(path, data, acl, createMode);
    }

  }

  static class RetryLockWatcher implements AccumuloLockWatcher {

    private boolean lockHeld = false;

    @Override
    public void lostLock(LockLossReason reason) {
      this.lockHeld = false;
      System.out.println("lostLock: " + reason.toString());
    }

    @Override
    public void unableToMonitorLockNode(final Exception e) {
      System.out.println("UnableToMonitorLockNode: " + e.getMessage());
    }

    @Override
    public void acquiredLock() {
      this.lockHeld = true;
      System.out.println("acquiredLock");
    }

    @Override
    public void failedToAcquireLock(Exception e) {
      this.lockHeld = false;
      System.out.println("failedToAcquireLock");
    }

    public boolean isLockHeld() {
      return this.lockHeld;
    }
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

  static class TestALW implements AccumuloLockWatcher {

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
    public synchronized void unableToMonitorLockNode(Exception e) {
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

    zk.delete(parent);

    zk.mkdirs(parent);

    TestALW lw = new TestALW();

    zl.lock(lw, "test1".getBytes(UTF_8));

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

    zl.lock(lw, "test1".getBytes(UTF_8));

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

    zl.lock(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    zk.delete(zl.getLockPath());

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

    zl.lock(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    ZooLock zl2 = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    TestALW lw2 = new TestALW();

    zl2.lock(lw2, "test2".getBytes(UTF_8));

    assertFalse(lw2.locked);
    assertFalse(zl2.isLocked());

    ZooLock zl3 = new ZooLock(getCluster().getZooKeepers(), 30000, "secret", parent);

    TestALW lw3 = new TestALW();

    zl3.lock(lw3, "test3".getBytes(UTF_8));

    List<String> children = zk.getChildren(parent);
    Collections.sort(children);

    zk.delete(parent + "/" + children.get(1));

    lw2.waitForChanges(1);

    assertFalse(lw2.locked);
    assertNotNull(lw2.exception);
    assertNull(lw2.reason);

    zk.delete(parent + "/" + children.get(0));

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

      zl.lock(lw, "test1".getBytes(UTF_8));

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

  @Test(timeout = 60000)
  public void testLockRetryStrategy() throws Exception {
    String parent = "/zlretry";

    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeperWrapper zk1 = new ZooKeeperWrapper(getCluster().getZooKeepers(), 30000, watcher);
        ZooKeeperWrapper zk2 = new ZooKeeperWrapper(getCluster().getZooKeepers(), 30000, watcher)) {

      zk1.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));
      zk2.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      // Create the parent node
      zk1.createOnce(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      ZooReaderWriter zrw1 = new ZooReaderWriter(getCluster().getZooKeepers(), 30000, "secret") {
        @Override
        public ZooKeeper getZooKeeper() {
          return zk1;
        }
      };

      final RetryLockWatcher zlw1 = new RetryLockWatcher();
      final String zlPrefix1 = "zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#";
      ZooLock zl1 = new ZooLock(zrw1, parent) {
        @Override
        protected String getZLockPrefix() {
          return zlPrefix1;
        }
      };
      zl1.lock(zlw1, "test1".getBytes(UTF_8));
      // The call above creates two nodes in ZK because of the overridden create method in
      // ZooKeeperWrapper.
      // The nodes created are:
      // zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000000
      // zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000001
      //
      // ZooLock should realize this and remove the latter one and place a watcher on the first one
      // in case
      // the ZooKeeper ephemeral node is deleted by some external process.
      // Lastly, because zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000000 is the first child,
      // zl1 assumes
      // that it has the lock.

      ZooReaderWriter zrw2 = new ZooReaderWriter(getCluster().getZooKeepers(), 30000, "secret") {
        @Override
        public ZooKeeper getZooKeeper() {
          return zk2;
        }
      };

      final RetryLockWatcher zlw2 = new RetryLockWatcher();
      final String zlPrefix2 = "zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#";
      ZooLock zl2 = new ZooLock(zrw2, parent) {
        @Override
        protected String getZLockPrefix() {
          return zlPrefix2;
        }
      };
      zl2.lock(zlw2, "test1".getBytes(UTF_8));
      // The call above creates two nodes in ZK because of the overridden create method in
      // ZooKeeperWrapper.
      // The nodes created are:
      // zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000002
      // zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000003
      //
      // ZooLock should realize this and remove the latter one and place a watcher on the first one
      // in case
      // the ZooKeeper ephemeral node is deleted by some external process.
      // Because zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000002 is not the first child in the
      // list, it
      // places a watcher on zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000000 so that it may
      // try to acquire
      // the lock when zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000000 is removed.

      assertTrue(zlw1.isLockHeld());
      assertFalse(zlw2.isLockHeld());

      List<String> children = zk1.getChildren(parent, false);
      assertTrue(children.contains("zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000000"));
      assertFalse("this node should have been deleted",
          children.contains("zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000001"));
      assertTrue(children.contains("zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000002"));
      assertFalse("this node should have been deleted",
          children.contains("zlock#00000000-0000-0000-0000-BBBBBBBBBBBB#0000000003"));

      assertNull(zl1.getWatching());
      assertEquals("/zlretry/zlock#00000000-0000-0000-0000-AAAAAAAAAAAA#0000000000",
          zl2.getWatching());

      zl1.unlock();
      assertFalse(zlw1.isLockHeld());
      zk1.close();

      while (!zlw2.isLockHeld()) {
        LockSupport.parkNanos(50);
      }

      assertTrue(zlw2.isLockHeld());
      zl2.unlock();
      zk2.close();

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

      zl.lock(lw, "test1".getBytes(UTF_8));
      assertEquals("test1", new String(zk.getData(zl.getLockPath(), null, null)));

      zl.replaceLockData("test2".getBytes(UTF_8));
      assertEquals("test2", new String(zk.getData(zl.getLockPath(), null, null)));
    }
  }

}

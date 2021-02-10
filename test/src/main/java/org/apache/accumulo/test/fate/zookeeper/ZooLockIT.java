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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static class ZooLockWrapper extends ZooLock {

    protected ZooLockWrapper(ZooKeeper zookeeper, String path, UUID uuid) {
      super(zookeeper, path, uuid);
    }

  }

  static class RetryLockWatcher implements AccumuloLockWatcher {

    private boolean lockHeld = false;

    @Override
    public void lostLock(LockLossReason reason) {
      this.lockHeld = false;
    }

    @Override
    public void unableToMonitorLockNode(final Exception e) {}

    @Override
    public void acquiredLock() {
      this.lockHeld = true;
    }

    @Override
    public void failedToAcquireLock(Exception e) {
      this.lockHeld = false;
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

  private static ZooLock getZooLock(String parent, UUID uuid) {
    Map<String,String> props = new HashMap<>();
    props.put(Property.INSTANCE_ZK_HOST.toString(), getCluster().getZooKeepers());
    props.put(Property.INSTANCE_ZK_TIMEOUT.toString(), "30000");
    props.put(Property.INSTANCE_SECRET.toString(), "secret");
    return new ZooLock(new ConfigurationCopy(props), parent, uuid);
  }

  private static ZooLock getZooLock(ZooKeeperWrapper zkw, String parent, UUID uuid) {
    return new ZooLockWrapper(zkw, parent, uuid);
  }

  @Test(timeout = 10000)
  public void testDeleteParent() throws Exception {
    String parent = "/zltestDeleteParent-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl = getZooLock(parent, UUID.randomUUID());

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
    String parent = "/zltestNoParent-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl = getZooLock(parent, UUID.randomUUID());

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
    String parent = "/zltestDeleteLock-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooReaderWriter zk = new ZooReaderWriter(getCluster().getZooKeepers(), 30000, "secret");
    zk.mkdirs(parent);

    ZooLock zl = getZooLock(parent, UUID.randomUUID());

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

  @Test(timeout = 15000)
  public void testDeleteWaiting() throws Exception {
    String parent = "/zltestDeleteWaiting-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooReaderWriter zk = new ZooReaderWriter(getCluster().getZooKeepers(), 30000, "secret");
    zk.mkdirs(parent);

    ZooLock zl = getZooLock(parent, UUID.randomUUID());

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lock(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    ZooLock zl2 = getZooLock(parent, UUID.randomUUID());

    TestALW lw2 = new TestALW();

    zl2.lock(lw2, "test2".getBytes(UTF_8));

    assertFalse(lw2.locked);
    assertFalse(zl2.isLocked());

    ZooLock zl3 = getZooLock(parent, UUID.randomUUID());

    TestALW lw3 = new TestALW();

    zl3.lock(lw3, "test3".getBytes(UTF_8));

    List<String> children =
        ZooLock.validateAndSortChildrenByLockPrefix(parent, zk.getChildren(parent));

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
    String parent = "/zltestUnexpectedEvent-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeper zk = new ZooKeeper(getCluster().getZooKeepers(), 30000, watcher)) {
      zk.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      ZooLock zl = getZooLock(parent, UUID.randomUUID());

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
  public void testLockSerial() throws Exception {
    String parent = "/zlretryLockSerial";

    ConnectedWatcher watcher1 = new ConnectedWatcher();
    ConnectedWatcher watcher2 = new ConnectedWatcher();
    try (ZooKeeperWrapper zk1 = new ZooKeeperWrapper(getCluster().getZooKeepers(), 30000, watcher1);
        ZooKeeperWrapper zk2 =
            new ZooKeeperWrapper(getCluster().getZooKeepers(), 30000, watcher2)) {

      zk1.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));
      zk2.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));

      while (!watcher1.isConnected()) {
        Thread.sleep(200);
      }

      while (!watcher2.isConnected()) {
        Thread.sleep(200);
      }

      // Create the parent node
      zk1.createOnce(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      final RetryLockWatcher zlw1 = new RetryLockWatcher();
      ZooLock zl1 =
          getZooLock(zk1, parent, UUID.fromString("00000000-0000-0000-0000-aaaaaaaaaaaa"));
      zl1.lock(zlw1, "test1".getBytes(UTF_8));
      // The call above creates two nodes in ZK because of the overridden create method in
      // ZooKeeperWrapper.
      // The nodes created are:
      // zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000000
      // zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000001
      //
      // ZooLock should realize this and remove the latter one and place a watcher on the first one
      // in case the ZooKeeper ephemeral node is deleted by some external process.
      // Lastly, because zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000000 is the first child,
      // zl1 assumes that it has the lock.

      final RetryLockWatcher zlw2 = new RetryLockWatcher();
      ZooLock zl2 =
          getZooLock(zk2, parent, UUID.fromString("00000000-0000-0000-0000-bbbbbbbbbbbb"));
      zl2.lock(zlw2, "test1".getBytes(UTF_8));
      // The call above creates two nodes in ZK because of the overridden create method in
      // ZooKeeperWrapper.
      // The nodes created are:
      // zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000002
      // zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000003
      //
      // ZooLock should realize this and remove the latter one and place a watcher on the first one
      // in case
      // the ZooKeeper ephemeral node is deleted by some external process.
      // Because zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000002 is not the first child in the
      // list, it places a watcher on zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000000
      // so that it may try to acquire the lock when
      // zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000000 is removed.

      assertTrue(zlw1.isLockHeld());
      assertFalse(zlw2.isLockHeld());

      List<String> children = zk1.getChildren(parent, false);
      assertTrue(children.contains("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000000"));
      assertFalse("this node should have been deleted",
          children.contains("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000001"));
      assertTrue(children.contains("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000002"));
      assertFalse("this node should have been deleted",
          children.contains("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000003"));

      assertNull(zl1.getWatching());
      assertEquals("/zlretryLockSerial/zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000000",
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

  static class LockWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LockWorker.class);

    private final String parent;
    private final UUID uuid;
    private final CountDownLatch getLockLatch;
    private final CountDownLatch lockCompletedLatch;
    private final CountDownLatch unlockLatch = new CountDownLatch(1);
    private final RetryLockWatcher lockWatcher = new RetryLockWatcher();
    private volatile Exception ex = null;

    public LockWorker(final String parent, final UUID uuid, final CountDownLatch lockLatch,
        final CountDownLatch lockCompletedLatch) {
      this.parent = parent;
      this.uuid = uuid;
      this.getLockLatch = lockLatch;
      this.lockCompletedLatch = lockCompletedLatch;
    }

    public void unlock() {
      unlockLatch.countDown();
    }

    public boolean holdsLock() {
      return lockWatcher.isLockHeld();
    }

    @Override
    public void run() {
      try {
        ConnectedWatcher watcher = new ConnectedWatcher();
        try (ZooKeeperWrapper zk =
            new ZooKeeperWrapper(getCluster().getZooKeepers(), 30000, watcher)) {
          zk.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));
          while (!watcher.isConnected()) {
            Thread.sleep(50);
          }
          ZooLock zl = getZooLock(zk, parent, uuid);
          getLockLatch.countDown(); // signal we are done
          getLockLatch.await(); // wait for others to finish
          zl.lock(lockWatcher, "test1".getBytes(UTF_8)); // race to the lock
          lockCompletedLatch.countDown();
          unlockLatch.await();
          zl.unlock();
        }
      } catch (Exception e) {
        LOG.error("Error in LockWorker.run() for {}", uuid, e);
        ex = e;
      }
    }

    public Throwable getException() {
      return ex;
    }

    @Override
    public String toString() {
      return "LockWorker [name=" + uuid + ", holdsLock()=" + holdsLock() + "]";
    }

  }

  private int parseLockWorkerName(String child) {
    if (child.startsWith("zlock#00000000-0000-0000-0000-000000000000#")) {
      return 0;
    } else if (child.startsWith("zlock#00000000-0000-0000-0000-111111111111#")) {
      return 1;
    } else if (child.startsWith("zlock#00000000-0000-0000-0000-222222222222#")) {
      return 2;
    } else if (child.startsWith("zlock#00000000-0000-0000-0000-333333333333#")) {
      return 3;
    } else {
      return -1;
    }
  }

  @Test(timeout = 60000)
  public void testLockParallel() throws Exception {
    String parent = "/zlParallel";

    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeperWrapper zk = new ZooKeeperWrapper(getCluster().getZooKeepers(), 30000, watcher)) {
      zk.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));

      while (!watcher.isConnected()) {
        Thread.sleep(50);
      }
      // Create the parent node
      zk.createOnce(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      int numWorkers = 4;
      final CountDownLatch getLockLatch = new CountDownLatch(numWorkers);
      final CountDownLatch lockFinishedLatch = new CountDownLatch(numWorkers);
      final List<LockWorker> workers = new ArrayList<>(numWorkers);
      final List<Thread> threads = new ArrayList<>(numWorkers);
      for (int i = 0; i < numWorkers; i++) {
        UUID uuid = UUID.fromString(
            "00000000-0000-0000-0000-aaaaaaaaaaaa".replaceAll("a", Integer.toString(i)));
        LockWorker w = new LockWorker(parent, uuid, getLockLatch, lockFinishedLatch);
        Thread t = new Thread(w);
        workers.add(w);
        threads.add(t);
        t.start();
      }

      workers.forEach(w -> assertNull(w.getException()));
      getLockLatch.await(); // Threads compete for lock
      workers.forEach(w -> assertNull(w.getException()));
      lockFinishedLatch.await(); // Threads lock logic complete
      workers.forEach(w -> assertNull(w.getException()));

      for (int i = 4; i > 0; i--) {
        List<String> children =
            ZooLock.validateAndSortChildrenByLockPrefix(parent, zk.getChildren(parent, false));
        while (children.size() != i) {
          Thread.sleep(100);
          children = zk.getChildren(parent, false);
        }
        assertEquals(i, children.size());
        String first = children.get(0);
        int workerWithLock = parseLockWorkerName(first);
        LockWorker worker = workers.get(workerWithLock);
        assertTrue(worker.holdsLock());
        workers.forEach(w -> {
          if (w != worker) {
            assertFalse(w.holdsLock());
          }
        });
        worker.unlock();
        Thread.sleep(100); // need to wait here so that the watchers fire.
      }

      workers.forEach(w -> assertFalse(w.holdsLock()));
      workers.forEach(w -> assertNull(w.getException()));
      assertEquals(0, zk.getChildren(parent, false).size());

      threads.forEach(t -> {
        try {
          t.join();
        } catch (InterruptedException e) {
          // ignore
        }
      });
    }

  }

  @Test(timeout = 10000)
  public void testTryLock() throws Exception {
    String parent = "/zltestTryLock-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl = getZooLock(parent, UUID.randomUUID());

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
    String parent = "/zltestChangeData-" + this.hashCode() + "-l" + pdCount.incrementAndGet();
    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeper zk = new ZooKeeper(getCluster().getZooKeepers(), 30000, watcher)) {
      zk.addAuthInfo("digest", "accumulo:secret".getBytes(UTF_8));

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      ZooLock zl = getZooLock(parent, UUID.randomUUID());

      TestALW lw = new TestALW();

      zl.lock(lw, "test1".getBytes(UTF_8));
      assertEquals("test1", new String(zk.getData(zl.getLockPath(), null, null)));

      zl.replaceLockData("test2".getBytes(UTF_8));
      assertEquals("test2", new String(zk.getData(zl.getLockPath(), null, null)));
    }
  }

}

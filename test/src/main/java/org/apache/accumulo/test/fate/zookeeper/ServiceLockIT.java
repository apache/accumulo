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
package org.apache.accumulo.test.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.AccumuloLockWatcher;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooSession;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Uninterruptibles;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class ServiceLockIT {

  @TempDir
  private static File tempDir;

  private static ZooKeeperTestingServer szk = null;

  @BeforeAll
  public static void setup() throws Exception {
    szk = new ZooKeeperTestingServer(tempDir);
    szk.initPaths("/accumulo/" + InstanceId.of(UUID.randomUUID()));
  }

  @AfterAll
  public static void teardown() throws Exception {
    szk.close();
  }

  static class ZooKeeperWrapper extends ZooKeeper {

    public ZooKeeperWrapper(String connectString, int sessionTimeout, Watcher watcher)
        throws IOException {
      super(connectString, sessionTimeout, watcher);
    }

    public void createOnce(String path, byte[] data, List<ACL> acl, CreateMode createMode)
        throws KeeperException, InterruptedException {
      super.create(path, data, acl, createMode);
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

  private static class ServiceLockWrapper extends ServiceLock {

    protected ServiceLockWrapper(ZooKeeper zookeeper, ServiceLockPath path, UUID uuid) {
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

  private static ServiceLock getZooLock(ServiceLockPath parent, UUID uuid) {
    var zooKeeper = ZooSession.getAuthenticatedSession(szk.getConn(), 30000, "digest",
        "accumulo:secret".getBytes(UTF_8));
    return new ServiceLock(zooKeeper, parent, uuid);
  }

  private static ServiceLock getZooLock(ZooKeeperWrapper zkw, ServiceLockPath parent, UUID uuid) {
    return new ServiceLockWrapper(zkw, parent, uuid);
  }

  @Test
  @Timeout(10)
  public void testDeleteParent() throws Exception {
    var parent = ServiceLock
        .path("/zltestDeleteParent-" + this.hashCode() + "-l" + pdCount.incrementAndGet());

    ServiceLock zl = getZooLock(parent, UUID.randomUUID());

    assertFalse(zl.isLocked());

    ZooReaderWriter zk = szk.getZooReaderWriter();

    // intentionally created parent after lock
    zk.mkdirs(parent.toString());

    zk.delete(parent.toString());

    zk.mkdirs(parent.toString());

    TestALW lw = new TestALW();

    zl.lock(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    zl.unlock();
  }

  @Test
  @Timeout(10)
  public void testNoParent() throws Exception {
    var parent =
        ServiceLock.path("/zltestNoParent-" + this.hashCode() + "-l" + pdCount.incrementAndGet());

    ServiceLock zl = getZooLock(parent, UUID.randomUUID());

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lock(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertFalse(lw.locked);
    assertFalse(zl.isLocked());
    assertNotNull(lw.exception);
    assertNull(lw.reason);
  }

  @Test
  @Timeout(10)
  public void testDeleteLock() throws Exception {
    var parent =
        ServiceLock.path("/zltestDeleteLock-" + this.hashCode() + "-l" + pdCount.incrementAndGet());

    ZooReaderWriter zk = szk.getZooReaderWriter();
    zk.mkdirs(parent.toString());

    ServiceLock zl = getZooLock(parent, UUID.randomUUID());

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

  @Test
  @Timeout(15)
  public void testDeleteWaiting() throws Exception {
    var parent = ServiceLock
        .path("/zltestDeleteWaiting-" + this.hashCode() + "-l" + pdCount.incrementAndGet());

    ZooReaderWriter zk = szk.getZooReaderWriter();
    zk.mkdirs(parent.toString());

    ServiceLock zl = getZooLock(parent, UUID.randomUUID());

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lock(lw, "test1".getBytes(UTF_8));

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    ServiceLock zl2 = getZooLock(parent, UUID.randomUUID());

    TestALW lw2 = new TestALW();

    zl2.lock(lw2, "test2".getBytes(UTF_8));

    assertFalse(lw2.locked);
    assertFalse(zl2.isLocked());

    ServiceLock zl3 = getZooLock(parent, UUID.randomUUID());

    TestALW lw3 = new TestALW();

    zl3.lock(lw3, "test3".getBytes(UTF_8));

    List<String> children = ServiceLock.validateAndSort(parent, zk.getChildren(parent.toString()));

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

  @Test
  @Timeout(10)
  public void testUnexpectedEvent() throws Exception {
    var parent = ServiceLock
        .path("/zltestUnexpectedEvent-" + this.hashCode() + "-l" + pdCount.incrementAndGet());

    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeper zk = new ZooKeeper(szk.getConn(), 30000, watcher)) {
      ZooUtil.digestAuth(zk, "secret");

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      zk.create(parent.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      ServiceLock zl = getZooLock(parent, UUID.randomUUID());

      assertFalse(zl.isLocked());

      // would not expect data to be set on this node, but it should not cause problems.....
      zk.setData(parent.toString(), "foo".getBytes(UTF_8), -1);

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

  @Test
  @Timeout(60)
  public void testLockSerial() throws Exception {
    var parent = ServiceLock.path("/zlretryLockSerial");

    ConnectedWatcher watcher1 = new ConnectedWatcher();
    ConnectedWatcher watcher2 = new ConnectedWatcher();
    try (ZooKeeperWrapper zk1 = new ZooKeeperWrapper(szk.getConn(), 30000, watcher1);
        ZooKeeperWrapper zk2 = new ZooKeeperWrapper(szk.getConn(), 30000, watcher2)) {

      ZooUtil.digestAuth(zk1, "secret");
      ZooUtil.digestAuth(zk2, "secret");

      while (!watcher1.isConnected()) {
        Thread.sleep(200);
      }

      while (!watcher2.isConnected()) {
        Thread.sleep(200);
      }

      // Create the parent node
      zk1.createOnce(parent.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);

      final RetryLockWatcher zlw1 = new RetryLockWatcher();
      ServiceLock zl1 =
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
      ServiceLock zl2 =
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

      List<String> children = zk1.getChildren(parent.toString(), false);
      assertTrue(children.contains("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000000"));
      assertFalse(children.contains("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000001"),
          "this node should have been deleted");
      assertTrue(children.contains("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000002"));
      assertFalse(children.contains("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000003"),
          "this node should have been deleted");

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

    private final ServiceLockPath parent;
    private final UUID uuid;
    private final CountDownLatch getLockLatch;
    private final CountDownLatch lockCompletedLatch;
    private final CountDownLatch unlockLatch = new CountDownLatch(1);
    private final RetryLockWatcher lockWatcher = new RetryLockWatcher();
    private volatile Exception ex = null;

    public LockWorker(final ServiceLockPath parent, final UUID uuid, final CountDownLatch lockLatch,
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
        try (ZooKeeperWrapper zk = new ZooKeeperWrapper(szk.getConn(), 30000, watcher)) {
          ZooUtil.digestAuth(zk, "secret");

          while (!watcher.isConnected()) {
            Thread.sleep(50);
          }
          ServiceLock zl = getZooLock(zk, parent, uuid);
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

  @Test
  @Timeout(60)
  public void testLockParallel() throws Exception {
    var parent = ServiceLock.path("/zlParallel");

    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeperWrapper zk = new ZooKeeperWrapper(szk.getConn(), 30000, watcher)) {
      ZooUtil.digestAuth(zk, "secret");

      while (!watcher.isConnected()) {
        Thread.sleep(50);
      }
      // Create the parent node
      zk.createOnce(parent.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);

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
            ServiceLock.validateAndSort(parent, zk.getChildren(parent.toString(), null));
        while (children.size() != i) {
          Thread.sleep(100);
          children = zk.getChildren(parent.toString(), false);
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
      assertEquals(0, zk.getChildren(parent.toString(), false).size());

      threads.forEach(Uninterruptibles::joinUninterruptibly);
    }

  }

  @Test
  @Timeout(10)
  public void testTryLock() throws Exception {
    var parent =
        ServiceLock.path("/zltestTryLock-" + this.hashCode() + "-l" + pdCount.incrementAndGet());

    ServiceLock zl = getZooLock(parent, UUID.randomUUID());

    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeper zk = new ZooKeeper(szk.getConn(), 30000, watcher)) {
      ZooUtil.digestAuth(zk, "secret");

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      for (int i = 0; i < 10; i++) {
        zk.create(parent.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        zk.delete(parent.toString(), -1);
      }

      zk.create(parent.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

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

  @Test
  @Timeout(10)
  public void testChangeData() throws Exception {
    var parent =
        ServiceLock.path("/zltestChangeData-" + this.hashCode() + "-l" + pdCount.incrementAndGet());
    ConnectedWatcher watcher = new ConnectedWatcher();
    try (ZooKeeper zk = new ZooKeeper(szk.getConn(), 30000, watcher)) {
      ZooUtil.digestAuth(zk, "secret");

      while (!watcher.isConnected()) {
        Thread.sleep(200);
      }

      zk.create(parent.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      ServiceLock zl = getZooLock(parent, UUID.randomUUID());

      TestALW lw = new TestALW();

      zl.lock(lw, "test1".getBytes(UTF_8));
      assertEquals("test1", new String(zk.getData(zl.getLockPath(), null, null)));

      zl.replaceLockData("test2".getBytes(UTF_8));
      assertEquals("test2", new String(zk.getData(zl.getLockPath(), null, null)));
    }
  }

}

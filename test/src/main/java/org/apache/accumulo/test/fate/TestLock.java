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
package org.apache.accumulo.test.fate;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLock {
  private static final Logger log = LoggerFactory.getLogger(TestLock.class);
  final CountDownLatch lockAcquiredLatch;

  public TestLock() {
    this.lockAcquiredLatch = new CountDownLatch(1);
  }

  /**
   * Used to create a dummy lock id to be passed in the creation of a {@link UserFateStore} or a
   * {@link MetaFateStore}. Useful as a quicker and simpler alternative to
   * {@link TestLock#createTestLock(ServerContext)} for tests where reserving transactions is needed
   * AND the reservations for the test will be stored in a different location from the Managers fate
   * stores. Can always use {@link TestLock#createTestLock(ServerContext)} to be safe if unsure
   * which to use.
   */
  public static ZooUtil.LockID createDummyLockID() {
    return new ZooUtil.LockID("/path", "node", 123);
  }

  /**
   * Used to create a real lock (one held in ZK) to be passed in the creation of a
   * {@link UserFateStore} or a {@link MetaFateStore}. Useful for tests where reserving transactions
   * is needed AND the reservations for the test will be stored in the same location as the Managers
   * fate stores. This is needed so the Manager will recognize and not delete these reservations.
   * See similar {@link TestLock#createDummyLockID()}
   */
  public ServiceLock createTestLock(ServerContext context) throws InterruptedException {
    var zk = context.getZooSession();
    UUID uuid = UUID.randomUUID();
    ServiceLockPaths.ServiceLockPath slp = context.getServerPaths().createTestLockPath();
    ServiceLock lock = new ServiceLock(zk, slp, uuid);
    TestLockWatcher lw = new TestLockWatcher();
    ServiceLockData.ServiceDescriptors descriptors = new ServiceLockData.ServiceDescriptors();
    descriptors.addService(new ServiceLockData.ServiceDescriptor(uuid,
        ServiceLockData.ThriftService.NONE, "fake_test_host", ResourceGroupId.DEFAULT));
    ServiceLockData sld = new ServiceLockData(descriptors);
    String lockPath = slp.toString();
    String parentLockPath = lockPath.substring(0, lockPath.lastIndexOf("/"));

    try {
      var zrw = zk.asReaderWriter();
      zrw.putPersistentData(parentLockPath, new byte[0], NodeExistsPolicy.SKIP);
      zrw.putPersistentData(lockPath, new byte[0], NodeExistsPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Error creating path in ZooKeeper", e);
    }

    lock.lock(lw, sld);
    lockAcquiredLatch.await();

    return lock;
  }

  private final AtomicBoolean lockAcquired = new AtomicBoolean(false);

  class TestLockWatcher implements ServiceLock.AccumuloLockWatcher {

    @Override
    public void lostLock(ServiceLock.LockLossReason reason) {
      log.warn("Lost lock: " + reason.toString());
    }

    @Override
    public void unableToMonitorLockNode(Exception e) {
      log.warn("Unable to monitor lock: " + e.getMessage());
    }

    @Override
    public void acquiredLock() {
      lockAcquiredLatch.countDown();
      log.debug("Acquired ZooKeeper lock for test");
      lockAcquired.getAndSet(true);
    }

    @Override
    public void failedToAcquireLock(Exception e) {
      log.warn("Failed to acquire ZooKeeper lock for test", e);
      lockAcquired.getAndSet(false);
    }
  }
}

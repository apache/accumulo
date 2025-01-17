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
package org.apache.accumulo.core.lock;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLock.AccumuloLockWatcher;
import org.apache.accumulo.core.lock.ServiceLock.LockLossReason;
import org.apache.accumulo.core.lock.ServiceLock.LockWatcher;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.util.Halt;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceLockSupport {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceLockSupport.class);

  /**
   * Ensures that the resource group node in ZooKeeper is created for this server
   */
  public static void createNonHaServiceLockPath(Type server, ZooReaderWriter zrw,
      ServiceLockPath slp) throws KeeperException, InterruptedException {
    // The ServiceLockPath contains a resource group in the path which is not created
    // at initialization time. If it does not exist, then create it.
    String rgPath = slp.toString().substring(0, slp.toString().lastIndexOf("/" + slp.getServer()));
    LOG.debug("Creating {} resource group path in zookeeper: {}", server, rgPath);
    try {
      zrw.mkdirs(rgPath);
      zrw.putPersistentData(slp.toString(), new byte[] {}, NodeExistsPolicy.SKIP);
    } catch (NoAuthException e) {
      LOG.error("Failed to write to ZooKeeper. Ensure that"
          + " accumulo.properties, specifically instance.secret, is consistent.");
      throw e;
    }
  }

  /**
   * Lock Watcher used by Highly Available services. These are services where only instance is
   * running at a time, but another backup service can be started that will be used if the active
   * service instance fails and loses its lock in ZK.
   */
  public static class HAServiceLockWatcher implements AccumuloLockWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(HAServiceLockWatcher.class);

    private final Type server;
    private volatile boolean acquiredLock = false;
    private volatile boolean failedToAcquireLock = false;

    public HAServiceLockWatcher(Type server) {
      this.server = server;
    }

    @Override
    public void lostLock(LockLossReason reason) {
      Halt.halt(server + " lock in zookeeper lost (reason = " + reason + "), exiting!", -1);
    }

    @Override
    public void unableToMonitorLockNode(final Exception e) {
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      Halt.halt(-1, () -> LOG.error("FATAL: No longer able to monitor {} lock node", server, e));

    }

    @Override
    public synchronized void acquiredLock() {
      LOG.debug("Acquired {} lock", server);

      if (acquiredLock || failedToAcquireLock) {
        Halt.halt("Zoolock in unexpected state AL " + acquiredLock + " " + failedToAcquireLock, -1);
      }

      acquiredLock = true;
      notifyAll();
    }

    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      LOG.warn("Failed to get {} lock", server, e);

      if (e instanceof NoAuthException) {
        String msg =
            "Failed to acquire " + server + " lock due to incorrect ZooKeeper authentication.";
        LOG.error("{} Ensure instance.secret is consistent across Accumulo configuration", msg, e);
        Halt.halt(msg, -1);
      }

      if (acquiredLock) {
        Halt.halt("Zoolock in unexpected state acquiredLock true with FAL " + failedToAcquireLock,
            -1);
      }

      failedToAcquireLock = true;
      notifyAll();
    }

    public synchronized void waitForChange() {
      while (!acquiredLock && !failedToAcquireLock) {
        try {
          LOG.info("{} lock held by someone else, waiting for a change in state", server);
          wait();
        } catch (InterruptedException e) {
          // empty
        }
      }
    }

    public boolean isLockAcquired() {
      return acquiredLock;
    }

    public boolean isFailedToAcquireLock() {
      return failedToAcquireLock;
    }

  }

  /**
   * Lock Watcher used by non-HA services
   */
  public static class ServiceLockWatcher implements LockWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceLockWatcher.class);

    private final Type server;
    private final Supplier<Boolean> shuttingDown;
    private final Consumer<Type> lostLockAction;

    public ServiceLockWatcher(Type server, Supplier<Boolean> shuttingDown,
        Consumer<Type> lostLockAction) {
      this.server = server;
      this.shuttingDown = shuttingDown;
      this.lostLockAction = lostLockAction;
    }

    @Override
    public void lostLock(final LockLossReason reason) {
      Halt.halt(1, () -> {
        if (!shuttingDown.get()) {
          LOG.error("{} lost lock (reason = {}), exiting.", server, reason);
        }
        lostLockAction.accept(server);
      });
    }

    @Override
    public void unableToMonitorLockNode(final Exception e) {
      Halt.halt(1, () -> LOG.error("Lost ability to monitor {} lock, exiting.", server, e));
    }

  }

}

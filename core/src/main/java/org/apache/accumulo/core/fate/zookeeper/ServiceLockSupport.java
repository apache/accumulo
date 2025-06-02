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
package org.apache.accumulo.core.fate.zookeeper;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.fate.zookeeper.ServiceLock.AccumuloLockWatcher;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockWatcher;
import org.apache.accumulo.core.util.Halt;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceLockSupport {

  /**
   * Lock Watcher used by Highly Available services. These are services where only instance is
   * running at a time, but another backup service can be started that will be used if the active
   * service instance fails and loses its lock in ZK.
   */
  public static class HAServiceLockWatcher implements AccumuloLockWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(HAServiceLockWatcher.class);

    private final String serviceName;
    private final Supplier<Boolean> shutdownComplete;
    private volatile boolean acquiredLock = false;
    private volatile boolean failedToAcquireLock = false;

    public HAServiceLockWatcher(String serviceName, Supplier<Boolean> shutdownComplete) {
      this.serviceName = serviceName;
      this.shutdownComplete = shutdownComplete;
    }

    @Override
    public void lostLock(LockLossReason reason) {
      if (shutdownComplete.get()) {
        Halt.halt(0, serviceName + " lock in zookeeper lost (reason = " + reason
            + "), exiting cleanly because shutdown is complete.");
      } else {
        Halt.halt(-1, serviceName + " lock in zookeeper lost (reason = " + reason + "), exiting!");
      }
    }

    @Override
    public void unableToMonitorLockNode(final Exception e) {
      Halt.halt(-1, "FATAL: No longer able to monitor " + serviceName + " lock node", e);
    }

    @Override
    public synchronized void acquiredLock() {
      LOG.debug("Acquired {} lock", serviceName);

      if (acquiredLock || failedToAcquireLock) {
        Halt.halt(-1, "Zoolock in unexpected state AL " + acquiredLock + " " + failedToAcquireLock);
      }

      acquiredLock = true;
      notifyAll();
    }

    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      LOG.warn("Failed to get {} lock", serviceName, e);

      if (e instanceof NoAuthException) {
        String msg =
            "Failed to acquire " + serviceName + " lock due to incorrect ZooKeeper authentication.";
        LOG.error("{} Ensure instance.secret is consistent across Accumulo configuration", msg, e);
        Halt.halt(-1, msg);
      }

      if (acquiredLock) {
        Halt.halt(-1,
            "Zoolock in unexpected state acquiredLock true with FAL " + failedToAcquireLock);
      }

      failedToAcquireLock = true;
      notifyAll();
    }

    public synchronized void waitForChange() {
      while (!acquiredLock && !failedToAcquireLock) {
        try {
          LOG.info("{} lock held by someone else, waiting for a change in state", serviceName);
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

    private final String serviceName;
    private final Supplier<Boolean> shutdownComplete;
    private final Consumer<String> lostLockAction;

    public ServiceLockWatcher(String serviceName, Supplier<Boolean> shutdownComplete,
        Consumer<String> lostLockAction) {
      this.serviceName = serviceName;
      this.shutdownComplete = shutdownComplete;
      this.lostLockAction = lostLockAction;
    }

    @Override
    public void lostLock(final LockLossReason reason) {
      if (shutdownComplete.get()) {
        Halt.halt(0,
            serviceName + " lost lock (reason = " + reason
                + "), exiting cleanly because shutdown is complete.",
            () -> lostLockAction.accept(serviceName));
      } else {
        Halt.halt(1, serviceName + " lost lock (reason = " + reason + "), exiting.",
            () -> lostLockAction.accept(serviceName));
      }
    }

    @Override
    public void unableToMonitorLockNode(final Exception e) {
      Halt.halt(1, "Lost ability to monitor " + serviceName + " lock, exiting.", e);
    }

  }

}

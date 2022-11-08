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
package org.apache.accumulo.coordinator;

import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.core.util.Halt;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorLockWatcher implements ServiceLock.AccumuloLockWatcher {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLockWatcher.class);

  private volatile boolean acquiredLock = false;
  private volatile boolean failedToAcquireLock = false;

  @Override
  public void lostLock(LockLossReason reason) {
    Halt.halt("Coordinator lock in zookeeper lost (reason = " + reason + "), exiting!", -1);
  }

  @Override
  public void unableToMonitorLockNode(final Exception e) {
    // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
    Halt.halt(-1, () -> LOG.error("FATAL: No longer able to monitor Coordinator lock node", e));

  }

  @Override
  public synchronized void acquiredLock() {
    LOG.debug("Acquired Coordinator lock");

    if (acquiredLock || failedToAcquireLock) {
      Halt.halt("Zoolock in unexpected state AL " + acquiredLock + " " + failedToAcquireLock, -1);
    }

    acquiredLock = true;
    notifyAll();
  }

  @Override
  public synchronized void failedToAcquireLock(Exception e) {
    LOG.warn("Failed to get Coordinator lock", e);

    if (e instanceof NoAuthException) {
      String msg = "Failed to acquire Coordinator lock due to incorrect ZooKeeper authentication.";
      LOG.error("{} Ensure instance.secret is consistent across Accumulo configuration", msg, e);
      Halt.halt(msg, -1);
    }

    if (acquiredLock) {
      Halt.halt("Zoolock in unexpected state FAL " + acquiredLock + " " + failedToAcquireLock, -1);
    }

    failedToAcquireLock = true;
    notifyAll();
  }

  public synchronized void waitForChange() {
    while (!acquiredLock && !failedToAcquireLock) {
      try {
        LOG.info("Coordinator lock held by someone else, waiting for a change in state");
        wait();
      } catch (InterruptedException e) {}
    }
  }

  public boolean isAcquiredLock() {
    return acquiredLock;
  }

  public boolean isFailedToAcquireLock() {
    return failedToAcquireLock;
  }

}

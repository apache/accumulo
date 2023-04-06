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
package org.apache.accumulo.server.conf.store.impl;

import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides a barrier to block operations until a resource signals that it is ready. This class
 * encapsulates a wait / notify operation using a lock and condition. Instances are initialized with
 * the resource unavailable - call {@link #setReady()} when the resource is available
 */
public class ReadyMonitor {

  private final String resourceName;
  private final long timeout;

  // used to preserve memory ordering consistency (volatile semantics)
  private final AtomicBoolean haveConnection = new AtomicBoolean(false);
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition readySignal = lock.newCondition();

  /**
   * Create an instance of a ready monitor.
   *
   * @param resourceName the resource name guarded by this monitor (used by logging)
   * @param timeout the max time in milliseconds this will block waiting.
   */
  public ReadyMonitor(final String resourceName, final long timeout) {
    this.resourceName = resourceName;
    this.timeout = timeout;
  }

  /**
   * Non-blocking call to get ready state. It will throw is the ZooKeeper connection has closed
   * (non-recoverable condition)
   *
   * @return true if resource is ready, false otherwise
   */
  public boolean test() {
    if (isClosed.get()) {
      throw new IllegalStateException("ZooKeeper has closed the connection - cannot continue");
    }
    return haveConnection.get();
  }

  /**
   * Method blocks until the resource is ready. If the resource does not become ready within the
   * timeout an IllegalStateException is thrown.
   *
   * @throws IllegalStateException if the resource does not signal ready withing timeout.
   */
  public void isReady() {

    if (test()) {
      return;
    }

    // loop handles a spurious notification
    while (!haveConnection.get()) {
      try {
        lock.lock();
        if (!readySignal.await(timeout, TimeUnit.MILLISECONDS)) {
          var msg = resourceName + " failed to be ready within " + timeout + " ms";
          throw new IllegalStateException(msg);
        }
        // re-signal in case notification missed
        readySignal.signalAll();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(resourceName + " interrupted while waiting until ready ",
            ex);
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * ZooKeeper has closed the connection. This is a terminal state that cannot be recoved from
   * without creating a new ZooKeeper client.
   */
  public void setClosed() {
    isClosed.set(true);
  }

  /**
   * Indicate that the resource is ready.
   */
  public void setReady() {

    // already set - noop
    if (test()) {
      return;
    }

    try {
      lock.lock();
      haveConnection.set(true);
      readySignal.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Indicate that resource is NOT available and that calls to {@link #isReady()} should block.
   */
  public void clearReady() {
    haveConnection.set(false);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ReadyMonitor.class.getSimpleName() + "[", "]")
        .add("resourceName='" + resourceName + "'").add("timeout=" + timeout)
        .add("haveConnection=" + haveConnection).toString();
  }
}

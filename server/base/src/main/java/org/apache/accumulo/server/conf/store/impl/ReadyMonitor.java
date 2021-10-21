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
package org.apache.accumulo.server.conf.store.impl;

import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a barrier to block operations until a resource signals that it is ready. This class
 * encapsulates a wait / notify operation using a lock and condition. Instances are initialized with
 * the resource unavailable - call {@link #setReady()} when the resource is available
 */
public class ReadyMonitor {

  private final static Logger log = LoggerFactory.getLogger(ReadyMonitor.class);

  private final String resourceName;
  private final long timeout;

  // used to preserve memory ordering consistency (volatile semantics)
  private final AtomicBoolean isReady = new AtomicBoolean(false);

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition readySignal = lock.newCondition();

  /**
   * Create an instance of a ready monitor.
   *
   * @param resourceName
   *          the resource name guarded by this monitor (used by logging)
   * @param timeout
   *          the max time in milliseconds this will block waiting.
   */
  public ReadyMonitor(final String resourceName, final long timeout) {
    this.resourceName = resourceName;
    this.timeout = timeout;
  }

  /**
   * Non-blocking call to get ready state
   *
   * @return true if resource is ready, false otherwise
   */
  public boolean test() {
    return isReady.get();
  }

  /**
   * Method blocks until the resource is ready. If the resource does not become ready within the
   * timeout an IllegalStateException is thrown.
   *
   * @throws IllegalStateException
   *           if the resource does not signal ready withing timeout.
   */
  public void isReady() {

    if (isReady.get()) {
      return;
    }

    // loop handles a spurious notification
    while (!isReady.get()) {
      try {
        lock.lock();
        if (!readySignal.await(timeout, TimeUnit.MILLISECONDS)) {
          var msg = resourceName + " failed to be ready within " + timeout + " ms";
          log.trace(msg);
          throw new IllegalStateException(msg);
        }
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
   * Indicate that the resource is ready.
   */
  public void setReady() {

    // already set - noop
    if (test()) {
      return;
    }

    try {
      lock.lock();
      isReady.set(true);
      readySignal.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Indicate that resource is NOT available and that calls to {@link #isReady()} should block.
   */
  public void clearReady() {
    isReady.set(false);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ReadyMonitor.class.getSimpleName() + "[", "]")
        .add("resourceName='" + resourceName + "'").add("timeout=" + timeout)
        .add("isReady=" + isReady).toString();
  }
}

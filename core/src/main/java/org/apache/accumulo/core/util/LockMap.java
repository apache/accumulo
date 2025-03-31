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
package org.apache.accumulo.core.util;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;

/**
 * Allows locking on arbitrary keys by creating and disposes of per key locks as needed.
 */
public class LockMap<T> {

  // This class relies on the atomic nature of the ConcurrentHashMap compute function. Not all
  // concurrent map impls have an atomic compute function.
  private final ConcurrentHashMap<T,PerKeyLockImpl> locks = new ConcurrentHashMap<>();

  public interface PerKeyLock extends AutoCloseable {

    /**
     * Unlocks the per key lock and if no other thread is using the lock disposes of it.
     */
    @Override
    void close();
  }

  private class PerKeyLockImpl implements PerKeyLock {
    private final Lock lock = new ReentrantLock();
    private int refCount = 1;
    private final T key;

    private PerKeyLockImpl(T key) {
      this.key = key;
    }

    @Override
    public void close() {
      lock.unlock();
      returnLock(key, this);
    }
  }

  /**
   * Finds an existing per key lock or creates a new one if none exists, then locks it. Will never
   * create more than one lock for the same key during the same time period.
   */
  public PerKeyLock lock(T key) {
    Objects.requireNonNull(key);
    var perKeyLock = getOrCreateLock(key);
    perKeyLock.lock.lock();
    return perKeyLock;
  }

  private PerKeyLockImpl getOrCreateLock(T key) {
    // Create a lock for extents as needed. Assuming only one thread will execute the compute
    // function per key.
    var rcl = locks.compute(key, (k, v) -> {
      if (v == null) {
        return new PerKeyLockImpl(key);
      } else {
        Preconditions.checkState(v.refCount > 0);
        v.refCount++;
        return v;
      }
    });
    return rcl;
  }

  private void returnLock(T key, PerKeyLockImpl rcl) {
    // Dispose of the lock if nothing else is using it. Assuming only one thread will execute the
    // compute function per key.
    locks.compute(key, (k, v) -> {
      Objects.requireNonNull(v);
      Preconditions.checkState(v.refCount > 0);
      // while the ref count was >0 the reference should not have changed in the map
      Preconditions.checkState(v == rcl);
      v.refCount--;
      if (v.refCount == 0) {
        // No threads are using the lock anymore, so dispose of it
        return null;
      } else {
        return v;
      }
    });
  }

}

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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.QueueLock;
import org.junit.jupiter.api.Test;

public class DistributedReadWriteLockTest {

  // Non-zookeeper version of QueueLock
  public static class MockQueueLock implements QueueLock {

    long next = 0L;
    final SortedMap<Long,byte[]> locks = new TreeMap<>();

    @Override
    public synchronized SortedMap<Long,byte[]> getEarlierEntries(long entry) {
      SortedMap<Long,byte[]> result = new TreeMap<>();
      result.putAll(locks.headMap(entry + 1));
      return result;
    }

    @Override
    public synchronized void removeEntry(long entry) {
      synchronized (locks) {
        locks.remove(entry);
        locks.notifyAll();
      }
    }

    @Override
    public synchronized long addEntry(byte[] data) {
      long result;
      synchronized (locks) {
        locks.put(result = next++, data);
        locks.notifyAll();
      }
      return result;
    }
  }

  // some data that is probably not going to update atomically
  static class SomeData {
    private AtomicIntegerArray data = new AtomicIntegerArray(100);
    private AtomicInteger counter = new AtomicInteger();

    void read() {
      for (int i = 0; i < data.length(); i++) {
        assertEquals(counter.get(), data.get(i));
      }
    }

    void write() {
      int nextCount = counter.incrementAndGet();
      for (int i = data.length() - 1; i >= 0; i--) {
        data.set(i, nextCount);
      }
    }
  }

  @Test
  public void testLock() throws Exception {
    final SomeData data = new SomeData();
    data.write();
    data.read();
    QueueLock qlock = new MockQueueLock();

    final ReadWriteLock locker = new DistributedReadWriteLock(qlock, "locker1".getBytes());
    final Lock readLock = locker.readLock();
    final Lock writeLock = locker.writeLock();
    readLock.lock();
    readLock.unlock();
    writeLock.lock();
    writeLock.unlock();
    readLock.lock();
    readLock.unlock();

    // do a bunch of reads/writes in separate threads, look for inconsistent updates
    Thread[] threads = new Thread[2];
    for (int i = 0; i < threads.length; i++) {
      final int which = i;
      threads[i] = new Thread(() -> {
        if (which % 2 == 0) {
          final Lock wl = locker.writeLock();
          wl.lock();
          try {
            data.write();
          } finally {
            wl.unlock();
          }
        } else {
          final Lock rl = locker.readLock();
          rl.lock();
          data.read();
          try {
            data.read();
          } finally {
            rl.unlock();
          }
        }
      });
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
  }

}

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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.QueueLock;
import org.apache.accumulo.core.fate.zookeeper.FateLock.FateLockEntry;
import org.junit.jupiter.api.Test;

public class DistributedReadWriteLockTest {

  // Non-zookeeper version of QueueLock
  public static class MockQueueLock implements QueueLock {

    long next = 0L;
    final SortedMap<Long,FateLockEntry> locks = new TreeMap<>();

    @Override
    public synchronized SortedMap<Long,Supplier<FateLockEntry>>
        getEntries(BiPredicate<Long,Supplier<FateLockEntry>> predicate) {
      SortedMap<Long,Supplier<FateLockEntry>> result = new TreeMap<>();
      locks.forEach((seq, lockData) -> {
        if (predicate.test(seq, () -> lockData)) {
          result.put(seq, () -> lockData);
        }
      });
      return result;
    }

    @Override
    public synchronized void removeEntry(FateLockEntry data, long entry) {
      synchronized (locks) {
        locks.remove(entry);
        locks.notifyAll();
      }
    }

    @Override
    public synchronized long addEntry(FateLockEntry entry) {
      long result;
      synchronized (locks) {
        locks.put(result = next++, entry);
        locks.notifyAll();
      }
      return result;
    }
  }

  // some data that is probably not going to update atomically
  static class SomeData {
    private final AtomicIntegerArray data = new AtomicIntegerArray(100);
    private final AtomicInteger counter = new AtomicInteger();

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

    final ReadWriteLock locker =
        new DistributedReadWriteLock(qlock, FateId.from(FateInstanceType.USER, UUID.randomUUID()));
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

  @Test
  public void testFateLockEntrySerDes() {
    var uuid = UUID.randomUUID();
    var entry = FateLockEntry.from(LockType.READ, FateId.from(FateInstanceType.USER, uuid));
    assertEquals(LockType.READ, entry.getLockType());
    assertEquals(FateId.from(FateInstanceType.USER, uuid), entry.getFateId());

    String serialized = entry.serialize();
    var deserialized = FateLockEntry.deserialize(serialized);
    assertEquals(entry, deserialized);
  }
}

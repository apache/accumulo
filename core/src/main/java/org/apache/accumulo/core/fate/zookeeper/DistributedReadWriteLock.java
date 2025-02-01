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

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.zookeeper.FateLock.FateLockEntry;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ReadWriteLock that can be implemented in ZooKeeper. Features the ability to store data with the
 * lock, and recover the lock using that data to find the lock.
 */
public class DistributedReadWriteLock implements java.util.concurrent.locks.ReadWriteLock {

  public enum LockType {
    READ, WRITE,
  }

  // This kind of lock can be easily implemented by ZooKeeper
  // You make an entry at the bottom of the queue, readers run when there are no writers ahead of
  // them,
  // a writer only runs when they are at the top of the queue.
  public interface QueueLock {
    SortedMap<Long,Supplier<FateLockEntry>>
        getEntries(BiPredicate<Long,Supplier<FateLockEntry>> predicate);

    void removeEntry(FateLockEntry data, long seq);

    long addEntry(FateLockEntry entry);
  }

  private static final Logger log = LoggerFactory.getLogger(DistributedReadWriteLock.class);

  public static interface DistributedLock extends Lock {
    LockType getType();
  }

  static class ReadLock implements DistributedLock {

    final QueueLock qlock;
    final FateId fateId;
    long entry = -1;

    ReadLock(QueueLock qlock, FateId fateId) {
      this.qlock = qlock;
      this.fateId = fateId;
    }

    // for recovery
    ReadLock(QueueLock qlock, FateId fateId, long entry) {
      this.qlock = qlock;
      this.fateId = fateId;
      this.entry = entry;
    }

    @Override
    public LockType getType() {
      return LockType.READ;
    }

    @Override
    public void lock() {
      while (true) {
        try {
          if (tryLock(1, DAYS)) {
            return;
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          log.warn("Interrupted while waiting to acquire lock", ex);
        }
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      while (!Thread.currentThread().isInterrupted()) {
        if (tryLock(100, MILLISECONDS)) {
          return;
        }
      }
    }

    @Override
    public boolean tryLock() {
      if (entry == -1) {
        entry = qlock.addEntry(FateLockEntry.from(this.getType(), this.fateId));
        log.info("Added lock entry {} fateId {} lockType {}", entry, fateId, getType());
      }

      SortedMap<Long,Supplier<FateLockEntry>> entries =
          qlock.getEntries((seq, lockData) -> seq <= entry);
      for (Entry<Long,Supplier<FateLockEntry>> entry : entries.entrySet()) {
        if (entry.getKey().equals(this.entry)) {
          return true;
        }
        FateLockEntry lockEntry = entry.getValue().get();
        if (lockEntry.getLockType() == LockType.WRITE) {
          return false;
        }
      }
      throw new IllegalStateException("Did not find our own lock in the queue: " + this.entry
          + " fateId " + this.fateId + " lockType " + getType());
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      long now = System.currentTimeMillis();
      long returnTime = now + MILLISECONDS.convert(time, unit);
      while (returnTime > now) {
        if (tryLock()) {
          return true;
        }
        // TODO: do something better than poll - ACCUMULO-1310
        UtilWaitThread.sleep(100);
        now = System.currentTimeMillis();
      }
      return false;
    }

    @Override
    public void unlock() {
      if (entry == -1) {
        return;
      }
      log.debug("Removing lock entry {} fateId {} lockType {}", entry, this.fateId, getType());
      qlock.removeEntry(new FateLockEntry(this.getType(), this.fateId), entry);
      entry = -1;
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
  }

  static class WriteLock extends ReadLock {

    WriteLock(QueueLock qlock, FateId fateId) {
      super(qlock, fateId);
    }

    WriteLock(QueueLock qlock, FateId fateId, long entry) {
      super(qlock, fateId, entry);
    }

    @Override
    public LockType getType() {
      return LockType.WRITE;
    }

    @Override
    public boolean tryLock() {
      if (entry == -1) {
        entry = qlock.addEntry(FateLockEntry.from(this.getType(), this.fateId));
        log.info("Added lock entry {} fateId {} lockType {}", entry, this.fateId, getType());
      }
      SortedMap<Long,Supplier<FateLockEntry>> entries =
          qlock.getEntries((seq, locData) -> seq <= entry);
      Iterator<Entry<Long,Supplier<FateLockEntry>>> iterator = entries.entrySet().iterator();
      if (!iterator.hasNext()) {
        throw new IllegalStateException("Did not find our own lock in the queue: " + this.entry
            + " fateId " + this.fateId + " lockType " + getType());
      }
      return iterator.next().getKey().equals(entry);
    }
  }

  private final QueueLock qlock;
  private final FateId fateId;

  public DistributedReadWriteLock(QueueLock qlock, FateId fateId) {
    this.qlock = qlock;
    this.fateId = fateId;
  }

  public static DistributedLock recoverLock(QueueLock qlock, FateId fateId) {
    SortedMap<Long,Supplier<FateLockEntry>> entries = qlock.getEntries((seq, lockData) -> {
      // TODO fix this lambda
      return lockData.get().fateId.equals(fateId);
    });

    switch (entries.size()) {
      case 0:
        return null;
      case 1:
        var entry = entries.entrySet().iterator().next();
        FateLockEntry lockEntry = entry.getValue().get();
        switch (lockEntry.getLockType()) {
          case READ:
            return new ReadLock(qlock, lockEntry.getFateId(), entry.getKey());
          case WRITE:
            return new WriteLock(qlock, lockEntry.getFateId(), entry.getKey());
          default:
            throw new IllegalStateException("Unknown lock type " + lockEntry.getLockType());
        }
      default:
        throw new IllegalStateException("Found more than one lock node " + entries);
    }
  }

  @Override
  public DistributedLock readLock() {
    return new ReadLock(qlock, fateId);
  }

  @Override
  public DistributedLock writeLock() {
    return new WriteLock(qlock, fateId);
  }
}

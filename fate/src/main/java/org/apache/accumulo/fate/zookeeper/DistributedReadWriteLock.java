/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.fate.zookeeper;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

// A ReadWriteLock that can be implemented in ZooKeeper.  Features the ability to store data
// with the lock, and recover the lock using that data to find the lock.
public class DistributedReadWriteLock implements java.util.concurrent.locks.ReadWriteLock {

  static enum LockType {
    READ, WRITE,
  };

  // serializer for lock type and user data
  static class ParsedLock {
    public ParsedLock(LockType type, byte[] userData) {
      this.type = type;
      this.userData = Arrays.copyOf(userData, userData.length);
    }

    public ParsedLock(byte[] lockData) {
      if (lockData == null || lockData.length < 1)
        throw new IllegalArgumentException();

      int split = -1;
      for (int i = 0; i < lockData.length; i++) {
        if (lockData[i] == ':') {
          split = i;
          break;
        }
      }

      if (split == -1)
        throw new IllegalArgumentException();

      this.type = LockType.valueOf(new String(lockData, 0, split, UTF_8));
      this.userData = Arrays.copyOfRange(lockData, split + 1, lockData.length);
    }

    public LockType getType() {
      return type;
    }

    public byte[] getUserData() {
      return userData;
    }

    public byte[] getLockData() {
      byte typeBytes[] = type.name().getBytes(UTF_8);
      byte[] result = new byte[userData.length + 1 + typeBytes.length];
      System.arraycopy(typeBytes, 0, result, 0, typeBytes.length);
      result[typeBytes.length] = ':';
      System.arraycopy(userData, 0, result, typeBytes.length + 1, userData.length);
      return result;
    }

    private LockType type;
    private byte[] userData;
  }

  // This kind of lock can be easily implemented by ZooKeeper
  // You make an entry at the bottom of the queue, readers run when there are no writers ahead of them,
  // a writer only runs when they are at the top of the queue.
  public interface QueueLock {
    SortedMap<Long,byte[]> getEarlierEntries(long entry);

    void removeEntry(long entry);

    long addEntry(byte[] data);
  }

  public static final Logger log = Logger.getLogger(DistributedReadWriteLock.class);

  static class ReadLock implements Lock {

    QueueLock qlock;
    byte[] userData;
    long entry = -1;

    ReadLock(QueueLock qlock, byte[] userData) {
      this.qlock = qlock;
      this.userData = userData;
    }

    // for recovery
    ReadLock(QueueLock qlock, byte[] userData, long entry) {
      this.qlock = qlock;
      this.userData = userData;
      this.entry = entry;
    }

    protected LockType lockType() {
      return LockType.READ;
    }

    @Override
    public void lock() {
      while (true) {
        try {
          if (tryLock(1, TimeUnit.DAYS))
            return;
        } catch (InterruptedException ex) {
          // ignored
        }
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      while (!Thread.currentThread().isInterrupted()) {
        if (tryLock(100, TimeUnit.MILLISECONDS))
          return;
      }
    }

    @Override
    public boolean tryLock() {
      if (entry == -1) {
        entry = qlock.addEntry(new ParsedLock(this.lockType(), this.userData).getLockData());
        log.info("Added lock entry " + entry + " userData " + new String(this.userData, UTF_8) + " lockType " + lockType());
      }
      SortedMap<Long,byte[]> entries = qlock.getEarlierEntries(entry);
      for (Entry<Long,byte[]> entry : entries.entrySet()) {
        ParsedLock parsed = new ParsedLock(entry.getValue());
        if (entry.getKey().equals(this.entry))
          return true;
        if (parsed.type == LockType.WRITE)
          return false;
      }
      throw new IllegalStateException("Did not find our own lock in the queue: " + this.entry + " userData " + new String(this.userData, UTF_8) + " lockType "
          + lockType());
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      long now = System.currentTimeMillis();
      long returnTime = now + TimeUnit.MILLISECONDS.convert(time, unit);
      while (returnTime > now) {
        if (tryLock())
          return true;
        // TODO: do something better than poll - ACCUMULO-1310
        UtilWaitThread.sleep(100);
        now = System.currentTimeMillis();
      }
      return false;
    }

    @Override
    public void unlock() {
      if (entry == -1)
        return;
      log.debug("Removing lock entry " + entry + " userData " + new String(this.userData, UTF_8) + " lockType " + lockType());
      qlock.removeEntry(entry);
      entry = -1;
    }

    @Override
    public Condition newCondition() {
      throw new NotImplementedException();
    }
  }

  static class WriteLock extends ReadLock {

    WriteLock(QueueLock qlock, byte[] userData) {
      super(qlock, userData);
    }

    WriteLock(QueueLock qlock, byte[] userData, long entry) {
      super(qlock, userData, entry);
    }

    @Override
    protected LockType lockType() {
      return LockType.WRITE;
    }

    @Override
    public boolean tryLock() {
      if (entry == -1) {
        entry = qlock.addEntry(new ParsedLock(this.lockType(), this.userData).getLockData());
        log.info("Added lock entry " + entry + " userData " + new String(this.userData, UTF_8) + " lockType " + lockType());
      }
      SortedMap<Long,byte[]> entries = qlock.getEarlierEntries(entry);
      Iterator<Entry<Long,byte[]>> iterator = entries.entrySet().iterator();
      if (!iterator.hasNext())
        throw new IllegalStateException("Did not find our own lock in the queue: " + this.entry + " userData " + new String(this.userData, UTF_8)
            + " lockType " + lockType());
      if (iterator.next().getKey().equals(entry))
        return true;
      return false;
    }
  }

  private QueueLock qlock;
  private byte[] data;

  public DistributedReadWriteLock(QueueLock qlock, byte[] data) {
    this.qlock = qlock;
    this.data = Arrays.copyOf(data, data.length);
  }

  static public Lock recoverLock(QueueLock qlock, byte[] data) {
    SortedMap<Long,byte[]> entries = qlock.getEarlierEntries(Long.MAX_VALUE);
    for (Entry<Long,byte[]> entry : entries.entrySet()) {
      ParsedLock parsed = new ParsedLock(entry.getValue());
      if (Arrays.equals(data, parsed.getUserData())) {
        switch (parsed.getType()) {
          case READ:
            return new ReadLock(qlock, parsed.getUserData(), entry.getKey());
          case WRITE:
            return new WriteLock(qlock, parsed.getUserData(), entry.getKey());
        }
      }
    }
    return null;
  }

  @Override
  public Lock readLock() {
    return new ReadLock(qlock, data);
  }

  @Override
  public Lock writeLock() {
    return new WriteLock(qlock, data);
  }
}

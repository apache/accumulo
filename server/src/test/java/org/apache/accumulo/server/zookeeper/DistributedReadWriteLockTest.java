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
package org.apache.accumulo.server.zookeeper;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import junit.framework.Assert;

import org.apache.accumulo.server.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.server.zookeeper.DistributedReadWriteLock.QueueLock;
import org.junit.Test;

public class DistributedReadWriteLockTest {
  
  // Non-zookeeper version of QueueLock
  public static class MockQueueLock implements QueueLock {
    
    long next = 0L;
    SortedMap<Long,byte[]> locks = new TreeMap<Long,byte[]>();
    
    @Override
    synchronized public SortedMap<Long,byte[]> getEarlierEntries(long entry) {
      SortedMap<Long,byte[]> result = new TreeMap<Long,byte[]>();
      result.putAll(locks.headMap(entry + 1));
      return result;
    }
    
    @Override
    synchronized public void removeEntry(long entry) {
      synchronized (locks) {
        locks.remove(entry);
        locks.notifyAll();
      }
    }
    
    @Override
    synchronized public long addEntry(byte[] data) {
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
    volatile int[] data = new int[100];
    volatile int counter;
    
    void read() {
      for (int i = 0; i < data.length; i++)
        Assert.assertEquals(counter, data[i]);
    }
    
    void write() {
      ++counter;
      for (int i = data.length - 1; i >= 0; i--)
        data[i] = counter;
    }
  }
  
  @Test
  public void testLock() throws Exception {
    final SomeData data = new SomeData();
    data.write();
    data.read();
    QueueLock qlock = new MockQueueLock();
    
    final ReadWriteLock locker = new DistributedReadWriteLock(qlock, "locker1".getBytes());
    Lock readLock = locker.readLock();
    Lock writeLock = locker.writeLock();
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
      threads[i] = new Thread() {
        public void run() {
          if (which % 2 == 0) {
            Lock wl = locker.writeLock();
            wl.lock();
            try {
              data.write();
            } finally {
              wl.unlock();
            }
          } else {
            Lock rl = locker.readLock();
            rl.lock();
            data.read();
            try {
              data.read();
            } finally {
              rl.unlock();
            }
          }
        }
      };
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
  }
  
}

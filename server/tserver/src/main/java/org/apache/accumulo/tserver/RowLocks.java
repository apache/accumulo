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
package org.apache.accumulo.tserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.tserver.ConditionalMutationSet.DeferFilter;
import org.apache.accumulo.tserver.data.ServerConditionalMutation;

/**
 *
 */
class RowLocks {

  private Map<ByteSequence,RowLock> rowLocks = new HashMap<ByteSequence,RowLock>();

  static class RowLock {
    ReentrantLock rlock;
    int count;
    ByteSequence rowSeq;

    RowLock(ReentrantLock rlock, ByteSequence rowSeq) {
      this.rlock = rlock;
      this.count = 0;
      this.rowSeq = rowSeq;
    }

    public boolean tryLock() {
      return rlock.tryLock();
    }

    public void lock() {
      rlock.lock();
    }

    public void unlock() {
      rlock.unlock();
    }
  }

  private RowLock getRowLock(ArrayByteSequence rowSeq) {
    RowLock lock = rowLocks.get(rowSeq);
    if (lock == null) {
      lock = new RowLock(new ReentrantLock(), rowSeq);
      rowLocks.put(rowSeq, lock);
    }

    lock.count++;
    return lock;
  }

  private void returnRowLock(RowLock lock) {
    if (lock.count == 0)
      throw new IllegalStateException();
    lock.count--;

    if (lock.count == 0) {
      rowLocks.remove(lock.rowSeq);
    }
  }

  List<RowLock> acquireRowlocks(Map<KeyExtent,List<ServerConditionalMutation>> updates, Map<KeyExtent,List<ServerConditionalMutation>> deferred) {
    ArrayList<RowLock> locks = new ArrayList<RowLock>();

    // assume that mutations are in sorted order to avoid deadlock
    synchronized (rowLocks) {
      for (List<ServerConditionalMutation> scml : updates.values()) {
        for (ServerConditionalMutation scm : scml) {
          locks.add(getRowLock(new ArrayByteSequence(scm.getRow())));
        }
      }
    }

    HashSet<ByteSequence> rowsNotLocked = null;

    // acquire as many locks as possible, not blocking on rows that are already locked
    if (locks.size() > 1) {
      for (RowLock rowLock : locks) {
        if (!rowLock.tryLock()) {
          if (rowsNotLocked == null)
            rowsNotLocked = new HashSet<ByteSequence>();
          rowsNotLocked.add(rowLock.rowSeq);
        }
      }
    } else {
      // if there is only one lock, then wait for it
      locks.get(0).lock();
    }

    if (rowsNotLocked != null) {

      final HashSet<ByteSequence> rnlf = rowsNotLocked;
      // assume will get locks needed, do something expensive otherwise
      ConditionalMutationSet.defer(updates, deferred, new DeferFilter() {
        @Override
        public void defer(List<ServerConditionalMutation> scml, List<ServerConditionalMutation> okMutations, List<ServerConditionalMutation> deferred) {
          for (ServerConditionalMutation scm : scml) {
            if (rnlf.contains(new ArrayByteSequence(scm.getRow())))
              deferred.add(scm);
            else
              okMutations.add(scm);

          }
        }
      });

      ArrayList<RowLock> filteredLocks = new ArrayList<RowLock>();
      ArrayList<RowLock> locksToReturn = new ArrayList<RowLock>();
      for (RowLock rowLock : locks) {
        if (rowsNotLocked.contains(rowLock.rowSeq)) {
          locksToReturn.add(rowLock);
        } else {
          filteredLocks.add(rowLock);
        }
      }

      synchronized (rowLocks) {
        for (RowLock rowLock : locksToReturn) {
          returnRowLock(rowLock);
        }
      }

      locks = filteredLocks;
    }
    return locks;
  }

  void releaseRowLocks(List<RowLock> locks) {
    for (RowLock rowLock : locks) {
      rowLock.unlock();
    }

    synchronized (rowLocks) {
      for (RowLock rowLock : locks) {
        returnRowLock(rowLock);
      }
    }
  }

}

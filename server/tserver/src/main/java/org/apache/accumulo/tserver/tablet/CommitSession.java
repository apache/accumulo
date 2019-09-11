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
package org.apache.accumulo.tserver.tablet;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.log.DfsLogger;

import com.google.common.base.Preconditions;

public class CommitSession {

  private final long seq;
  private final InMemoryMap memTable;
  private final Tablet committer;

  private final Lock commitSessionLock = new ReentrantLock();
  private final Condition noOutstandingCommits =
      commitSessionLock.newCondition(); 

  private final AtomicInteger commitsInProgress;
  private long maxCommittedTime = Long.MIN_VALUE;

  CommitSession(Tablet committer, long seq, InMemoryMap imm) {
    this.seq = seq;
    this.memTable = imm;
    this.committer = committer;
    this.commitsInProgress = new AtomicInteger();
  }

  public long getWALogSeq() {
    return seq;
  }

  public void decrementCommitsInProgress() {
    this.commitSessionLock.lock();
    try {
      Preconditions.checkState(this.commitsInProgress.get() > 0);
      final int newCount = this.commitsInProgress.decrementAndGet();
      if (newCount == 0) {
        this.noOutstandingCommits.signal();
      }
    } finally {
      this.commitSessionLock.unlock();
    }
  }

  public void incrementCommitsInProgress() {
    this.commitSessionLock.lock();
    try {
      Preconditions.checkState(this.commitsInProgress.get() >= 0);
      this.commitsInProgress.incrementAndGet();
    } finally {
      this.commitSessionLock.unlock();
    }
  }

  public void waitForCommitsToFinish() {
    this.commitSessionLock.lock();
    try {
      while (this.commitsInProgress.get() > 0) {
        this.noOutstandingCommits.awaitUninterruptibly();
      }
    } finally {
      this.commitSessionLock.unlock();
    }
  }

  public void abortCommit() {
    committer.abortCommit(this);
  }

  public void commit(List<Mutation> mutations) {
    committer.commit(this, mutations);
  }

  public boolean beginUpdatingLogsUsed(DfsLogger copy, boolean mincFinish) {
    return committer.beginUpdatingLogsUsed(memTable, copy, mincFinish);
  }

  public void finishUpdatingLogsUsed() {
    committer.finishUpdatingLogsUsed();
  }

  public int getLogId() {
    return committer.getLogId();
  }

  public KeyExtent getExtent() {
    return committer.getExtent();
  }

  public void updateMaxCommittedTime(long time) {
    maxCommittedTime = Math.max(time, maxCommittedTime);
  }

  public long getMaxCommittedTime() {
    if (maxCommittedTime == Long.MIN_VALUE)
      throw new IllegalStateException("Tried to read max committed time when it was never set");
    return maxCommittedTime;
  }

  public void mutate(List<Mutation> mutations, int count) {
    memTable.mutate(mutations, count);
  }
}

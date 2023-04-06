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
package org.apache.accumulo.tserver.tablet;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitSession {

  private static final Logger log = LoggerFactory.getLogger(CommitSession.class);

  private final long seq;
  private final InMemoryMap memTable;
  private final Tablet committer;

  private int commitsInProgress;
  private long maxCommittedTime = Long.MIN_VALUE;

  CommitSession(Tablet committer, long seq, InMemoryMap imm) {
    this.seq = seq;
    this.memTable = imm;
    this.committer = committer;
    commitsInProgress = 0;
  }

  public long getWALogSeq() {
    return seq;
  }

  public void decrementCommitsInProgress() {
    if (commitsInProgress < 1) {
      throw new IllegalStateException("commitsInProgress = " + commitsInProgress);
    }

    commitsInProgress--;
    if (commitsInProgress == 0) {
      committer.notifyAll();
    }
  }

  public void incrementCommitsInProgress() {
    if (commitsInProgress < 0) {
      throw new IllegalStateException("commitsInProgress = " + commitsInProgress);
    }

    commitsInProgress++;
  }

  public void waitForCommitsToFinish() {
    while (commitsInProgress > 0) {
      try {
        committer.wait(50);
      } catch (InterruptedException e) {
        log.warn("InterruptedException", e);
      }
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
    if (maxCommittedTime == Long.MIN_VALUE) {
      throw new IllegalStateException("Tried to read max committed time when it was never set");
    }
    return maxCommittedTime;
  }

  public void mutate(List<Mutation> mutations, int count) {
    memTable.mutate(mutations, count);
  }
}

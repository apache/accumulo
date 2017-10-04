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

import java.util.Objects;

import org.apache.accumulo.tserver.compaction.MajorCompactionReason;

final class CompactionRunner implements Runnable, Comparable<CompactionRunner> {

  private final Tablet tablet;
  private final MajorCompactionReason reason;
  private final long queued;

  public CompactionRunner(Tablet tablet, MajorCompactionReason reason) {
    this.tablet = tablet;
    queued = System.currentTimeMillis();
    this.reason = reason;
  }

  @Override
  public void run() {

    tablet.majorCompact(reason, queued);

    // if there is more work to be done, queue another major compaction
    synchronized (tablet) {
      if (reason == MajorCompactionReason.NORMAL && tablet.needsMajorCompaction(reason))
        tablet.initiateMajorCompaction(reason);
    }
  }

  // We used to synchronize on the Tablet before fetching this information,
  // but this method is called by the compaction queue thread to re-order the compactions.
  // The compaction queue holds a lock during this sort.
  // A tablet lock can be held while putting itself on the queue, so we can't lock the tablet
  // while pulling information used to sort the tablets in the queue, or we may get deadlocked.
  // See ACCUMULO-1110.
  private int getNumFiles() {
    return tablet.getDatafileManager().getNumFiles();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(reason) + Objects.hashCode(queued) + getNumFiles();
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || (obj != null && obj instanceof CompactionRunner && 0 == compareTo((CompactionRunner) obj));
  }

  @Override
  public int compareTo(CompactionRunner o) {
    int cmp = reason.compareTo(o.reason);
    if (cmp != 0)
      return cmp;

    if (reason == MajorCompactionReason.USER || reason == MajorCompactionReason.CHOP) {
      // for these types of compactions want to do the oldest first
      cmp = (int) (queued - o.queued);
      if (cmp != 0)
        return cmp;
    }

    return o.getNumFiles() - this.getNumFiles();
  }
}

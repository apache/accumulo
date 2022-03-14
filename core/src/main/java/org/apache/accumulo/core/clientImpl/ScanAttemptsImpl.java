/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.ScanServerDispatcher;
import org.apache.accumulo.core.spi.scan.ScanServerDispatcher.ScanAttempt;

import com.google.common.collect.Sets;

public class ScanAttemptsImpl {

  public static class ScanAttemptImpl
      implements org.apache.accumulo.core.spi.scan.ScanServerDispatcher.ScanAttempt {

    private final ScanServerDispatcher.Action requestedAction;
    private final long time;
    private final Result result;
    private volatile long mutationCount = Long.MAX_VALUE;

    public ScanAttemptImpl(ScanServerDispatcher.Action action, long time, Result result) {
      this.requestedAction = action;
      this.time = time;
      this.result = result;
    }

    @Override
    public long getTime() {
      return time;
    }

    @Override
    public Result getResult() {
      return result;
    }

    @Override
    public ScanServerDispatcher.Action getAction() {
      return requestedAction;
    }

    // TODO this comparator is a bit iffy.. added the hashcode at the end in case two diff attempts
    // have the same time and result
    private static Comparator<ScanAttempt> COMPARATOR =
        Comparator.comparingLong(ScanAttempt::getTime).reversed()
            .thenComparing(ScanAttempt::getResult).thenComparing(ScanAttempt::hashCode);

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((this.result == null) ? 0 : this.result.hashCode());
      result = prime * result + (int) (time ^ (time >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ScanAttemptImpl other = (ScanAttemptImpl) obj;
      if (result != other.result)
        return false;
      if (time != other.time)
        return false;
      return true;
    }

    @Override
    public int compareTo(ScanAttempt o) {
      return COMPARATOR.compare(this, o);
    }

    private void setMutationCount(long mc) {
      this.mutationCount = mc;
    }

    public long getMutationCount() {
      return mutationCount;
    }
  }

  private SortedSet<ScanAttempt> attempts = new ConcurrentSkipListSet<>();
  private ConcurrentSkipListMap<TabletId,SortedSet<ScanAttempt>> attemptsByTablet =
      new ConcurrentSkipListMap<>();
  private long mutationCounter = 0;

  public void add(ScanServerDispatcher.Action action, long time, ScanAttempt.Result result) {

    ScanAttemptImpl sa = new ScanAttemptImpl(action, time, result);

    attempts.add(sa);
    action.getTablets().forEach(tablet -> attemptsByTablet
        .computeIfAbsent(tablet, k -> new ConcurrentSkipListSet<>()).add(sa));

    synchronized (this) {
      // now that the scan attempt obj is added to all concurrent data structs, make it visible

      // need to atomically increment the counter AND set the counter on the object
      sa.setMutationCount(mutationCounter++);
    }

  }

  ScanServerDispatcher.ScanAttempts snapshot() {
    // allows only seeing scan attempt objs that were added before this call

    long snapMC;
    synchronized (ScanAttemptsImpl.this) {
      snapMC = mutationCounter;
    }

    return new ScanServerDispatcher.ScanAttempts() {
      @Override
      public Collection<ScanAttempt> all() {
        return Sets.filter(attempts,
            attempt -> ((ScanAttemptImpl) attempt).getMutationCount() <= snapMC);
      }

      @Override
      public SortedSet<ScanAttempt> forTablet(TabletId tablet) {
        return Sets.filter(attemptsByTablet.getOrDefault(tablet, Collections.emptySortedSet()),
            attempt -> ((ScanAttemptImpl) attempt).getMutationCount() <= snapMC);
      }
    };
  }
}

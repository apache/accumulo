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

import com.google.common.collect.Sets;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.EcScanManager;
import org.apache.accumulo.core.spi.scan.EcScanManager.ScanAttempt;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ScanAttemptsImpl {

  public static class ScanAttemptImpl
      implements org.apache.accumulo.core.spi.scan.EcScanManager.ScanAttempt {

    private final EcScanManager.Action requestedAction;
    private final String server;
    private final long time;
    private final Result result;
    private final TabletId tablet;
    private volatile long mutationCount = Long.MAX_VALUE;

    public ScanAttemptImpl(EcScanManager.Action action, String server, long time, Result result,
        TabletId tablet) {
      this.requestedAction = action;
      this.server = server;
      this.time = time;
      this.result = result;
      this.tablet = tablet;
    }

    @Override public long getTime() {
      return time;
    }

    @Override public Result getResult() {
      return result;
    }

    @Override public EcScanManager.Action getAction() {
      return requestedAction;
    }

    @Override public String getServer() {
      return server;
    }

    @Override public TabletId getTablet() {
      return tablet;
    }

    private static Comparator<ScanAttempt> COMPARATOR =
        Comparator.comparingLong(ScanAttempt::getTime).reversed()
            .thenComparing(ScanAttempt::getServer).thenComparing(ScanAttempt::getResult)
            .thenComparing(ScanAttempt::getAction);

    @Override public int compareTo(ScanAttempt o) {
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
  private ConcurrentSkipListMap<String,SortedSet<ScanAttempt>> attemptsByServer =
      new ConcurrentSkipListMap<>();
  private long mutationCounter = 0;

  public void add(EcScanManager.Action action, String server, long time, ScanAttempt.Result result,
      TabletId tablet) {

    ScanAttemptImpl sa = new ScanAttemptImpl(action, server, time, result, tablet);

    attempts.add(sa);
    attemptsByTablet.computeIfAbsent(tablet, k -> new ConcurrentSkipListSet<>()).add(sa);
    attemptsByServer.computeIfAbsent(server, k -> new ConcurrentSkipListSet<>()).add(sa);

    synchronized (this) {
      // now that the scan attempt obj is added to all concurrent data structs, make it visible

      // need to atomically increment the counter AND set the counter on the object
      sa.setMutationCount(mutationCounter++);
    }

  }

  EcScanManager.ScanAttempts snapshot() {
    // allows only seeing scan attempt objs that were added before this call

    long snapMC;
    synchronized (ScanAttemptsImpl.this) {
      snapMC = mutationCounter;
    }

    return new EcScanManager.ScanAttempts() {
      @Override public Collection<ScanAttempt> all() {
        return Sets.filter(attempts,
            attempt -> ((ScanAttemptImpl) attempt).getMutationCount() <= snapMC);
      }

      @Override public SortedSet<ScanAttempt> forServer(String server) {
        return Sets.filter(attemptsByServer.getOrDefault(server, Collections.emptySortedSet()),
            attempt -> ((ScanAttemptImpl) attempt).getMutationCount() <= snapMC);
      }

      @Override public SortedSet<ScanAttempt> forTablet(TabletId tablet) {
        return Sets.filter(attemptsByServer.getOrDefault(tablet, Collections.emptySortedSet()),
            attempt -> ((ScanAttemptImpl) attempt).getMutationCount() <= snapMC);
      }
    };
  }
}

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
package org.apache.accumulo.core.clientImpl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.ScanServerAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to track scan attempts for the ScanServerSelector. Its designed to accept
 * updates concurrently (useful for the batch scanner) and offers a snapshot. When a snapshot is
 * obtained it will not change, this class will still accept updates after generating a snapshot.
 * Snapshots are useful for ensuring that authors of ScanServerSelector plugins do not have to
 * consider strange concurrency issues when writing a plugin.
 */
public class ScanServerAttemptsImpl {

  private static final Logger LOG = LoggerFactory.getLogger(ScanServerAttemptsImpl.class);

  static class ScanServerAttemptImpl implements ScanServerAttempt {

    private final String server;
    private final long time;
    private final Result result;
    private volatile long mutationCount = Long.MAX_VALUE;

    ScanServerAttemptImpl(Result result, String server, long time) {
      this.result = result;
      this.server = Objects.requireNonNull(server);
      this.time = time;
    }

    @Override
    public String getServer() {
      return server;
    }

    @Override
    public long getEndTime() {
      return time;
    }

    @Override
    public Result getResult() {
      return result;
    }

    private void setMutationCount(long mc) {
      this.mutationCount = mc;
    }

    public long getMutationCount() {
      return mutationCount;
    }
  }

  private final Map<TabletId,Collection<ScanServerAttemptImpl>> attempts =
      new ConcurrentHashMap<>();
  private long mutationCounter = 0;

  private void add(TabletId tablet, ScanServerAttempt.Result result, String server, long endTime) {

    ScanServerAttemptImpl sa = new ScanServerAttemptImpl(result, server, endTime);

    attempts.computeIfAbsent(tablet, k -> ConcurrentHashMap.newKeySet()).add(sa);

    synchronized (this) {
      // now that the scan attempt obj is added to all concurrent data structs, make it visible
      // need to atomically increment the counter AND set the counter on the object
      sa.setMutationCount(mutationCounter++);
    }

  }

  public interface ScanAttemptReporter {
    void report(ScanServerAttempt.Result result);
  }

  ScanAttemptReporter createReporter(String server, TabletId tablet) {
    return new ScanAttemptReporter() {
      @Override
      public void report(ScanServerAttempt.Result result) {
        LOG.trace("Received result: {}", result);
        add(tablet, result, server, System.currentTimeMillis());
      }
    };
  }

  /**
   * Creates and returns a snapshot of {@link ScanServerAttempt} objects that were added before this
   * call
   *
   * @return TabletIds mapped to a collection of {@link ScanServerAttempt} objects associated with
   *         that TabletId
   */
  Map<TabletId,Collection<ScanServerAttemptImpl>> snapshot() {

    final long mutationCounterSnapshot;
    synchronized (ScanServerAttemptsImpl.this) {
      mutationCounterSnapshot = mutationCounter;
    }

    Map<TabletId,Collection<ScanServerAttemptImpl>> result = new ConcurrentHashMap<>();

    attempts.forEach((tabletId, scanAttempts) -> {

      // filter out ScanServerScanAttempt objects that were added after this call
      List<ScanServerAttemptImpl> filteredScanAttempts = scanAttempts.stream()
          .filter(scanAttempt -> scanAttempt.getMutationCount() < mutationCounterSnapshot)
          .collect(Collectors.toList());

      // only add an entry to the map if there are ScanServerScanAttempt objects for the current
      // TabletId
      if (!filteredScanAttempts.isEmpty())
        result.put(tabletId, filteredScanAttempts);

    });

    return result;
  }
}

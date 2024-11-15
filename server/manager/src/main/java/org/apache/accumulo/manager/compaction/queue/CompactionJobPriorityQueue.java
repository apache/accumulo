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
package org.apache.accumulo.manager.compaction.queue;

import static com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * Priority Queue for {@link CompactionJob}s that supports a maximum size. When a job is added and
 * the queue is at maximum size the new job is compared to the lowest job with the lowest priority.
 * The new job will either replace the lowest priority one or be ignored.
 *
 * <p>
 * When jobs are added for tablet, any previous jobs that are queued for the tablet are removed.
 * </p>
 */
public class CompactionJobPriorityQueue {

  private static final Logger log = LoggerFactory.getLogger(CompactionJobPriorityQueue.class);

  private final CompactorGroupId groupId;

  @VisibleForTesting
  static final int FUTURE_CHECK_THRESHOLD = 10_000;

  private class CjpqKey implements Comparable<CjpqKey> {
    private final CompactionJob job;

    // this exists to make every entry unique even if the job is the same, this is done because a
    // treeset is used as a queue
    private final long seq;

    CjpqKey(CompactionJob job) {
      this.job = job;
      this.seq = nextSeq.incrementAndGet();
    }

    @Override
    public int compareTo(CjpqKey oe) {
      int cmp = CompactionJobPrioritizer.JOB_COMPARATOR.compare(this.job, oe.job);
      if (cmp == 0) {
        cmp = Long.compare(seq, oe.seq);
      }
      return cmp;
    }

    @Override
    public int hashCode() {
      return Objects.hash(job, seq);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CjpqKey cjqpKey = (CjpqKey) o;
      return seq == cjqpKey.seq && job.equals(cjqpKey.job);
    }
  }

  // There are two reasons for using a TreeMap instead of a PriorityQueue. First the maximum size
  // behavior is not supported with a PriorityQueue. Second a PriorityQueue does not support
  // efficiently removing entries from anywhere in the queue. Efficient removal is needed for the
  // case where tablets decided to issues different compaction jobs than what is currently queued.
  private final TreeMap<CjpqKey,CompactionJobQueues.MetaJob> jobQueue;
  private final AtomicInteger maxSize;
  private final AtomicLong rejectedJobs;
  private final AtomicLong dequeuedJobs;
  private final ArrayDeque<CompletableFuture<CompactionJobQueues.MetaJob>> futures;
  private long futuresAdded = 0;
  private final Map<KeyExtent,Timer> jobAges;
  private final Supplier<CompactionJobPriorityQueueStats> jobQueueStats;
  private final AtomicReference<Optional<io.micrometer.core.instrument.Timer>> jobQueueTimer;

  private static class TabletJobs {
    final long generation;
    final HashSet<CjpqKey> jobs;

    private TabletJobs(long generation, HashSet<CjpqKey> jobs) {
      this.generation = generation;
      this.jobs = jobs;
    }
  }

  // This map tracks what jobs a tablet currently has in the queue. Its used to efficiently remove
  // jobs in the queue when new jobs are queued for a tablet.
  private final Map<KeyExtent,TabletJobs> tabletJobs;

  private final AtomicLong nextSeq = new AtomicLong(0);

  public CompactionJobPriorityQueue(CompactorGroupId groupId, int maxSize) {
    this.jobQueue = new TreeMap<>();
    this.maxSize = new AtomicInteger(maxSize);
    this.tabletJobs = new HashMap<>();
    this.groupId = groupId;
    this.rejectedJobs = new AtomicLong(0);
    this.dequeuedJobs = new AtomicLong(0);
    this.futures = new ArrayDeque<>();
    this.jobAges = new ConcurrentHashMap<>();
    this.jobQueueStats = Suppliers.memoizeWithExpiration(
        () -> new CompactionJobPriorityQueueStats(jobAges), 5, TimeUnit.SECONDS);
    this.jobQueueTimer = new AtomicReference<>(Optional.empty());
  }

  public synchronized void removeOlderGenerations(Ample.DataLevel level, long currGeneration) {
    List<KeyExtent> removals = new ArrayList<>();

    tabletJobs.forEach((extent, jobs) -> {
      if (Ample.DataLevel.of(extent.tableId()) == level && jobs.generation < currGeneration) {
        removals.add(extent);
      }
    });

    if (!removals.isEmpty()) {
      log.trace("Removed {} queued tablets that no longer need compaction for {} {}",
          removals.size(), groupId, level);
    }

    // Also clears jobAge timer for tablets that do not need compaction anymore
    removals.forEach(ke -> removePreviousSubmissions(ke, true));
  }

  /**
   * @return the number of jobs added. If the queue is closed returns -1
   */
  public synchronized int add(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs,
      long generation) {
    Preconditions.checkArgument(jobs.stream().allMatch(job -> job.getGroup().equals(groupId)));

    // Do not clear jobAge timers, they are cleared later at the end of this method
    // if there are no jobs for the extent so we do not reset the timer for an extent
    // that had previous jobs and still has jobs
    removePreviousSubmissions(tabletMetadata.getExtent(), false);

    HashSet<CjpqKey> newEntries = new HashSet<>(jobs.size());

    int jobsAdded = 0;
    outer: for (CompactionJob job : jobs) {
      var future = futures.poll();
      while (future != null) {
        // its expected that if futures are present then the queue is empty, if this is not true
        // then there is a bug
        Preconditions.checkState(jobQueue.isEmpty());
        // Queue should be empty so jobAges should be empty
        Preconditions.checkState(jobAges.isEmpty());
        if (future.complete(new CompactionJobQueues.MetaJob(job, tabletMetadata))) {
          // successfully completed a future with this job, so do not need to queue the job
          jobsAdded++;
          // Record a time of 0 as job as we were able to complete immediately and there
          // were no jobs waiting
          jobQueueTimer.get().ifPresent(jqt -> jqt.record(Duration.ZERO));
          continue outer;
        } // else the future was canceled or timed out so could not complete it
        future = futures.poll();
      }

      CjpqKey cjqpKey = addJobToQueue(tabletMetadata, job);
      if (cjqpKey != null) {
        checkState(newEntries.add(cjqpKey));
        jobsAdded++;
      } else {
        // The priority for this job was lower than all other priorities and not added
        // In this case we will return true even though a subset of the jobs, or none,
        // were added
      }
    }

    if (!newEntries.isEmpty()) {
      checkState(tabletJobs.put(tabletMetadata.getExtent(), new TabletJobs(generation, newEntries))
          == null);
      jobAges.computeIfAbsent(tabletMetadata.getExtent(), e -> Timer.startNew());
    } else {
      jobAges.remove(tabletMetadata.getExtent());
    }

    return jobsAdded;
  }

  public synchronized int getMaxSize() {
    return maxSize.get();
  }

  public synchronized void setMaxSize(int maxSize) {
    Preconditions.checkArgument(maxSize > 0,
        "Maximum size of the Compaction job priority queue must be greater than 0");
    this.maxSize.set(maxSize);
  }

  public long getRejectedJobs() {
    return rejectedJobs.get();
  }

  public long getDequeuedJobs() {
    return dequeuedJobs.get();
  }

  public synchronized long getQueuedJobs() {
    return jobQueue.size();
  }

  public synchronized long getLowestPriority() {
    if (jobQueue.isEmpty()) {
      return 0;
    }
    return jobQueue.lastKey().job.getPriority();
  }

  public synchronized CompactionJobQueues.MetaJob poll() {
    var first = jobQueue.pollFirstEntry();

    if (first != null) {
      dequeuedJobs.getAndIncrement();
      var extent = first.getValue().getTabletMetadata().getExtent();
      var timer = jobAges.get(extent);
      checkState(timer != null);
      jobQueueTimer.get().ifPresent(jqt -> jqt.record(timer.elapsed()));
      log.trace("Compaction job age for {} is {} ms", extent, timer.elapsed(TimeUnit.MILLISECONDS));
      Set<CompactionJobPriorityQueue.CjpqKey> jobs = tabletJobs.get(extent).jobs;
      checkState(jobs.remove(first.getKey()));
      // If there are no more jobs for this extent we can remove the timer, otherwise
      // we need to reset it
      if (jobs.isEmpty()) {
        tabletJobs.remove(extent);
        jobAges.remove(extent);
      } else {
        timer.restart();
      }
    }
    return first == null ? null : first.getValue();
  }

  public synchronized CompletableFuture<CompactionJobQueues.MetaJob> getAsync() {
    var job = poll();
    if (job != null) {
      return CompletableFuture.completedFuture(job);
    }

    // There is currently nothing in the queue, so create an uncompleted future and queue it up to
    // be completed when something does arrive.
    CompletableFuture<CompactionJobQueues.MetaJob> future = new CompletableFuture<>();
    futures.add(future);
    futuresAdded++;
    // Handle the case where nothing is ever being added to this queue and futures are constantly
    // being obtained and cancelled. If nothing is done these canceled futures would just keep
    // building up in memory. The following code periodically checks to see if there are canceled
    // futures to remove.
    if (futuresAdded % FUTURE_CHECK_THRESHOLD == 0
        && futures.size() >= 2 * FUTURE_CHECK_THRESHOLD) {
      futures.removeIf(CompletableFuture::isDone);
      // It is not expected that the future we just created would be done, if it were it would have
      // been removed.
      Preconditions.checkState(!future.isDone());
    }
    return future;
  }

  @VisibleForTesting
  synchronized int futuresSize() {
    return futures.size();
  }

  // exists for tests
  synchronized CompactionJobQueues.MetaJob peek() {
    var firstEntry = jobQueue.firstEntry();
    return firstEntry == null ? null : firstEntry.getValue();
  }

  private void removePreviousSubmissions(KeyExtent extent, boolean removeJobAges) {
    CompactionJobPriorityQueue.TabletJobs prevJobs = tabletJobs.get(extent);
    if (prevJobs != null) {
      prevJobs.jobs.forEach(jobQueue::remove);
      tabletJobs.remove(extent);
      if (removeJobAges) {
        jobAges.remove(extent);
        log.trace("Removed jobAge timer for tablet {} that no longer needs compaction", extent);
      }
    }
  }

  private CjpqKey addJobToQueue(TabletMetadata tabletMetadata, CompactionJob job) {
    if (jobQueue.size() >= maxSize.get()) {
      var lastEntry = jobQueue.lastKey();
      if (job.getPriority() <= lastEntry.job.getPriority()) {
        // the queue is full and this job has a lower or same priority than the lowest job in the
        // queue, so do not add it
        rejectedJobs.getAndIncrement();
        return null;
      } else {
        // the new job has a higher priority than the lowest job in the queue, so remove the lowest
        if (jobQueue.pollLastEntry() != null) {
          rejectedJobs.getAndIncrement();
        }
      }
    }

    var key = new CjpqKey(job);
    jobQueue.put(key, new CompactionJobQueues.MetaJob(job, tabletMetadata));
    return key;
  }

  public synchronized void clearIfInactive(Duration duration) {
    // IF the minimum age of jobs in the queue is older than the
    // duration then clear all the maps as this queue is now
    // considered inactive
    if (getJobQueueStats().getMinAge().compareTo(duration) > 0) {
      jobQueue.clear();
      tabletJobs.clear();
      jobAges.clear();
    }
  }

  public CompactionJobPriorityQueueStats getJobQueueStats() {
    return jobQueueStats.get();
  }

  public void setJobQueueTimerCallback(io.micrometer.core.instrument.Timer jobQueueTimer) {
    this.jobQueueTimer.set(Optional.of(jobQueueTimer));
  }

  // Used for unit testing, can return the map as is because
  // it is a ConcurrentHashMap
  @VisibleForTesting
  Map<KeyExtent,Timer> getJobAges() {
    return jobAges;
  }

  public static class CompactionJobPriorityQueueStats {
    private final Duration minAge;
    private final Duration maxAge;
    private final Duration avgAge;

    @VisibleForTesting
    CompactionJobPriorityQueueStats(Map<KeyExtent,Timer> jobAges) {
      final Stat stats = new Stat();
      jobAges.values().forEach(t -> stats.addStat(t.elapsed(TimeUnit.MILLISECONDS)));
      this.minAge = Duration.ofMillis(stats.min());
      this.maxAge = Duration.ofMillis(stats.max());
      this.avgAge = Duration.ofMillis(Math.round(stats.mean()));
    }

    public Duration getMinAge() {
      return minAge;
    }

    public Duration getMaxAge() {
      return maxAge;
    }

    public Duration getAvgAge() {
      return avgAge;
    }
  }
}

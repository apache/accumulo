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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;

import com.google.common.base.Preconditions;

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
  // ELASTICITY_TODO unit test this class
  private final CompactorGroupId groupId;

  private class CjpqKey implements Comparable<CjpqKey> {
    private final CompactionJob job;

    // this exists to make every entry unique even if the job is the same, this is done because a
    // treeset is used as a queue
    private final long seq;

    CjpqKey(CompactionJob job) {
      this.job = job;
      this.seq = nextSeq++;
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
  private final int maxSize;
  private final AtomicLong rejectedJobs;
  private final AtomicLong dequeuedJobs;

  // This map tracks what jobs a tablet currently has in the queue. Its used to efficiently remove
  // jobs in the queue when new jobs are queued for a tablet.
  private final Map<KeyExtent,List<CjpqKey>> tabletJobs;

  private long nextSeq;

  private boolean closed = false;

  public CompactionJobPriorityQueue(CompactorGroupId groupId, int maxSize) {
    this.jobQueue = new TreeMap<>();
    this.maxSize = maxSize;
    this.tabletJobs = new HashMap<>();
    this.groupId = groupId;
    this.rejectedJobs = new AtomicLong(0);
    this.dequeuedJobs = new AtomicLong(0);
  }

  public synchronized boolean add(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs) {
    if (closed) {
      return false;
    }

    Preconditions.checkArgument(jobs.stream().allMatch(job -> job.getGroup().equals(groupId)));

    removePreviousSubmissions(tabletMetadata.getExtent());

    List<CjpqKey> newEntries = new ArrayList<>(jobs.size());

    for (CompactionJob job : jobs) {
      CjpqKey cjqpKey = addJobToQueue(tabletMetadata, job);
      if (cjqpKey != null) {
        newEntries.add(cjqpKey);
      }
    }

    if (!newEntries.isEmpty()) {
      checkState(tabletJobs.put(tabletMetadata.getExtent(), newEntries) == null);
    }

    return true;
  }

  public long getMaxSize() {
    return maxSize;
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
      List<CjpqKey> jobs = tabletJobs.get(extent);
      checkState(jobs.remove(first.getKey()));
      if (jobs.isEmpty()) {
        tabletJobs.remove(extent);
      }
    }
    return first == null ? null : first.getValue();
  }

  public synchronized boolean closeIfEmpty() {
    if (jobQueue.isEmpty()) {
      closed = true;
      return true;
    }

    return false;
  }

  private void removePreviousSubmissions(KeyExtent extent) {
    List<CjpqKey> prevJobs = tabletJobs.get(extent);
    if (prevJobs != null) {
      prevJobs.forEach(jobQueue::remove);
      tabletJobs.remove(extent);
    }
  }

  private CjpqKey addJobToQueue(TabletMetadata tabletMetadata, CompactionJob job) {
    if (jobQueue.size() >= maxSize) {
      var lastEntry = jobQueue.lastKey();
      if (job.getPriority() <= lastEntry.job.getPriority()) {
        // the queue is full and this job has a lower or same priority than the lowest job in the
        // queue, so do not add it
        rejectedJobs.getAndIncrement();
        return null;
      } else {
        // the new job has a higher priority than the lowest job in the queue, so remove the lowest
        jobQueue.pollLastEntry();
      }

    }

    var key = new CjpqKey(job);
    jobQueue.put(key, new CompactionJobQueues.MetaJob(job, tabletMetadata));
    return key;
  }
}

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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.stream.Collectors;

import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionJobQueues {

  private static final Logger log = LoggerFactory.getLogger(CompactionJobQueues.class);

  // The code in this class specifically depends on the behavior of ConcurrentHashMap, behavior that
  // other ConcurrentMap implementations may not have. The behavior it depended on is that the
  // compute functions for a key are atomic. This is documented on its javadoc and in the impl it
  // can be observed that scoped locks are acquired. Other concurrent map impls may run the compute
  // lambdas concurrently for a given key, which may still be correct but is more difficult to
  // analyze.
  private final ConcurrentHashMap<CompactorGroupId,CompactionJobPriorityQueue> priorityQueues =
      new ConcurrentHashMap<>();

  private final int queueSize;

  public CompactionJobQueues(int queueSize) {
    this.queueSize = queueSize;
  }

  public void add(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs) {
    if (jobs.size() == 1) {
      var executorId = jobs.iterator().next().getGroup();
      add(tabletMetadata, executorId, jobs);
    } else {
      jobs.stream().collect(Collectors.groupingBy(CompactionJob::getGroup))
          .forEach(((groupId, compactionJobs) -> add(tabletMetadata, groupId, compactionJobs)));
    }
  }

  public KeySetView<CompactorGroupId,CompactionJobPriorityQueue> getQueueIds() {
    return priorityQueues.keySet();
  }

  public CompactionJobPriorityQueue getQueue(CompactorGroupId groupId) {
    return priorityQueues.get(groupId);
  }

  public long getQueueMaxSize(CompactorGroupId groupId) {
    var prioQ = priorityQueues.get(groupId);
    return prioQ == null ? 0 : prioQ.getMaxSize();
  }

  public long getQueuedJobs(CompactorGroupId groupId) {
    var prioQ = priorityQueues.get(groupId);
    return prioQ == null ? 0 : prioQ.getQueuedJobs();
  }

  public long getDequeuedJobs(CompactorGroupId groupId) {
    var prioQ = priorityQueues.get(groupId);
    return prioQ == null ? 0 : prioQ.getDequeuedJobs();
  }

  public long getRejectedJobs(CompactorGroupId groupId) {
    var prioQ = priorityQueues.get(groupId);
    return prioQ == null ? 0 : prioQ.getRejectedJobs();
  }

  public long getLowestPriority(CompactorGroupId groupId) {
    var prioQ = priorityQueues.get(groupId);
    return prioQ == null ? 0 : prioQ.getLowestPriority();
  }

  public long getQueueCount() {
    return priorityQueues.mappingCount();
  }

  public long getQueuedJobCount() {
    long count = 0;
    for (CompactionJobPriorityQueue queue : priorityQueues.values()) {
      count += queue.getQueuedJobs();
    }
    return count;
  }

  public static class MetaJob {
    private final CompactionJob job;

    // the metadata from which the compaction job was derived
    private final TabletMetadata tabletMetadata;

    public MetaJob(CompactionJob job, TabletMetadata tabletMetadata) {
      this.job = job;
      this.tabletMetadata = tabletMetadata;
    }

    public CompactionJob getJob() {
      return job;
    }

    public TabletMetadata getTabletMetadata() {
      return tabletMetadata;
    }
  }

  public MetaJob poll(CompactorGroupId groupId) {
    var prioQ = priorityQueues.get(groupId);
    if (prioQ == null) {
      return null;
    }
    MetaJob mj = prioQ.poll();

    if (mj == null) {
      priorityQueues.computeIfPresent(groupId, (eid, pq) -> {
        if (pq.closeIfEmpty()) {
          return null;
        } else {
          return pq;
        }
      });
    }
    return mj;
  }

  private void add(TabletMetadata tabletMetadata, CompactorGroupId groupId,
      Collection<CompactionJob> jobs) {

    // TODO log level
    if (log.isDebugEnabled()) {
      log.debug("Adding jobs to queue {} {} {}", groupId, tabletMetadata.getExtent(),
          jobs.stream().map(job -> "#files:" + job.getFiles().size() + ",prio:" + job.getPriority()
              + ",kind:" + job.getKind()).collect(Collectors.toList()));
    }

    var pq = priorityQueues.computeIfAbsent(groupId,
        gid -> new CompactionJobPriorityQueue(gid, queueSize));
    while (!pq.add(tabletMetadata, jobs)) {
      // This loop handles race condition where poll() closes empty priority queues. The queue could
      // be closed after its obtained from the map and before add is called.
      pq = priorityQueues.computeIfAbsent(groupId,
          gid -> new CompactionJobPriorityQueue(gid, queueSize));
    }
  }
}

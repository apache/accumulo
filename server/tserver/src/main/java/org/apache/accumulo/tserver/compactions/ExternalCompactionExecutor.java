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
package org.apache.accumulo.tserver.compactions;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.tserver.compactions.SubmittedJob.Status;

import com.google.common.base.Preconditions;

public class ExternalCompactionExecutor implements CompactionExecutor {

  // This exist to provide an accurate count of queued compactions for metrics. The PriorityQueue is
  // not used because its size may be off due to it containing cancelled compactions. The collection
  // below should not contain cancelled compactions. A concurrent set was not used because those do
  // not have constant time size operations.
  private Set<ExternalJob> queuedTask = Collections.synchronizedSet(new HashSet<>());

  private class ExternalJob extends SubmittedJob {
    private final AtomicReference<Status> status = new AtomicReference<>(Status.QUEUED);
    private final Compactable compactable;
    private final CompactionServiceId csid;
    private volatile ExternalCompactionId ecid;
    private final AtomicLong cancelCount = new AtomicLong();
    private final long timeCreated;

    ExternalJob(CompactionJob job, Compactable compactable, CompactionServiceId csid) {
      super(job);
      this.compactable = compactable;
      this.csid = csid;
      queuedTask.add(this);
      this.timeCreated = System.currentTimeMillis();
    }

    @Override
    public Status getStatus() {
      var s = status.get();
      if (s == Status.RUNNING && ecid != null && !compactable.isActive(ecid)) {
        s = Status.COMPLETE;
      }

      return s;
    }

    @Override
    public boolean cancel(Status expectedStatus) {

      boolean canceled = false;

      if (expectedStatus == Status.QUEUED) {
        canceled = status.compareAndSet(expectedStatus, Status.CANCELED);
        if (canceled) {
          queuedTask.remove(this);
        }

        if (canceled && cancelCount.incrementAndGet() % 1024 == 0) {
          // Occasionally clean the queue of canceled tasks that have hung around because of their
          // low priority. This runs periodically, instead of every time something is canceled, to
          // avoid hurting performance.
          queue.removeIf(ej -> ej.getStatus() == Status.CANCELED);
        }
      }

      return canceled;
    }

    public KeyExtent getExtent() {
      return compactable.getExtent();
    }

    public long getTimeCreated() {
      return timeCreated;
    }
  }

  private PriorityBlockingQueue<ExternalJob> queue;
  private CompactionExecutorId ceid;

  public ExternalCompactionExecutor(CompactionExecutorId ceid) {
    this.ceid = ceid;
    Comparator<ExternalJob> priorityComparator =
        Comparator.comparingLong(ej -> ej.getJob().getPriority());
    priorityComparator =
        priorityComparator.reversed().thenComparingLong(ExternalJob::getTimeCreated);

    this.queue = new PriorityBlockingQueue<ExternalJob>(100,
        priorityComparator.thenComparing(priorityComparator));
  }

  @Override
  public SubmittedJob submit(CompactionServiceId csid, CompactionJob job, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    Preconditions.checkArgument(!compactable.getExtent().isMeta());
    ExternalJob extJob = new ExternalJob(job, compactable, csid);
    queue.add(extJob);
    return extJob;
  }

  @Override
  public int getCompactionsRunning(CType ctype) {
    if (ctype == CType.EXTERNAL)
      throw new UnsupportedOperationException();
    return 0;
  }

  @Override
  public int getCompactionsQueued(CType ctype) {
    if (ctype != CType.EXTERNAL)
      return 0;
    return queuedTask.size();
  }

  @Override
  public void stop() {}

  ExternalCompactionJob reserveExternalCompaction(long priority, String compactorId,
      ExternalCompactionId externalCompactionId) {

    ExternalCompactionJob found = null;

    while (found == null) {
      ExternalJob extJob = queue.poll();
      while (extJob != null && extJob.getStatus() != Status.QUEUED) {
        // Remove non-queued state from queue
        extJob = queue.poll();
      }

      if (extJob == null) {
        found = null;
        // nothing left in queue
        break;
      }

      if (extJob.getJob().getPriority() >= priority) {
        if (extJob.status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
          queuedTask.remove(extJob);
          var ecj = extJob.compactable.reserveExternalCompaction(extJob.csid, extJob.getJob(),
              compactorId, externalCompactionId);
          if (ecj == null) {
            found = null;
            break;
          } else {
            extJob.ecid = ecj.getExternalCompactionId();
            found = ecj;
          }
        } else {
          // Something else modified this job, try again
          continue;
        }
      } else {
        queue.add(extJob);
        found = null;
        break;
      }
    }
    return found;
  }

  public TCompactionQueueSummary summarize() {
    long priority = 0;
    ExternalJob topJob = queue.peek();
    while (topJob != null && topJob.getStatus() != Status.QUEUED) {
      queue.removeIf(extJob -> extJob.getStatus() != Status.QUEUED);
      topJob = queue.peek();
    }

    if (topJob != null) {
      priority = topJob.getJob().getPriority();
    }

    return new TCompactionQueueSummary(((CompactionExecutorIdImpl) ceid).getExternalName(),
        priority);
  }

  public CompactionExecutorId getId() {
    return ceid;
  }

  @Override
  public void compactableClosed(KeyExtent extent) {
    List<ExternalJob> taskToCancel;
    synchronized (queuedTask) {
      taskToCancel = queuedTask.stream().filter(ejob -> ejob.getExtent().equals(extent))
          .collect(Collectors.toList());
    }

    taskToCancel.forEach(task -> task.cancel(Status.QUEUED));
  }

}

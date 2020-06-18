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

import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;
import org.apache.accumulo.tserver.TabletServerResourceManager;
import org.apache.htrace.wrappers.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CompactionExecutor {

  private static final Logger log = LoggerFactory.getLogger(CompactionExecutor.class);

  private PriorityBlockingQueue<Runnable> queue;
  private ExecutorService executor;
  private final CompactionExecutorId ceid;
  private AtomicLong cancelCount = new AtomicLong();

  private class CompactionTask extends SubmittedJob implements Runnable {

    private AtomicReference<Status> status = new AtomicReference<>(Status.QUEUED);
    private Compactable compactable;
    private CompactionServiceId csid;
    private Consumer<Compactable> completionCallback;

    public CompactionTask(CompactionJob job, Compactable compactable, CompactionServiceId csid,
        Consumer<Compactable> completionCallback) {
      super(job);
      this.compactable = compactable;
      this.csid = csid;
      this.completionCallback = completionCallback;
    }

    @Override
    public void run() {

      try {
        if (status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
          compactable.compact(csid, getJob());
          completionCallback.accept(compactable);
        }
      } catch (Exception e) {
        log.warn("Compaction failed for {} on {}", compactable.getExtent(), getJob(), e);
        status.compareAndSet(Status.RUNNING, Status.FAILED);
      } finally {
        status.compareAndSet(Status.RUNNING, Status.COMPLETE);
      }
    }

    @Override
    public Status getStatus() {
      return status.get();
    }

    @Override
    public boolean cancel(Status expectedStatus) {

      boolean canceled = false;

      if (expectedStatus == Status.QUEUED) {
        canceled = status.compareAndSet(expectedStatus, Status.CANCELED);
      }

      if (canceled && cancelCount.incrementAndGet() % 1024 == 0) {
        // nMeed to occasionally clean the queue, it could have canceled task with low priority that
        // hang around. Avoid cleaning it every time something is canceled as that could be
        // expensive.
        queue.removeIf(runnable -> ((CompactionTask) runnable).getStatus() == Status.CANCELED);
      }

      return canceled;
    }

  }

  private static CompactionJob getJob(Runnable r) {
    if (r instanceof TraceRunnable) {
      return getJob(((TraceRunnable) r).getRunnable());
    }

    if (r instanceof CompactionTask) {
      return ((CompactionTask) r).getJob();
    }

    throw new IllegalArgumentException("Unknown runnable type " + r.getClass().getName());
  }

  CompactionExecutor(CompactionExecutorId ceid, int threads, TabletServerResourceManager tsrm) {
    this.ceid = ceid;
    var comparator =
        Comparator.comparing(CompactionExecutor::getJob, CompactionJobPrioritizer.JOB_COMPARATOR);

    queue = new PriorityBlockingQueue<Runnable>(100, comparator);

    executor = tsrm.createCompactionExecutor(ceid, threads, queue);
  }

  public SubmittedJob submit(CompactionServiceId csid, CompactionJob job, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    Preconditions.checkArgument(job.getExecutor().equals(ceid));
    var ctask = new CompactionTask(job, compactable, csid, completionCallback);
    executor.execute(ctask);
    return ctask;
  }
}

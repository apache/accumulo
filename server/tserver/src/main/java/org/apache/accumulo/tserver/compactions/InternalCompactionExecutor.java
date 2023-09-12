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
package org.apache.accumulo.tserver.compactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.tserver.compactions.SubmittedJob.Status;
import org.apache.accumulo.tserver.metrics.CompactionExecutorsMetrics;
import org.apache.accumulo.tserver.metrics.CompactionExecutorsMetrics.CeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Runs compactions within the tserver.
 */
public class InternalCompactionExecutor implements CompactionExecutor {

  private static final Logger log = LoggerFactory.getLogger(InternalCompactionExecutor.class);

  private PriorityBlockingQueue<Runnable> queue;
  private final CompactionExecutorId ceid;
  private AtomicLong cancelCount = new AtomicLong();
  private ThreadPoolExecutor threadPool;

  // This set provides an accurate count of queued compactions for metrics. The PriorityQueue is
  // not used because its size may be off due to it containing cancelled compactions. The collection
  // below should not contain cancelled compactions. A concurrent set was not used because those do
  // not have constant time size operations.
  private final Set<InternalJob> queuedJob = Collections.synchronizedSet(new HashSet<>());

  private final CeMetrics metricCloser;

  private final RateLimiter readLimiter;
  private final RateLimiter writeLimiter;

  // used to signal if running compactions for this service should keep running
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);

  private class InternalJob extends SubmittedJob implements Runnable {

    private final AtomicReference<Status> status = new AtomicReference<>(Status.QUEUED);
    private final Compactable compactable;
    private final CompactionServiceId csid;
    private final Consumer<Compactable> completionCallback;
    private final long queuedTime;

    public InternalJob(CompactionJob job, Compactable compactable, CompactionServiceId csid,
        Consumer<Compactable> completionCallback) {
      super(job);
      this.compactable = compactable;
      this.csid = csid;
      this.completionCallback = completionCallback;
      queuedJob.add(this);
      queuedTime = System.currentTimeMillis();
    }

    @Override
    public void run() {

      try {
        if (status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
          queuedJob.remove(this);
          compactable.compact(csid, getJob(), keepRunning::get, readLimiter, writeLimiter,
              queuedTime);
          completionCallback.accept(compactable);
        }
      } catch (RuntimeException e) {
        log.warn("Compaction failed for {} on {}", compactable.getExtent(), getJob(), e);
        status.compareAndSet(Status.RUNNING, Status.FAILED);
        completionCallback.accept(compactable);
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

      if (canceled) {
        queuedJob.remove(this);
      }

      if (canceled && cancelCount.incrementAndGet() % 1024 == 0) {
        // Occasionally clean the queue of canceled jobs that have hung around because of their low
        // priority. This runs periodically, instead of every time something is canceled, to avoid
        // hurting performance.
        queue.removeIf(runnable -> {
          runnable = TraceUtil.unwrap(runnable);
          InternalJob internalJob;
          if (runnable instanceof InternalJob) {
            internalJob = (InternalJob) runnable;
          } else {
            throw new IllegalArgumentException(
                "Unknown runnable type " + runnable.getClass().getName());
          }
          return internalJob.getStatus() == Status.CANCELED;
        });
      }

      return canceled;
    }

    public KeyExtent getExtent() {
      return compactable.getExtent();
    }
  }

  private static CompactionJob getJob(Runnable r) {
    r = TraceUtil.unwrap(r);
    if (r instanceof InternalJob) {
      return ((InternalJob) r).getJob();
    }
    throw new IllegalArgumentException("Unknown runnable type " + r.getClass().getName());
  }

  InternalCompactionExecutor(CompactionExecutorId ceid, int threads,
      CompactionExecutorsMetrics ceMetrics, RateLimiter readLimiter, RateLimiter writeLimiter) {
    this.ceid = ceid;
    var comparator = Comparator.comparing(InternalCompactionExecutor::getJob,
        CompactionJobPrioritizer.JOB_COMPARATOR);

    queue = new PriorityBlockingQueue<>(100, comparator);

    threadPool = ThreadPools.getServerThreadPools().createThreadPool(threads, threads, 60,
        TimeUnit.SECONDS, "compaction." + ceid, queue, false);

    metricCloser =
        ceMetrics.addExecutor(ceid, () -> threadPool.getActiveCount(), () -> queuedJob.size());

    this.readLimiter = readLimiter;
    this.writeLimiter = writeLimiter;

    log.debug("Created compaction executor {} with {} threads", ceid, threads);
  }

  @Override
  public SubmittedJob submit(CompactionServiceId csid, CompactionJob job, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    Preconditions.checkArgument(job.getExecutor().equals(ceid));
    var internalJob = new InternalJob(job, compactable, csid, completionCallback);
    try {
      threadPool.execute(internalJob);
    } catch (RejectedExecutionException e) {
      if (threadPool.isShutdown()) {
        log.trace("Ignoring rejected execution exception because thread pool is shutdown", e);
        internalJob.cancel(Status.QUEUED);
      } else {
        throw new IllegalStateException(e);
      }
    }
    return internalJob;
  }

  public void setThreads(int numThreads) {
    ThreadPools.resizePool(threadPool, () -> numThreads, "compaction." + ceid);
  }

  @Override
  public int getCompactionsRunning(CType ctype) {
    if (ctype != CType.INTERNAL) {
      return 0;
    }
    return threadPool.getActiveCount();
  }

  @Override
  public int getCompactionsQueued(CType ctype) {
    if (ctype != CType.INTERNAL) {
      return 0;
    }
    return queuedJob.size();
  }

  @Override
  public void stop() {
    // Let compactions that are running keep running, but stop accepting new compactions.
    threadPool.shutdown();

    int running = threadPool.getActiveCount();

    List<InternalJob> jobToCancel;
    synchronized (queuedJob) {
      jobToCancel = new ArrayList<>(queuedJob);
    }

    // Cancel any compactions queued for the thread pool that have not yet started running.
    int canceled = 0;
    for (var job : jobToCancel) {
      if (job.cancel(Status.QUEUED)) {
        canceled++;
      }
    }

    keepRunning.set(false);

    log.debug("Stopped compaction executor {} running:{} canceled:{}", ceid, running, canceled);
    metricCloser.close();
  }

  @Override
  public void compactableClosed(KeyExtent extent) {
    List<InternalJob> jobToCancel;
    synchronized (queuedJob) {
      jobToCancel = queuedJob.stream().filter(job -> job.getExtent().equals(extent))
          .collect(Collectors.toList());
    }

    jobToCancel.forEach(job -> job.cancel(Status.QUEUED));
  }
}

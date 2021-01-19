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
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.tserver.metrics.CompactionExecutorsMetrics;
import org.apache.htrace.wrappers.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CompactionExecutor {

  private static final Logger log = LoggerFactory.getLogger(CompactionExecutor.class);

  private PriorityBlockingQueue<Runnable> queue;
  private final CompactionExecutorId ceid;
  private AtomicLong cancelCount = new AtomicLong();
  private ThreadPoolExecutor threadPool;

  // This exist to provide an accurate count of queued compactions for metrics. The PriorityQueue is
  // not used because its size may be off due to it containing cancelled compactions. The collection
  // below should not contain cancelled compactions. A concurrent set was not used because those do
  // not have constant time size operations.
  private Set<CompactionTask> queuedTask = Collections.synchronizedSet(new HashSet<>());

  private AutoCloseable metricCloser;

  private RateLimiter readLimiter;
  private RateLimiter writeLimiter;

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
      queuedTask.add(this);
    }

    @Override
    public void run() {

      try {
        if (status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
          queuedTask.remove(this);
          compactable.compact(csid, getJob(), readLimiter, writeLimiter);
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

      if (canceled)
        queuedTask.remove(this);

      if (canceled && cancelCount.incrementAndGet() % 1024 == 0) {
        // Occasionally clean the queue of canceled tasks that have hung around because of their low
        // priority. This runs periodically, instead of every time something is canceled, to avoid
        // hurting performance.
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

  CompactionExecutor(CompactionExecutorId ceid, int threads, CompactionExecutorsMetrics ceMetrics,
      RateLimiter readLimiter, RateLimiter writeLimiter) {
    this.ceid = ceid;
    var comparator =
        Comparator.comparing(CompactionExecutor::getJob, CompactionJobPrioritizer.JOB_COMPARATOR);

    queue = new PriorityBlockingQueue<Runnable>(100, comparator);

    threadPool = ThreadPools.createThreadPool(threads, threads, 60, TimeUnit.SECONDS,
        "compaction." + ceid, queue, OptionalInt.empty(), true);

    metricCloser =
        ceMetrics.addExecutor(ceid, () -> threadPool.getActiveCount(), () -> queuedTask.size());

    this.readLimiter = readLimiter;
    this.writeLimiter = writeLimiter;

    log.debug("Created compaction executor {} with {} threads", ceid, threads);
  }

  public SubmittedJob submit(CompactionServiceId csid, CompactionJob job, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    Preconditions.checkArgument(job.getExecutor().equals(ceid));
    var ctask = new CompactionTask(job, compactable, csid, completionCallback);
    threadPool.execute(ctask);
    return ctask;
  }

  public void setThreads(int numThreads) {

    int coreSize = threadPool.getCorePoolSize();

    if (numThreads < coreSize) {
      threadPool.setCorePoolSize(numThreads);
      threadPool.setMaximumPoolSize(numThreads);
    } else if (numThreads > coreSize) {
      threadPool.setMaximumPoolSize(numThreads);
      threadPool.setCorePoolSize(numThreads);
    }

    if (numThreads != coreSize) {
      log.debug("Adjusted compaction executor {} threads from {} to {}", ceid, coreSize,
          numThreads);
    }
  }

  public int getCompactionsRunning() {
    return threadPool.getActiveCount();
  }

  public int getCompactionsQueued() {
    return queuedTask.size();
  }

  public void stop() {
    threadPool.shutdownNow();
    log.debug("Stopped compaction executor {}", ceid);
    try {
      metricCloser.close();
    } catch (Exception e) {
      log.warn("Failed to close metrics {}", ceid, e);
    }
  }
}

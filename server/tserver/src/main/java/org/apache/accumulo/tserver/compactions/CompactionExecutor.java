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

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CompactionExecutor {

  private static final Logger log = LoggerFactory.getLogger(CompactionExecutor.class);

  private PriorityBlockingQueue<Runnable> queue;
  private ThreadPoolExecutor executor;
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

    private String getSize(Collection<CompactableFile> files) {
      long sum = files.stream().mapToLong(CompactableFile::getEstimatedSize).sum();
      return FileUtils.byteCountToDisplaySize(sum);
    }

    @Override
    public void run() {

      try {
        if (status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
          log.info("Running compaction for {} on {}", compactable.getExtent(), ceid);
          compactable.compact(csid, getJob());
          completionCallback.accept(compactable);
          // TODO debug?
          log.info("Finished compaction for {} on {} files {} size {} {}", compactable.getExtent(),
              ceid, getJob().getFiles().size(), getSize(getJob().getFiles()), getJob().getKind());
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
      } else {
        // TODO
      }

      if (canceled && cancelCount.incrementAndGet() % 1024 == 0) {
        // need to occasionally clean the queue, it could have canceled task with low priority that
        // hang around
        queue.removeIf(runnable -> ((CompactionTask) runnable).getStatus() == Status.CANCELED);
      }

      return canceled;
    }

  }

  private static long extractPriority(Runnable r) {
    return ((CompactionTask) r).getJob().getPriority();
  }

  private static long extractJobFiles(Runnable r) {
    return ((CompactionTask) r).getJob().getFiles().size();
  }

  CompactionExecutor(CompactionExecutorId ceid, int threads) {
    this.ceid = ceid;
    var comparator = Comparator.comparingLong(CompactionExecutor::extractPriority)
        .thenComparingLong(CompactionExecutor::extractJobFiles).reversed();

    queue = new PriorityBlockingQueue<Runnable>(100, comparator);

    // TODO use code in TSRM to create pools.. and name threads
    executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, queue);

  }

  public SubmittedJob submit(CompactionServiceId csid, CompactionJob job, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    Preconditions.checkArgument(job.getExecutor().equals(ceid));
    var ctask = new CompactionTask(job, compactable, csid, completionCallback);
    executor.execute(ctask);
    return ctask;
  }
}

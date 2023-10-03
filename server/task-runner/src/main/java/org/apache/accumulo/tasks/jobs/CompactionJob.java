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
package org.apache.accumulo.tasks.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.UnknownCompactionIdException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tasks.TaskMessageType;
import org.apache.accumulo.core.tasks.compaction.CompactionTask;
import org.apache.accumulo.core.tasks.compaction.CompactionTaskCompleted;
import org.apache.accumulo.core.tasks.compaction.CompactionTaskFailed;
import org.apache.accumulo.core.tasks.compaction.CompactionTaskStatus;
import org.apache.accumulo.core.tasks.thrift.TaskManager.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.apache.accumulo.server.compaction.CompactionInfo;
import org.apache.accumulo.server.compaction.FileCompactor;
import org.apache.accumulo.server.compaction.RetryableThriftCall;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.tasks.TaskRunnerProcess;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CompactionJob extends Job<CompactionTask> {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionJob.class);
  private static final long TEN_MEGABYTES = 10485760;

  protected static final CompactionJobHolder JOB_HOLDER = new CompactionJobHolder();

  public static void checkIfCanceled(ServerContext ctx) {
    TExternalCompactionJob job = JOB_HOLDER.getJob();
    if (job != null) {
      try {
        var extent = KeyExtent.fromThrift(job.getExtent());
        var ecid = ExternalCompactionId.of(job.getExternalCompactionId());

        TabletMetadata tabletMeta =
            ctx.getAmple().readTablet(extent, ColumnType.ECOMP, ColumnType.PREV_ROW);
        if (tabletMeta == null || !tabletMeta.getExternalCompactions().containsKey(ecid)) {
          // table was deleted OR tablet was split or merged OR tablet no longer thinks compaction
          // is running for some reason
          LOG.info("Cancelling compaction {} that no longer has a metadata entry at {}", ecid,
              extent);
          JOB_HOLDER.cancel(job.getExternalCompactionId());
          return;
        }

        if (job.getKind() == TCompactionKind.USER) {

          var cconf = CompactionConfigStorage.getConfig(ctx, job.getFateTxId());

          if (cconf == null) {
            LOG.info("Cancelling compaction {} for user compaction that no longer exists {} {}",
                ecid, FateTxId.formatTid(job.getFateTxId()), extent);
            JOB_HOLDER.cancel(job.getExternalCompactionId());
          }
        }
      } catch (RuntimeException | KeeperException e) {
        LOG.warn("Failed to check if compaction {} for {} was canceled.",
            job.getExternalCompactionId(), KeyExtent.fromThrift(job.getExtent()), e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Returns the number of seconds to wait in between progress checks based on input file sizes
   *
   * @param numBytes number of bytes in input file
   * @return number of seconds to wait between progress checks
   */
  public static long calculateProgressCheckTime(long numBytes) {
    return Math.max(1, (numBytes / TEN_MEGABYTES));
  }

  protected final LongAdder totalInputEntries = new LongAdder();
  protected final LongAdder totalInputBytes = new LongAdder();
  protected final CountDownLatch started = new CountDownLatch(1);
  protected final CountDownLatch stopped = new CountDownLatch(1);
  protected final AtomicReference<Throwable> errorRef = new AtomicReference<>();
  protected final AtomicBoolean compactionRunning = new AtomicBoolean(false);

  private final TExternalCompactionJob details;
  private final AtomicReference<ExternalCompactionId> currentCompactionId;

  public CompactionJob(TaskRunnerProcess worker, CompactionTask msg,
      AtomicReference<ExternalCompactionId> currentCompactionId) throws TException {
    super(worker, msg);
    this.details = msg.getCompactionJob();
    this.currentCompactionId = currentCompactionId;
  }

  public TExternalCompactionJob getJobDetails() {
    return this.details;
  }

  @Override
  public Runnable createJob() throws TException {
    errorRef.set(null);
    return createCompactionJob(msg.getCompactionJob(), totalInputEntries, totalInputBytes, started,
        stopped, errorRef);
  }

  @Override
  public void executeJob(Thread executionThread) throws InterruptedException {
    try {
      JOB_HOLDER.set(this.details, executionThread);
      executionThread.start(); // start the compactionThread
      started.await(); // wait until the compactor is started
      final long inputEntries = totalInputEntries.sum();
      final long waitTime = calculateProgressCheckTime(totalInputBytes.sum());
      LOG.debug("Progress checks will occur every {} seconds", waitTime);
      String percentComplete = "unknown";

      while (!stopped.await(waitTime, TimeUnit.SECONDS)) {
        List<CompactionInfo> running =
            org.apache.accumulo.server.compaction.FileCompactor.getRunningCompactions();
        if (!running.isEmpty()) {
          // Compaction has started. There should only be one in the list
          CompactionInfo info = running.get(0);
          if (info != null) {
            if (inputEntries > 0) {
              percentComplete =
                  Float.toString((info.getEntriesRead() / (float) inputEntries) * 100);
            }
            String message = String.format(
                "Compaction in progress, read %d of %d input entries ( %s %s ), written %d entries, paused %d times",
                info.getEntriesRead(), inputEntries, percentComplete, "%", info.getEntriesWritten(),
                info.getTimesPaused());
            getTaskWorker().getCompactionWatcher().run();
            try {
              LOG.debug("Updating TaskManager with compaction progress: {}.", message);
              TCompactionStatusUpdate update =
                  new TCompactionStatusUpdate(TCompactionState.IN_PROGRESS, message, inputEntries,
                      info.getEntriesRead(), info.getEntriesWritten());
              updateCompactionState(details, update);
            } catch (RetriesExceededException e) {
              LOG.warn("Error updating TaskManager with compaction progress, error: {}",
                  e.getMessage());
            }
          }
        } else {
          LOG.error("Waiting on compaction thread to finish, but no RUNNING compaction");
        }
      }
      executionThread.join();
      LOG.trace("Compaction thread finished.");
      // Run the watcher again to clear out the finished compaction and set the
      // stuck count to zero.
      getTaskWorker().getCompactionWatcher().run();

      if (errorRef.get() != null) {
        // maybe the error occured because the table was deleted or something like that, so
        // force a cancel check to possibly reduce noise in the logs
        checkIfCanceled(getTaskWorker().getContext());
      }

      if (executionThread.isInterrupted() || JOB_HOLDER.isCancelled() || (errorRef.get() != null
          && errorRef.get().getClass().equals(InterruptedException.class))) {
        LOG.warn("Compaction thread was interrupted, sending CANCELLED state");
        try {
          TCompactionStatusUpdate update = new TCompactionStatusUpdate(TCompactionState.CANCELLED,
              "Compaction cancelled", -1, -1, -1);
          updateCompactionState(details, update);
          updateCompactionFailed(details);
        } catch (RetriesExceededException e) {
          LOG.error("Error updating TaskManager with compaction cancellation.", e);
        } finally {
          currentCompactionId.set(null);
        }
      } else if (errorRef.get() != null) {
        try {
          LOG.info("Updating TaskManager with compaction failure.");
          TCompactionStatusUpdate update = new TCompactionStatusUpdate(TCompactionState.FAILED,
              "Compaction failed due to: " + errorRef.get().getMessage(), -1, -1, -1);
          updateCompactionState(details, update);
          updateCompactionFailed(details);
        } catch (RetriesExceededException e) {
          LOG.error("Error updating TaskManager with compaction failure.", e);
        } finally {
          currentCompactionId.set(null);
        }
      } else {
        try {
          LOG.trace("Updating TaskManager with compaction completion.");
          updateCompactionCompleted(details, JOB_HOLDER.getStats());
        } catch (RetriesExceededException e) {
          LOG.error("Error updating TaskManager with compaction completion, cancelling compaction.",
              e);
          try {
            cancel(details.getExternalCompactionId());
          } catch (TException e1) {
            LOG.error("Error cancelling compaction.", e1);
          }
        } finally {
          currentCompactionId.set(null);
        }
      }
    } catch (RuntimeException e1) {
      LOG.error("Compactor thread was interrupted waiting for compaction to start, cancelling job",
          e1);
      try {
        cancel(details.getExternalCompactionId());
      } catch (TException e2) {
        LOG.error("Error cancelling compaction.", e2);
      }
    } finally {
      currentCompactionId.set(null);
      // In the case where there is an error in the foreground code the background compaction
      // may still be running. Must cancel it before starting another iteration of the loop to
      // avoid multiple threads updating shared state.
      while (executionThread.isAlive()) {
        executionThread.interrupt();
        executionThread.join(1000);
      }
    }

  }

  /**
   * Cancel the compaction with this id.
   *
   * @param externalCompactionId compaction id
   * @throws UnknownCompactionIdException if the externalCompactionId does not match the currently
   *         executing compaction
   * @throws TException thrift error
   */
  @Override
  public void cancel(String externalCompactionId) throws TException {
    if (JOB_HOLDER.cancel(externalCompactionId)) {
      LOG.info("Cancel requested for compaction job {}", externalCompactionId);
    } else {
      throw new UnknownCompactionIdException();
    }
  }

  /**
   * Create compaction runnable
   *
   * @param job compaction job
   * @param totalInputEntries object to capture total entries
   * @param totalInputBytes object to capture input file size
   * @param started started latch
   * @param stopped stopped latch
   * @param err reference to error
   * @return Runnable compaction job
   */
  private Runnable createCompactionJob(final TExternalCompactionJob job,
      final LongAdder totalInputEntries, final LongAdder totalInputBytes,
      final CountDownLatch started, final CountDownLatch stopped,
      final AtomicReference<Throwable> err) {

    return () -> {
      // Its only expected that a single compaction runs at a time. Multiple compactions running
      // at a time could cause odd behavior like out of order and unexpected thrift calls to the
      // TaskManager. This is a sanity check to ensure the expectation is met. Should this check
      // ever fail, it means there is a bug elsewhere.
      Preconditions.checkState(compactionRunning.compareAndSet(false, true));
      try {
        LOG.info("Starting up compaction runnable for job: {}", job);
        TCompactionStatusUpdate update =
            new TCompactionStatusUpdate(TCompactionState.STARTED, "Compaction started", -1, -1, -1);
        updateCompactionState(job, update);
        var extent = KeyExtent.fromThrift(job.getExtent());
        final AccumuloConfiguration aConfig;
        final TableConfiguration tConfig =
            getTaskWorker().getContext().getTableConfiguration(extent.tableId());

        if (!job.getOverrides().isEmpty()) {
          aConfig = new ConfigurationCopy(tConfig);
          job.getOverrides().forEach(((ConfigurationCopy) aConfig)::set);
          LOG.debug("Overriding table properties with {}", job.getOverrides());
        } else {
          aConfig = tConfig;
        }

        final ReferencedTabletFile outputFile =
            new ReferencedTabletFile(new Path(job.getOutputFile()));

        final Map<StoredTabletFile,DataFileValue> files = new TreeMap<>();
        job.getFiles().forEach(f -> {
          files.put(new StoredTabletFile(f.getMetadataFileEntry()),
              new DataFileValue(f.getSize(), f.getEntries(), f.getTimestamp()));
          totalInputEntries.add(f.getEntries());
          totalInputBytes.add(f.getSize());
        });

        final List<IteratorSetting> iters = new ArrayList<>();
        job.getIteratorSettings().getIterators()
            .forEach(tis -> iters.add(SystemIteratorUtil.toIteratorSetting(tis)));

        ExtCEnv cenv = new ExtCEnv(JOB_HOLDER, getTaskWorker().getResourceGroup());
        FileCompactor compactor = new FileCompactor(getTaskWorker().getContext(), extent, files,
            outputFile, job.isPropagateDeletes(), cenv, iters, aConfig, tConfig.getCryptoService(),
            getTaskWorker().getPausedCompactionMetrics());

        LOG.trace("Starting compactor");
        started.countDown();

        org.apache.accumulo.server.compaction.CompactionStats stat = compactor.call();
        TCompactionStats cs = new TCompactionStats();
        cs.setEntriesRead(stat.getEntriesRead());
        cs.setEntriesWritten(stat.getEntriesWritten());
        cs.setFileSize(stat.getFileSize());
        JOB_HOLDER.setStats(cs);

        LOG.info("Compaction completed successfully {} ", job.getExternalCompactionId());
        // Update state when completed
        TCompactionStatusUpdate update2 = new TCompactionStatusUpdate(TCompactionState.SUCCEEDED,
            "Compaction completed successfully", -1, -1, -1);
        updateCompactionState(job, update2);
      } catch (FileCompactor.CompactionCanceledException cce) {
        LOG.debug("Compaction canceled {}", job.getExternalCompactionId());
      } catch (Exception e) {
        LOG.error("Compaction failed", e);
        err.set(e);
      } finally {
        stopped.countDown();
        Preconditions.checkState(compactionRunning.compareAndSet(true, false));
      }
    };
  }

  /**
   * Send an update to the TaskManager for this job
   *
   * @param job compactionJob
   * @param update status update
   * @throws RetriesExceededException thrown when retries have been exceeded
   */
  protected void updateCompactionState(TExternalCompactionJob job, TCompactionStatusUpdate update)
      throws RetriesExceededException {
    RetryableThriftCall<String> thriftCall =
        new RetryableThriftCall<>(1000, RetryableThriftCall.MAX_WAIT_TIME, 25, () -> {
          Client taskManagerClient = getTaskWorker().getTaskManagerClient();
          try {
            CompactionTaskStatus status = TaskMessageType.COMPACTION_TASK_STATUS.getTaskMessage();
            status.setTaskId(job.getExternalCompactionId());
            status.setCompactionStatus(update);
            taskManagerClient.taskStatus(TraceUtil.traceInfo(),
                getTaskWorker().getContext().rpcCreds(), System.currentTimeMillis(),
                status.toThriftTask());
            return "";
          } finally {
            ThriftUtil.returnClient(taskManagerClient, getTaskWorker().getContext());
          }
        });
    thriftCall.run();
  }

  /**
   * Notify the TaskManager the job failed
   *
   * @param job current compaction job
   * @throws RetriesExceededException thrown when retries have been exceeded
   */
  protected void updateCompactionFailed(TExternalCompactionJob job)
      throws RetriesExceededException {
    RetryableThriftCall<String> thriftCall =
        new RetryableThriftCall<>(1000, RetryableThriftCall.MAX_WAIT_TIME, 25, () -> {
          Client taskManagerClient = getTaskWorker().getTaskManagerClient();
          try {
            CompactionTaskFailed failedMsg =
                TaskMessageType.COMPACTION_TASK_FAILED.getTaskMessage();
            failedMsg.setTaskId(job.getExternalCompactionId());
            failedMsg.setCompactionJob(job);
            taskManagerClient.taskFailed(TraceUtil.traceInfo(),
                getTaskWorker().getContext().rpcCreds(), failedMsg.toThriftTask());
            return "";
          } finally {
            ThriftUtil.returnClient(taskManagerClient, getTaskWorker().getContext());
          }
        });
    thriftCall.run();
  }

  /**
   * Update the TaskManager with the stats from the completed job
   *
   * @param job current compaction job
   * @param stats compaction stats
   * @throws RetriesExceededException thrown when retries have been exceeded
   */
  protected void updateCompactionCompleted(TExternalCompactionJob job, TCompactionStats stats)
      throws RetriesExceededException {
    RetryableThriftCall<String> thriftCall =
        new RetryableThriftCall<>(1000, RetryableThriftCall.MAX_WAIT_TIME, 25, () -> {
          Client taskManagerClient = getTaskWorker().getTaskManagerClient();
          try {
            CompactionTaskCompleted completedMsg =
                TaskMessageType.COMPACTION_TASK_COMPLETED.getTaskMessage();
            completedMsg.setTaskId(job.getExternalCompactionId());
            completedMsg.setCompactionJob(job);
            completedMsg.setCompactionStats(stats);
            taskManagerClient.taskCompleted(TraceUtil.traceInfo(),
                getTaskWorker().getContext().rpcCreds(), completedMsg.toThriftTask());
            return "";
          } finally {
            ThriftUtil.returnClient(taskManagerClient, getTaskWorker().getContext());
          }
        });
    thriftCall.run();
  }

}

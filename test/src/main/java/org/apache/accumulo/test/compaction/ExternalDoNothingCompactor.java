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
package org.apache.accumulo.test.compaction;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.tasks.compaction.CompactionTask;
import org.apache.accumulo.core.tasks.thrift.TaskRunner.Iface;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionCanceledException;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.tasks.TaskRunner;
import org.apache.accumulo.tasks.TaskRunnerProcess;
import org.apache.accumulo.tasks.jobs.CompactionJob;
import org.apache.accumulo.tasks.jobs.Job;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalDoNothingCompactor extends TaskRunner implements Iface {

  public static class DoNothingCompactionJob extends CompactionJob {

    public DoNothingCompactionJob(TaskRunnerProcess worker, CompactionTask msg,
        AtomicReference<ExternalCompactionId> currentCompactionId) throws TException {
      super(worker, msg, currentCompactionId);
    }

    @Override
    public Runnable createJob() throws TException {
      // Set this to true so that only 1 external compaction is run
      getTaskWorker().shutdown();

      return () -> {
        try {
          LOG.info("Starting up compaction runnable for job: {}", this.getJobDetails());
          TCompactionStatusUpdate update = new TCompactionStatusUpdate();
          update.setState(TCompactionState.STARTED);
          update.setMessage("Compaction started");
          updateCompactionState(this.getJobDetails(), update);

          LOG.info("Starting compactor");
          started.countDown();

          while (!JOB_HOLDER.isCancelled()) {
            LOG.info("Sleeping while job is not cancelled");
            UtilWaitThread.sleep(1000);
          }
          // Compactor throws this exception when cancelled
          throw new CompactionCanceledException();

        } catch (Exception e) {
          LOG.error("Compaction failed", e);
          errorRef.set(e);
        } finally {
          stopped.countDown();
        }
      };
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(ExternalDoNothingCompactor.class);

  ExternalDoNothingCompactor(ConfigOpts opts, String[] args) {
    super(opts, args);
  }

  @Override
  protected long getTimeBetweenCancelChecks() {
    return SECONDS.toMillis(5);
  }

  @Override
  protected Job<?> getNextJob(Supplier<UUID> uuid) throws RetriesExceededException {

    // Get the next job, use the parent class for this
    CompactionJob cjob = null;
    do {
      cjob = (CompactionJob) super.getNextJob(uuid);
      UtilWaitThread.sleep(1000);
    } while (!cjob.getJobDetails().isSetExternalCompactionId());

    // Take the job details and return a DoNothingCompactionJob
    try {
      CompactionTask task = new CompactionTask();
      task.setCompactionJob(cjob.getJobDetails());
      return new DoNothingCompactionJob(this, task, new AtomicReference<ExternalCompactionId>(
          ExternalCompactionId.from(cjob.getJobDetails().getExternalCompactionId())));
    } catch (TException e) {
      throw new RuntimeException("Error creating job", e);
    }
  }

  public static void main(String[] args) throws Exception {
    try (var compactor = new ExternalDoNothingCompactor(new ConfigOpts(), args)) {
      compactor.runServer();
    }
  }

}

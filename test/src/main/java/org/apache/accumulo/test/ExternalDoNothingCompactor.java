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
package org.apache.accumulo.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.core.compaction.thrift.CompactorService.Iface;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionCanceledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalDoNothingCompactor extends Compactor implements Iface {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalDoNothingCompactor.class);

  ExternalDoNothingCompactor(CompactorServerOpts opts, String[] args) {
    super(opts, args);
  }

  @Override
  protected void startCancelChecker(ScheduledThreadPoolExecutor schedExecutor,
      long timeBetweenChecks) {
    schedExecutor.scheduleWithFixedDelay(() -> checkIfCanceled(), 0, 5000, TimeUnit.MILLISECONDS);
  }

  @Override
  protected Runnable createCompactionJob(TExternalCompactionJob job, LongAdder totalInputEntries,
      LongAdder totalInputBytes, CountDownLatch started, CountDownLatch stopped,
      AtomicReference<Throwable> err) {

    return new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting up compaction runnable for job: {}", job);
          updateCompactionState(job, TCompactionState.STARTED, "Compaction started");

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
          err.set(e);
          throw new RuntimeException("Compaction failed", e);
        } finally {
          stopped.countDown();
        }
      }
    };

  }

  public static void main(String[] args) throws Exception {
    try (ExternalDoNothingCompactor compactor =
        new ExternalDoNothingCompactor(new CompactorServerOpts(), args)) {
      compactor.runServer();
    }
  }

}

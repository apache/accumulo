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
package org.apache.accumulo.server.conf.store.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.util.threads.ThreadPools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadyMonitorTest {

  private static final Logger log = LoggerFactory.getLogger(ReadyMonitorTest.class);

  // TODO - evaluate stress test private final int numWorkerThreads = 64;
  private final int numWorkerThreads = 4;
  private CountDownLatch readyToRunLatch = null;
  private CountDownLatch completedLatch = null;

  private ThreadPoolExecutor pool = null;

  @BeforeEach
  public void init() {

    readyToRunLatch = new CountDownLatch(numWorkerThreads);
    completedLatch = new CountDownLatch(numWorkerThreads);

    // these tests wait for all workers to signal ready using count down latch.
    // make thread pool larger to all to run and the wait for count down to reach 0 to proceed.
    int numPoolThreads = numWorkerThreads + 2;
    pool = ThreadPools.getServerThreadPools().createFixedThreadPool(numPoolThreads,
        "readyMonitor-test-pool", false);
  }

  @AfterEach
  public void teardown() {
    pool.shutdownNow();
    try {
      pool.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      // don't care.
      pool.shutdownNow();
    }
  }

  @Test
  public void isReadyST() {
    ReadyMonitor readyMonitor = new ReadyMonitor("test", 100);
    assertFalse(readyMonitor.test());

    readyMonitor.setReady();
    assertTrue(readyMonitor.test());

    readyMonitor.isReady();
  }

  @Test
  public void clearTest() {

    ReadyMonitor readyMonitor = new ReadyMonitor("test", 100);
    assertFalse(readyMonitor.test());

    readyMonitor.setReady();
    assertTrue(readyMonitor.test());

    readyMonitor.clearReady();
    assertFalse(readyMonitor.test());

    assertThrows(IllegalStateException.class, readyMonitor::isReady);
  }

  @Test
  public void notReady() {
    ReadyMonitor readyMonitor = new ReadyMonitor("test", 100);
    assertFalse(readyMonitor.test());

    assertThrows(IllegalStateException.class, readyMonitor::isReady);
  }

  @Test
  public void isReadyMT() throws Exception {
    ReadyMonitor readyMonitor = new ReadyMonitor("test", 100);
    assertFalse(readyMonitor.test());

    log.info("start latch - {}", readyToRunLatch.getCount());

    var readyTimeout = 10_000;

    List<Future<Long>> tasks = new ArrayList<>();
    for (int i = 0; i < numWorkerThreads; i++) {
      ReadyTask r = new ReadyTask(readyMonitor, readyTimeout, readyToRunLatch, completedLatch);
      tasks.add(pool.submit(r));
    }

    var allPresent = readyToRunLatch.await(10_000, TimeUnit.MILLISECONDS);
    assertTrue(allPresent, "failed - all worker tasks did not report ready");

    readyMonitor.setReady();

    var allComplete = completedLatch.await(20_000, TimeUnit.MILLISECONDS);
    assertTrue(allComplete,
        "failed - all expected tasks did not complete - count=" + completedLatch.getCount());

    tasks.forEach(f -> {
      try {
        var timeWaiting = f.get();
        log.debug("Received: {}", TimeUnit.NANOSECONDS.toMillis(timeWaiting));
        assertTrue(timeWaiting < TimeUnit.MILLISECONDS.toNanos(readyTimeout));
      } catch (ExecutionException | InterruptedException ex) {
        log.warn("Task failed", ex);
        fail("Task failed with exception - " + ex.getMessage());
      }
    });
  }

  private static class ReadyTask implements Callable<Long> {

    private final ReadyMonitor readyMonitor;
    private final CountDownLatch readyToRunLatch;
    private final CountDownLatch finishedLatch;

    public ReadyTask(final ReadyMonitor readyMonitor, final long readyTimeout,
        final CountDownLatch readyToRunLatch, final CountDownLatch finishedLatch) {
      this.readyMonitor = readyMonitor;
      this.readyToRunLatch = readyToRunLatch;
      this.finishedLatch = finishedLatch;
    }

    @Override
    public Long call() throws Exception {
      // signal ready to run
      readyToRunLatch.countDown();
      // time waiting for isReady to complete++
      long start = System.nanoTime();
      readyMonitor.isReady();
      finishedLatch.countDown();

      // returning nanoseconds.
      return System.nanoTime() - start;
    }
  }
}

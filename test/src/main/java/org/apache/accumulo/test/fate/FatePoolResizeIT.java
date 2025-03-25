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
package org.apache.accumulo.test.fate;

import static org.apache.accumulo.test.fate.FateStoreUtil.TEST_FATE_OP;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;

public abstract class FatePoolResizeIT extends SharedMiniClusterBase
    implements FateTestRunner<FatePoolResizeIT.PoolResizeTestEnv> {

  @Test
  public void testIncreaseSize() throws Exception {
    executeTest(this::testIncreaseSize);
  }

  protected void testIncreaseSize(FateStore<PoolResizeTestEnv> store, ServerContext sctx)
      throws Exception {
    final int numThreads = 10;
    final int newNumThreads = 20;
    final ConfigurationCopy config = initConfig(numThreads);
    final var env = new PoolResizeTestEnv();
    final Fate<PoolResizeTestEnv> fate = new FastFate<>(env, store, false, r -> r + "", config);
    try {
      // create a tx for all future threads. For now, only some of the txns will be workable
      for (int i = 0; i < newNumThreads; i++) {
        var fateId = fate.startTransaction();
        fate.seedTransaction(TEST_FATE_OP, fateId, new PoolResizeTestRepo(), true, "testing");
      }
      // wait for all available threads to be working on a tx
      Wait.waitFor(() -> env.numWorkers.get() == numThreads);
      assertEquals(numThreads, fate.getTxRunnersActive());
      config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, String.valueOf(newNumThreads));
      // wait for new config to be detected, new runners to be created, and for these runners to
      // pick up the rest of the available txns
      Wait.waitFor(() -> env.numWorkers.get() == newNumThreads);
      assertEquals(newNumThreads, fate.getTxRunnersActive());
      // finish work
      env.isReadyLatch.countDown();
      Wait.waitFor(() -> env.numWorkers.get() == 0);
      // workers should still be running since we haven't shutdown, just not working on anything
      assertEquals(newNumThreads, fate.getTxRunnersActive());
    } finally {
      fate.shutdown(1, TimeUnit.MINUTES);
      assertEquals(0, fate.getTxRunnersActive());
    }
  }

  @Test
  public void testDecreaseSize() throws Exception {
    executeTest(this::testDecreaseSize);
  }

  protected void testDecreaseSize(FateStore<PoolResizeTestEnv> store, ServerContext sctx)
      throws Exception {
    final int numThreads = 20;
    final int newNumThreads = 10;
    final ConfigurationCopy config = initConfig(numThreads);
    final var env = new PoolResizeTestEnv();
    final Fate<PoolResizeTestEnv> fate = new FastFate<>(env, store, false, r -> r + "", config);
    try {
      // create a tx for each thread
      for (int i = 0; i < numThreads; i++) {
        var fateId = fate.startTransaction();
        fate.seedTransaction(TEST_FATE_OP, fateId, new PoolResizeTestRepo(), true, "testing");
      }
      // wait for all threads to be working on a tx
      Wait.waitFor(() -> env.numWorkers.get() == numThreads);
      assertEquals(numThreads, fate.getTxRunnersActive());
      config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, String.valueOf(newNumThreads));
      // ensure another execution of the pool watcher task occurs after we change the size
      Thread.sleep(fate.getPoolWatcherDelay().toMillis() * 2);
      // at this point, FATE should detect that there are more transaction runners running than
      // configured. None can be safely stopped yet, (still in progress - haven't passed isReady).
      // We ensure none have been unexpectedly stopped, then we allow isReady to pass and the txns
      // to complete
      assertEquals(numThreads, env.numWorkers.get());
      env.isReadyLatch.countDown();
      // wait for the pool size to be decreased to the expected value
      Wait.waitFor(() -> fate.getTxRunnersActive() == newNumThreads);
      // wait for all threads to have completed their tx
      Wait.waitFor(() -> env.numWorkers.get() == 0);
      // wait a bit longer to ensure no more workers than expected were stopped
      Thread.sleep(5_000);
      assertEquals(newNumThreads, fate.getTxRunnersActive());
    } finally {
      fate.shutdown(1, TimeUnit.MINUTES);
      assertEquals(0, fate.getTxRunnersActive());
    }
  }

  private ConfigurationCopy initConfig(int numThreads) {
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, String.valueOf(numThreads));
    config.set(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL, "60m");
    return config;
  }

  public static class PoolResizeTestRepo implements Repo<PoolResizeTestEnv> {
    private static final long serialVersionUID = 1L;

    @Override
    public long isReady(FateId fateId, PoolResizeTestEnv environment) throws Exception {
      environment.numWorkers.incrementAndGet();
      environment.isReadyLatch.await();
      return 0;
    }

    @Override
    public String getName() {
      return this.getClass().getSimpleName();
    }

    @Override
    public Repo<PoolResizeTestEnv> call(FateId fateId, PoolResizeTestEnv environment)
        throws Exception {
      environment.numWorkers.decrementAndGet();
      return null;
    }

    @Override
    public void undo(FateId fateId, PoolResizeTestEnv environment) throws Exception {

    }

    @Override
    public String getReturn() {
      return null;
    }
  }

  public static class PoolResizeTestEnv extends FateTestRunner.TestEnv {
    private final AtomicInteger numWorkers = new AtomicInteger(0);
    private final CountDownLatch isReadyLatch = new CountDownLatch(1);
  }
}

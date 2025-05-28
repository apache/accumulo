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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;

/**
 * Tests the functionality of the FATE pools watcher task
 */
public abstract class FatePoolsWatcherITBase extends SharedMiniClusterBase
    implements FateTestRunner<FatePoolsWatcherITBase.PoolResizeTestEnv> {

  private static final Set<Fate.FateOperation> ALL_USER_FATE_OPS =
      Fate.FateOperation.getAllUserFateOps();
  private static final Set<Fate.FateOperation> ALL_META_FATE_OPS =
      Fate.FateOperation.getAllMetaFateOps();
  private static final Set<Fate.FateOperation> USER_FATE_OPS_SET1 =
      ALL_USER_FATE_OPS.stream().limit(ALL_USER_FATE_OPS.size() / 2).collect(Collectors.toSet());
  private static final Set<Fate.FateOperation> USER_FATE_OPS_SET2 =
      ALL_USER_FATE_OPS.stream().skip(ALL_USER_FATE_OPS.size() / 2).collect(Collectors.toSet());
  private static final Set<Fate.FateOperation> USER_FATE_OPS_SET3 =
      ALL_USER_FATE_OPS.stream().skip(ALL_USER_FATE_OPS.size() / 2 + 1).collect(Collectors.toSet());
  private static final Set<Fate.FateOperation> USER_FATE_OPS_SET4 = ALL_USER_FATE_OPS.stream()
      .skip(ALL_USER_FATE_OPS.size() / 2).limit(1).collect(Collectors.toSet());
  private static final Set<Fate.FateOperation> META_FATE_OPS_SET1 =
      ALL_META_FATE_OPS.stream().limit(ALL_META_FATE_OPS.size() / 2).collect(Collectors.toSet());
  private static final Set<Fate.FateOperation> META_FATE_OPS_SET2 =
      ALL_META_FATE_OPS.stream().skip(ALL_META_FATE_OPS.size() / 2).collect(Collectors.toSet());
  private static final Set<Fate.FateOperation> META_FATE_OPS_SET3 =
      ALL_META_FATE_OPS.stream().skip(ALL_META_FATE_OPS.size() / 2 + 1).collect(Collectors.toSet());
  private static final Set<Fate.FateOperation> META_FATE_OPS_SET4 = ALL_META_FATE_OPS.stream()
      .skip(ALL_META_FATE_OPS.size() / 2).limit(1).collect(Collectors.toSet());

  @Test
  public void testIncrease1() throws Exception {
    executeTest(this::testIncrease1);
  }

  protected void testIncrease1(FateStore<PoolResizeTestEnv> store, ServerContext sctx)
      throws Exception {
    // Tests changing the config for the FATE thread pools from
    // {<half the FATE ops/SET1>}: 4 <-- FateExecutor1
    // {<other half/SET2>}: 5 <-- FateExecutor2
    // ---->
    // {<half the FATE ops/SET1>}: 10 <-- FateExecutor1
    // {<other half minus one/SET3>}: 9 <-- FateExecutor3
    // {<remaining FATE op/SET4>}: 8 <-- FateExecutor4
    // This tests inc size of FATE thread pools for FateExecutors with unchanged fate ops, stopping
    // FateExecutors that are no longer valid (while ensuring none are stopped while in progress on
    // a transaction), and creating new FateExecutors as needed. Essentially, FateExecutor1's pool
    // size should be increased and FateExecutor3 and 4 should replace 2.

    boolean allAssertsOccurred = false;
    final ConfigurationCopy config = initConfigIncTest1();
    final var env = new PoolResizeTestEnv();
    final Fate<PoolResizeTestEnv> fate = new FastFate<>(env, store, false, r -> r + "", config);
    boolean isUserStore = store.type() == FateInstanceType.USER;
    final Set<Fate.FateOperation> set1 = isUserStore ? USER_FATE_OPS_SET1 : META_FATE_OPS_SET1;
    final Set<Fate.FateOperation> set2 = isUserStore ? USER_FATE_OPS_SET2 : META_FATE_OPS_SET2;
    final Set<Fate.FateOperation> set3 = isUserStore ? USER_FATE_OPS_SET3 : META_FATE_OPS_SET3;
    final Set<Fate.FateOperation> set4 = isUserStore ? USER_FATE_OPS_SET4 : META_FATE_OPS_SET4;
    final Fate.FateOperation fateOpFromSet1 = set1.iterator().next();
    final Fate.FateOperation fateOpFromSet2 = set2.iterator().next();
    final int numWorkersSet1 = 4;
    final int newNumWorkersSet1 = 10;
    final int numWorkersSet2 = 5;
    final int numWorkersSet3 = 9;
    final int numWorkersSet4 = 8;

    try {
      // create one transaction for each FateExecutor to work on
      fate.seedTransaction(fateOpFromSet1, fate.startTransaction(), new PoolResizeTestRepo(), true,
          "testing");
      fate.seedTransaction(fateOpFromSet2, fate.startTransaction(), new PoolResizeTestRepo(), true,
          "testing");

      // wait for the FateExecutors to work on the transactions
      Wait.waitFor(() -> env.numWorkers.get() == 2);
      // sum has been verified, verify each term
      Map<Fate.FateOperation,
          Long> seenCounts = store.list()
              .filter(fateIdStatus -> fateIdStatus.getFateOperation().isPresent()
                  && fateIdStatus.getFateReservation().isPresent())
              .collect(Collectors.groupingBy(fis -> fis.getFateOperation().orElseThrow(),
                  Collectors.counting()));
      Map<Fate.FateOperation,Long> expectedCounts = Map.of(fateOpFromSet1, 1L, fateOpFromSet2, 1L);
      assertEquals(expectedCounts, seenCounts);

      // wait for all transaction runners to be active
      Wait.waitFor(() -> fate.getTotalTxRunnersActive() == numWorkersSet1 + numWorkersSet2);
      // sum has been verified, verify each term
      assertEquals(numWorkersSet1, fate.getTxRunnersActive(set1));
      assertEquals(numWorkersSet2, fate.getTxRunnersActive(set2));

      changeConfigIncTest1(config);

      // After changing the config, the fate pool watcher should detect the change and increase the
      // pool size for the pool assigned to work on SET1
      Wait.waitFor(() -> fate.getTotalTxRunnersActive()
          == newNumWorkersSet1 + 1 + numWorkersSet3 + numWorkersSet4);
      // sum has been verified, verify each term
      assertEquals(newNumWorkersSet1, fate.getTxRunnersActive(set1));
      // The FateExecutor assigned to SET2 is no longer valid after the config change, so a
      // shutdown should be initiated and all the runners but the one working on a transaction
      // should be stopped.
      assertEquals(1, fate.getTxRunnersActive(set2));
      // New FateExecutors should be created for SET3 and SET4
      assertEquals(numWorkersSet3, fate.getTxRunnersActive(set3));
      assertEquals(numWorkersSet4, fate.getTxRunnersActive(set4));

      // num actively executing tasks should not be affected
      assertEquals(2, env.numWorkers.get());
      // sum has been verified, verify each term
      seenCounts = store.list()
          .filter(fateIdStatus -> fateIdStatus.getFateOperation().isPresent()
              && fateIdStatus.getFateReservation().isPresent())
          .collect(Collectors.groupingBy(fis -> fis.getFateOperation().orElseThrow(),
              Collectors.counting()));
      expectedCounts = Map.of(fateOpFromSet1, 1L, fateOpFromSet2, 1L);
      assertEquals(expectedCounts, seenCounts);

      // finish work
      env.isReadyLatch.countDown();

      Wait.waitFor(() -> env.numWorkers.get() == 0);

      // workers should still be running: we haven't shutdown FATE, just not working on anything
      Wait.waitFor(() -> fate.getTotalTxRunnersActive()
          == newNumWorkersSet1 + numWorkersSet3 + numWorkersSet4);
      // sum has been verified, verify each term
      assertEquals(newNumWorkersSet1, fate.getTxRunnersActive(set1));
      // The FateExecutor for SET2 should have finished work and be shutdown now since it was
      // previously invalidated by the config change and has since finished its assigned txn
      assertEquals(0, fate.getTxRunnersActive(set2));
      assertEquals(numWorkersSet3, fate.getTxRunnersActive(set3));
      assertEquals(numWorkersSet4, fate.getTxRunnersActive(set4));
      allAssertsOccurred = true;
    } catch (Exception e) {
      System.out.println("Failure: " + e);
    } finally {
      fate.shutdown(30, TimeUnit.SECONDS);
      assertTrue(allAssertsOccurred);
      assertEquals(0, fate.getTotalTxRunnersActive());
    }
  }

  @Test
  public void testIncrease2() throws Exception {
    executeTest(this::testIncrease2);
  }

  protected void testIncrease2(FateStore<PoolResizeTestEnv> store, ServerContext sctx) {
    // Tests changing the config for the FATE thread pools from
    // {<All FATE ops>}: 2 <-- FateExecutor1
    // ---->
    // {<All FATE ops>}: 3 <-- FateExecutor1
    // when 3 transactions need to be worked on. Ensures after the config change, the third tx
    // is picked up.
    boolean allAssertsOccurred = false;
    final ConfigurationCopy config = FateTestUtil.createTestFateConfig(2);
    final var env = new PoolResizeTestEnv();
    final Fate<PoolResizeTestEnv> fate = new FastFate<>(env, store, false, r -> r + "", config);
    final int numWorkers = 2;
    final int newNumWorkers = 3;
    final Set<Fate.FateOperation> allFateOps =
        store.type() == FateInstanceType.USER ? ALL_USER_FATE_OPS : ALL_META_FATE_OPS;
    final var fateOp = FateTestUtil.TEST_FATE_OP;
    try {
      for (int i = 0; i < newNumWorkers; i++) {
        fate.seedTransaction(fateOp, fate.startTransaction(), new PoolResizeTestRepo(), true,
            "testing");
      }

      // wait for the 2 threads to pick up 2 of the 3 transactions
      Wait.waitFor(() -> env.numWorkers.get() == numWorkers);
      Map<Fate.FateOperation,
          Long> seenCounts = store.list()
              .filter(fateIdStatus -> fateIdStatus.getFateOperation().isPresent()
                  && fateIdStatus.getFateReservation().isPresent())
              .collect(Collectors.groupingBy(fis -> fis.getFateOperation().orElseThrow(),
                  Collectors.counting()));
      Map<Fate.FateOperation,Long> expectedCounts = Map.of(fateOp, (long) numWorkers);
      assertEquals(expectedCounts, seenCounts);

      Wait.waitFor(() -> fate.getTotalTxRunnersActive() == numWorkers);
      assertEquals(numWorkers, fate.getTxRunnersActive(allFateOps));

      // increase the pool size
      changeConfigIncTest2(config, newNumWorkers);

      // wait for the final tx to be picked up
      Wait.waitFor(() -> env.numWorkers.get() == newNumWorkers);
      seenCounts = store.list()
          .filter(fateIdStatus -> fateIdStatus.getFateOperation().isPresent()
              && fateIdStatus.getFateReservation().isPresent())
          .collect(Collectors.groupingBy(fis -> fis.getFateOperation().orElseThrow(),
              Collectors.counting()));
      expectedCounts = Map.of(fateOp, (long) newNumWorkers);
      assertEquals(expectedCounts, seenCounts);

      Wait.waitFor(() -> fate.getTotalTxRunnersActive() == newNumWorkers);
      assertEquals(newNumWorkers, fate.getTxRunnersActive(allFateOps));

      // finish work
      env.isReadyLatch.countDown();

      Wait.waitFor(() -> env.numWorkers.get() == 0);

      // workers should still be running: we haven't shutdown FATE, just not working on anything
      Wait.waitFor(() -> fate.getTotalTxRunnersActive() == newNumWorkers);
      assertEquals(newNumWorkers, fate.getTxRunnersActive(allFateOps));

      allAssertsOccurred = true;
    } catch (Exception e) {
      System.out.println("Failure: " + e);
    } finally {
      fate.shutdown(30, TimeUnit.SECONDS);
      assertTrue(allAssertsOccurred);
      assertEquals(0, fate.getTotalTxRunnersActive());
    }
  }

  @Test
  public void testDecrease() throws Exception {
    executeTest(this::testDecrease);
  }

  protected void testDecrease(FateStore<PoolResizeTestEnv> store, ServerContext sctx)
      throws Exception {
    // Tests changing the config for the FATE thread pools from
    // {<half the FATE ops/SET1>}: 4 <-- FateExecutor1
    // {<other half minus one/SET3>}: 5 <-- FateExecutor2
    // {<remaining FATE op/SET4>}: 6 <-- FateExecutor3
    // ---->
    // {<half the FATE ops/SET1>}: 3 <-- FateExecutor1
    // {<other half/SET2>}: 2 <-- FateExecutor4
    // This tests dec size of FATE thread pools for FateExecutors with unchanged fate ops, stopping
    // FateExecutors that are no longer valid (while ensuring none are stopped while in progress on
    // a transaction), and creating new FateExecutors as needed. Essentially, FateExecutor1's pool
    // size should shrink and FateExecutor4 should replace 2 and 3.
    boolean allAssertsOccurred = false;
    final ConfigurationCopy config = initConfigDecTest();
    final var env = new PoolResizeTestEnv();
    final Fate<PoolResizeTestEnv> fate = new FastFate<>(env, store, false, r -> r + "", config);
    boolean isUserStore = store.type() == FateInstanceType.USER;
    final Set<Fate.FateOperation> set1 = isUserStore ? USER_FATE_OPS_SET1 : META_FATE_OPS_SET1;
    final Set<Fate.FateOperation> set2 = isUserStore ? USER_FATE_OPS_SET2 : META_FATE_OPS_SET2;
    final Set<Fate.FateOperation> set3 = isUserStore ? USER_FATE_OPS_SET3 : META_FATE_OPS_SET3;
    final Set<Fate.FateOperation> set4 = isUserStore ? USER_FATE_OPS_SET4 : META_FATE_OPS_SET4;
    final Fate.FateOperation fateOpFromSet1 = set1.iterator().next();
    final Fate.FateOperation fateOpFromSet3 = set3.iterator().next();
    final Fate.FateOperation fateOpFromSet4 = set4.iterator().next();
    final int numWorkersSet1 = 4;
    final int newNumWorkersSet1 = 3;
    final int numWorkersSet2 = 2;
    final int numWorkersSet3 = 5;
    final int numWorkersSet4 = 6;

    try {
      // create a tx for each thread
      for (int i = 0; i < numWorkersSet1; i++) {
        fate.seedTransaction(fateOpFromSet1, fate.startTransaction(), new PoolResizeTestRepo(),
            true, "testing");
      }
      for (int i = 0; i < numWorkersSet3; i++) {
        fate.seedTransaction(fateOpFromSet3, fate.startTransaction(), new PoolResizeTestRepo(),
            true, "testing");
      }
      for (int i = 0; i < numWorkersSet4; i++) {
        fate.seedTransaction(fateOpFromSet4, fate.startTransaction(), new PoolResizeTestRepo(),
            true, "testing");
      }
      // wait for all threads to be working on a tx
      Wait.waitFor(() -> env.numWorkers.get() == numWorkersSet1 + numWorkersSet3 + numWorkersSet4);
      // sum has been verified, verify each term
      Map<Fate.FateOperation,
          Long> seenCounts = store.list()
              .filter(fateIdStatus -> fateIdStatus.getFateOperation().isPresent()
                  && fateIdStatus.getFateReservation().isPresent())
              .collect(Collectors.groupingBy(fis -> fis.getFateOperation().orElseThrow(),
                  Collectors.counting()));
      Map<Fate.FateOperation,Long> expectedCounts = Map.of(fateOpFromSet1, (long) numWorkersSet1,
          fateOpFromSet3, (long) numWorkersSet3, fateOpFromSet4, (long) numWorkersSet4);
      assertEquals(expectedCounts, seenCounts);

      // wait for all transaction runners to be active
      Wait.waitFor(
          () -> fate.getTotalTxRunnersActive() == numWorkersSet1 + numWorkersSet3 + numWorkersSet4);
      // sum has been verified, verify each term
      assertEquals(numWorkersSet1, fate.getTxRunnersActive(set1));
      // this has not been set in the config yet so shouldn't exist
      assertEquals(0, fate.getTxRunnersActive(set2));
      assertEquals(numWorkersSet3, fate.getTxRunnersActive(set3));
      assertEquals(numWorkersSet4, fate.getTxRunnersActive(set4));

      changeConfigDecTest(config);

      // wait for another execution of the pool watcher task. This is signified by the start of
      // the executor for SET2.
      // At this point, FATE should detect that there are more tx runners running than configured
      // for the FateExecutor working on SET1, and should detect that the FateExecutors assigned to
      // SET3 and SET4 are no longer valid. None can safely be stopped yet, (still in progress -
      // haven't passed isReady). We ensure none have been unexpectedly stopped and that the new
      // executor has started (for SET2), then we allow isReady to pass and the txns to complete.
      Wait.waitFor(() -> fate.getTotalTxRunnersActive()
          == numWorkersSet1 + numWorkersSet2 + numWorkersSet3 + numWorkersSet4);
      // sum has been verified, verify each term
      assertEquals(numWorkersSet1, fate.getTxRunnersActive(set1));
      assertEquals(numWorkersSet2, fate.getTxRunnersActive(set2));
      assertEquals(numWorkersSet3, fate.getTxRunnersActive(set3));
      assertEquals(numWorkersSet4, fate.getTxRunnersActive(set4));

      // num actively executing tasks should not be affected
      assertEquals(numWorkersSet1 + numWorkersSet3 + numWorkersSet4, env.numWorkers.get());
      // sum has been verified, verify each term
      seenCounts = store.list()
          .filter(fateIdStatus -> fateIdStatus.getFateOperation().isPresent()
              && fateIdStatus.getFateReservation().isPresent())
          .collect(Collectors.groupingBy(fis -> fis.getFateOperation().orElseThrow(),
              Collectors.counting()));
      expectedCounts = Map.of(fateOpFromSet1, (long) numWorkersSet1, fateOpFromSet3,
          (long) numWorkersSet3, fateOpFromSet4, (long) numWorkersSet4);
      assertEquals(expectedCounts, seenCounts);

      // finish work
      env.isReadyLatch.countDown();

      // Wait for the expected changes to occur after work completes. The executors that were set
      // to shutdown (the executors assigned to SET3 and SET4) should have successfully shutdown,
      // the pool size for the executor for SET1 should have been updated, and the executor for
      // SET2 should still be running.
      Wait.waitFor(() -> env.numWorkers.get() == 0);

      Wait.waitFor(() -> fate.getTotalTxRunnersActive() == newNumWorkersSet1 + numWorkersSet2);
      // sum has been verified, verify each term
      assertEquals(newNumWorkersSet1, fate.getTxRunnersActive(set1));
      assertEquals(numWorkersSet2, fate.getTxRunnersActive(set2));
      // the FateExecutors for SET3 and SET4 should be fully shutdown now
      assertEquals(0, fate.getTxRunnersActive(set3));
      assertEquals(0, fate.getTxRunnersActive(set4));

      // wait a bit longer to ensure another iteration of the pool watcher check doesn't change
      // anything: everything is as expected now
      Thread.sleep(fate.getPoolWatcherDelay().toMillis() + 1_000);

      assertEquals(newNumWorkersSet1 + numWorkersSet2, fate.getTotalTxRunnersActive());
      assertEquals(newNumWorkersSet1, fate.getTxRunnersActive(set1));
      assertEquals(numWorkersSet2, fate.getTxRunnersActive(set2));
      assertEquals(0, fate.getTxRunnersActive(set3));
      assertEquals(0, fate.getTxRunnersActive(set4));
      allAssertsOccurred = true;
    } catch (Exception e) {
      System.out.println("Failure: " + e);
    } finally {
      fate.shutdown(30, TimeUnit.SECONDS);
      assertTrue(allAssertsOccurred);
      assertEquals(0, fate.getTotalTxRunnersActive());
    }
  }

  @Test
  public void testIdleCountHistory() throws Exception {
    executeTest(this::testIdleCountHistory);
  }

  protected void testIdleCountHistory(FateStore<PoolResizeTestEnv> store, ServerContext sctx)
      throws Exception {
    // Tests that a warning to increase pool size is logged when expected
    boolean allAssertsOccurred = false;
    var config = configIdleHistoryTest();
    final var env = new PoolResizeTestEnv();
    final Fate<PoolResizeTestEnv> fate = new FastFate<>(env, store, false, r -> r + "", config);
    try {
      // We have two worker threads. Submit 3 transactions that won't complete yet so we can check
      // for a warning
      for (int i = 0; i < 3; i++) {
        fate.seedTransaction(FateTestUtil.TEST_FATE_OP, fate.startTransaction(),
            new PoolResizeTestRepo(), true, "testing");
      }
      Wait.waitFor(() -> fate.getNeedMoreThreadsWarnCount().get() >= 1, 60_000, 1_000);
      // can finish work now
      env.isReadyLatch.countDown();
      Wait.waitFor(() -> env.numWorkers.get() == 0);
      allAssertsOccurred = true;
    } catch (Exception e) {
      System.out.println("Failure: " + e);
    } finally {
      fate.shutdown(30, TimeUnit.SECONDS);
      assertTrue(allAssertsOccurred);
      assertEquals(0, fate.getTotalTxRunnersActive());
    }
  }

  @Test
  public void testFatePoolsPartitioning() throws Exception {
    executeTest(this::testFatePoolsPartitioning);
  }

  protected void testFatePoolsPartitioning(FateStore<PoolResizeTestEnv> store, ServerContext sctx)
      throws Exception {
    // Ensures FATE ops are correctly partitioned between the pools. Configures 4 FateExecutors:
    // FateExecutor1 with 2 threads operating on 1/4 of FATE ops
    // FateExecutor2 with 3 threads operating on 1/4 of FATE ops
    // FateExecutor3 with 4 threads operating on 1/4 of FATE ops
    // FateExecutor4 with 5 threads operating on 1/4 of FATE ops
    // Seeds:
    // 5 transactions on FateExecutor1
    // 6 transactions on FateExecutor2
    // 1 transactions on FateExecutor3
    // 4 transactions on FateExecutor4
    // Ensures that we only see min(configured threads, transactions seeded) ever running
    // Also ensures that FateExecutors do not pick up any work that they shouldn't
    final int numThreadsPool1 = 2;
    final int numThreadsPool2 = 3;
    final int numThreadsPool3 = 4;
    final int numThreadsPool4 = 5;
    final int numSeedPool1 = 5;
    final int numSeedPool2 = 6;
    final int numSeedPool3 = 1;
    final int numSeedPool4 = 4;
    final long expectedRunningPool1 = Math.min(numThreadsPool1, numSeedPool1);
    final long expectedRunningPool2 = Math.min(numThreadsPool2, numSeedPool2);
    final long expectedRunningPool3 = Math.min(numThreadsPool3, numSeedPool3);
    final long expectedRunningPool4 = Math.min(numThreadsPool4, numSeedPool4);

    final int numUserOpsPerPool = ALL_USER_FATE_OPS.size() / 4;
    final int numMetaOpsPerPool = ALL_META_FATE_OPS.size() / 4;

    final Set<Fate.FateOperation> userPool1 =
        ALL_USER_FATE_OPS.stream().limit(numUserOpsPerPool).collect(Collectors.toSet());
    final Set<Fate.FateOperation> userPool2 = ALL_USER_FATE_OPS.stream().skip(numUserOpsPerPool)
        .limit(numUserOpsPerPool).collect(Collectors.toSet());
    final Set<Fate.FateOperation> userPool3 = ALL_USER_FATE_OPS.stream().skip(numUserOpsPerPool * 2)
        .limit(numUserOpsPerPool).collect(Collectors.toSet());
    // no limit for pool 4 in case total num ops is odd
    final Set<Fate.FateOperation> userPool4 =
        ALL_USER_FATE_OPS.stream().skip(numUserOpsPerPool * 3).collect(Collectors.toSet());

    final Set<Fate.FateOperation> metaPool1 =
        ALL_META_FATE_OPS.stream().limit(numMetaOpsPerPool).collect(Collectors.toSet());
    final Set<Fate.FateOperation> metaPool2 = ALL_META_FATE_OPS.stream().skip(numMetaOpsPerPool)
        .limit(numMetaOpsPerPool).collect(Collectors.toSet());
    final Set<Fate.FateOperation> metaPool3 = ALL_META_FATE_OPS.stream().skip(numMetaOpsPerPool * 2)
        .limit(numMetaOpsPerPool).collect(Collectors.toSet());
    // no limit for pool 4 in case total num ops is odd
    final Set<Fate.FateOperation> metaPool4 =
        ALL_META_FATE_OPS.stream().skip(numMetaOpsPerPool * 3).collect(Collectors.toSet());

    final ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_USER_CONFIG,
        String.format("{\"%s\": %s, \"%s\": %s, \"%s\": %s, \"%s\": %s}",
            userPool1.stream().map(Enum::name).collect(Collectors.joining(",")), numThreadsPool1,
            userPool2.stream().map(Enum::name).collect(Collectors.joining(",")), numThreadsPool2,
            userPool3.stream().map(Enum::name).collect(Collectors.joining(",")), numThreadsPool3,
            userPool4.stream().map(Enum::name).collect(Collectors.joining(",")), numThreadsPool4));
    config.set(Property.MANAGER_FATE_META_CONFIG,
        String.format("{\"%s\": %s, \"%s\": %s, \"%s\": %s, \"%s\": %s}",
            metaPool1.stream().map(Enum::name).collect(Collectors.joining(",")), numThreadsPool1,
            metaPool2.stream().map(Enum::name).collect(Collectors.joining(",")), numThreadsPool2,
            metaPool3.stream().map(Enum::name).collect(Collectors.joining(",")), numThreadsPool3,
            metaPool4.stream().map(Enum::name).collect(Collectors.joining(",")), numThreadsPool4));
    config.set(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL, "60m");

    final boolean isUserStore = store.type() == FateInstanceType.USER;

    final Fate.FateOperation fateOpFromPool1 =
        isUserStore ? userPool1.iterator().next() : metaPool1.iterator().next();
    final Fate.FateOperation fateOpFromPool2 =
        isUserStore ? userPool2.iterator().next() : metaPool2.iterator().next();
    final Fate.FateOperation fateOpFromPool3 =
        isUserStore ? userPool3.iterator().next() : metaPool3.iterator().next();
    final Fate.FateOperation fateOpFromPool4 =
        isUserStore ? userPool4.iterator().next() : metaPool4.iterator().next();

    final Set<Fate.FateOperation> pool1 = isUserStore ? userPool1 : metaPool1;
    final Set<Fate.FateOperation> pool2 = isUserStore ? userPool2 : metaPool2;
    final Set<Fate.FateOperation> pool3 = isUserStore ? userPool3 : metaPool3;
    final Set<Fate.FateOperation> pool4 = isUserStore ? userPool4 : metaPool4;

    boolean allAssertsOccurred = false;
    final var env = new PoolResizeTestEnv();
    final Fate<PoolResizeTestEnv> fate = new FastFate<>(env, store, false, r -> r + "", config);

    try {
      // seeding pool1/FateExecutor1
      for (int i = 0; i < numSeedPool1; i++) {
        fate.seedTransaction(fateOpFromPool1, fate.startTransaction(), new PoolResizeTestRepo(),
            true, "testing");
      }
      // seeding pool2/FateExecutor2
      for (int i = 0; i < numSeedPool2; i++) {
        fate.seedTransaction(fateOpFromPool2, fate.startTransaction(), new PoolResizeTestRepo(),
            true, "testing");
      }
      // seeding pool3/FateExecutor3
      for (int i = 0; i < numSeedPool3; i++) {
        fate.seedTransaction(fateOpFromPool3, fate.startTransaction(), new PoolResizeTestRepo(),
            true, "testing");
      }
      // seeding pool4/FateExecutor4
      for (int i = 0; i < numSeedPool4; i++) {
        fate.seedTransaction(fateOpFromPool4, fate.startTransaction(), new PoolResizeTestRepo(),
            true, "testing");
      }

      // wait for the threads to be working on the transactions
      Wait.waitFor(() -> env.numWorkers.get() == expectedRunningPool1 + expectedRunningPool2
          + expectedRunningPool3 + expectedRunningPool4);
      // sum has been verified, verify each term
      Map<Fate.FateOperation,
          Long> seenCounts = store.list()
              .filter(fateIdStatus -> fateIdStatus.getFateOperation().isPresent()
                  && fateIdStatus.getFateReservation().isPresent())
              .collect(Collectors.groupingBy(fis -> fis.getFateOperation().orElseThrow(),
                  Collectors.counting()));
      Map<Fate.FateOperation,Long> expectedCounts =
          Map.of(fateOpFromPool1, expectedRunningPool1, fateOpFromPool2, expectedRunningPool2,
              fateOpFromPool3, expectedRunningPool3, fateOpFromPool4, expectedRunningPool4);
      assertEquals(expectedCounts, seenCounts);

      // wait for all transaction runners to be active
      Wait.waitFor(() -> fate.getTotalTxRunnersActive()
          == numThreadsPool1 + numThreadsPool2 + numThreadsPool3 + numThreadsPool4);
      // sum has been verified, verify each term
      assertEquals(numThreadsPool1, fate.getTxRunnersActive(pool1));
      assertEquals(numThreadsPool2, fate.getTxRunnersActive(pool2));
      assertEquals(numThreadsPool3, fate.getTxRunnersActive(pool3));
      assertEquals(numThreadsPool4, fate.getTxRunnersActive(pool4));

      // wait a bit longer to ensure another iteration of the pool watcher check doesn't change
      // anything
      Thread.sleep(fate.getPoolWatcherDelay().toMillis() + 1_000);

      assertEquals(env.numWorkers.get(), expectedRunningPool1 + expectedRunningPool2
          + expectedRunningPool3 + expectedRunningPool4);
      // sum has been verified, verify each term
      seenCounts = store.list()
          .filter(fateIdStatus -> fateIdStatus.getFateOperation().isPresent()
              && fateIdStatus.getFateReservation().isPresent())
          .collect(Collectors.groupingBy(fis -> fis.getFateOperation().orElseThrow(),
              Collectors.counting()));
      expectedCounts =
          Map.of(fateOpFromPool1, expectedRunningPool1, fateOpFromPool2, expectedRunningPool2,
              fateOpFromPool3, expectedRunningPool3, fateOpFromPool4, expectedRunningPool4);
      assertEquals(expectedCounts, seenCounts);

      assertEquals(fate.getTotalTxRunnersActive(),
          numThreadsPool1 + numThreadsPool2 + numThreadsPool3 + numThreadsPool4);
      // sum has been verified, verify each term
      assertEquals(numThreadsPool1, fate.getTxRunnersActive(pool1));
      assertEquals(numThreadsPool2, fate.getTxRunnersActive(pool2));
      assertEquals(numThreadsPool3, fate.getTxRunnersActive(pool3));
      assertEquals(numThreadsPool4, fate.getTxRunnersActive(pool4));

      // can finish work now
      env.isReadyLatch.countDown();
      Wait.waitFor(() -> env.numWorkers.get() == 0);
      allAssertsOccurred = true;
    } finally {
      fate.shutdown(30, TimeUnit.SECONDS);
      assertTrue(allAssertsOccurred);
      assertEquals(0, fate.getTotalTxRunnersActive());
    }
  }

  private ConfigurationCopy initConfigIncTest1() {
    // {<half the FATE ops/SET1>}: 4
    // {<other half/SET2>}: 5
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_USER_CONFIG, "{\""
        + USER_FATE_OPS_SET1.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 4,\""
        + USER_FATE_OPS_SET2.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 5}");
    config.set(Property.MANAGER_FATE_META_CONFIG, "{\""
        + META_FATE_OPS_SET1.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 4,\""
        + META_FATE_OPS_SET2.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 5}");
    config.set(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL, "60m");
    return config;
  }

  private void changeConfigIncTest1(ConfigurationCopy config) {
    // {<half the FATE ops/SET1>}: 10
    // {<other half minus one/SET3>}: 9
    // {<remaining FATE op/SET4>}: 8
    config.set(Property.MANAGER_FATE_USER_CONFIG, "{\""
        + USER_FATE_OPS_SET1.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 10,"
        + "\"" + USER_FATE_OPS_SET3.stream().map(Enum::name).collect(Collectors.joining(","))
        + "\": 9,\"" + USER_FATE_OPS_SET4.stream().map(Enum::name).collect(Collectors.joining(","))
        + "\": 8}");
    config.set(Property.MANAGER_FATE_META_CONFIG, "{\""
        + META_FATE_OPS_SET1.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 10,"
        + "\"" + META_FATE_OPS_SET3.stream().map(Enum::name).collect(Collectors.joining(","))
        + "\": 9,\"" + META_FATE_OPS_SET4.stream().map(Enum::name).collect(Collectors.joining(","))
        + "\": 8}");
  }

  private void changeConfigIncTest2(ConfigurationCopy config, int numThreads) {
    config.set(Property.MANAGER_FATE_USER_CONFIG, "{\"" + Fate.FateOperation.getAllUserFateOps()
        .stream().map(Enum::name).collect(Collectors.joining(",")) + "\": " + numThreads + "}");
    config.set(Property.MANAGER_FATE_META_CONFIG, "{\"" + Fate.FateOperation.getAllMetaFateOps()
        .stream().map(Enum::name).collect(Collectors.joining(",")) + "\": " + numThreads + "}");
  }

  private ConfigurationCopy initConfigDecTest() {
    // {<half the FATE ops/SET1>}: 4
    // {<other half minus one/SET3>}: 5
    // {<remaining FATE op/SET4>}: 6
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_USER_CONFIG, "{\""
        + USER_FATE_OPS_SET1.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 4,"
        + "\"" + USER_FATE_OPS_SET3.stream().map(Enum::name).collect(Collectors.joining(","))
        + "\": 5,\"" + USER_FATE_OPS_SET4.stream().map(Enum::name).collect(Collectors.joining(","))
        + "\": 6}");
    config.set(Property.MANAGER_FATE_META_CONFIG, "{\""
        + META_FATE_OPS_SET1.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 4,"
        + "\"" + META_FATE_OPS_SET3.stream().map(Enum::name).collect(Collectors.joining(","))
        + "\": 5,\"" + META_FATE_OPS_SET4.stream().map(Enum::name).collect(Collectors.joining(","))
        + "\": 6}");
    config.set(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL, "60m");
    return config;
  }

  private void changeConfigDecTest(ConfigurationCopy config) {
    // {<half the FATE ops/SET1>}: 3
    // {<other half/SET2>}: 2
    config.set(Property.MANAGER_FATE_USER_CONFIG, "{\""
        + USER_FATE_OPS_SET1.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 3,\""
        + USER_FATE_OPS_SET2.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 2}");
    config.set(Property.MANAGER_FATE_META_CONFIG, "{\""
        + META_FATE_OPS_SET1.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 3,\""
        + META_FATE_OPS_SET2.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 2}");
  }

  private AccumuloConfiguration configIdleHistoryTest() {
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_USER_CONFIG, "{\""
        + ALL_USER_FATE_OPS.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 2}");
    config.set(Property.MANAGER_FATE_META_CONFIG, "{\""
        + ALL_META_FATE_OPS.stream().map(Enum::name).collect(Collectors.joining(",")) + "\": 2}");
    config.set(Property.MANAGER_FATE_IDLE_CHECK_INTERVAL, "1m");
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

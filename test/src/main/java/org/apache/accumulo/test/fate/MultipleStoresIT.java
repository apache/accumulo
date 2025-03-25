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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public abstract class MultipleStoresIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(MultipleStoresIT.class);

  @Test
  public void testReserveUnreserve() throws Exception {
    executeSleepingEnvTest(this::testReserveUnreserve);
  }

  private void testReserveUnreserve(TestStoreFactory<SleepingTestEnv> testStoreFactory)
      throws Exception {
    // reserving/unreserving a FateId should be reflected across instances of the stores
    final int numFateIds = 500;
    final List<FateStore.FateTxStore<SleepingTestEnv>> reservations = new ArrayList<>();
    final Set<FateId> allIds = new HashSet<>();
    final ZooUtil.LockID lock1 = new ZooUtil.LockID("/locks", "L1", 50);
    final ZooUtil.LockID lock2 = new ZooUtil.LockID("/locks", "L2", 52);
    Map<FateId,FateStore.FateReservation> activeReservations;
    final FateStore<SleepingTestEnv> store1 = testStoreFactory.create(lock1, null);
    final FateStore<SleepingTestEnv> store2 = testStoreFactory.create(lock2, null);
    final FateId fakeFateId = FateId.from(store1.type(), UUID.randomUUID());

    // Create the fate ids using store1
    for (int i = 0; i < numFateIds; i++) {
      assertTrue(allIds.add(store1.create()));
    }
    assertEquals(numFateIds, allIds.size());

    // Reserve half the fate ids using store1 and rest using store2, after reserving a fate id in
    // one, should not be able to reserve the same in the other. Should also not matter that all the
    // ids were created using store1
    int count = 0;
    for (FateId fateId : allIds) {
      if (count % 2 == 0) {
        reservations.add(store1.reserve(fateId));
        assertTrue(store2.tryReserve(fateId).isEmpty());
      } else {
        reservations.add(store2.reserve(fateId));
        assertTrue(store1.tryReserve(fateId).isEmpty());
      }
      count++;
    }
    // Try to reserve a non-existent fate id
    assertTrue(store1.tryReserve(fakeFateId).isEmpty());
    assertTrue(store2.tryReserve(fakeFateId).isEmpty());
    // Both stores should return the same reserved transactions
    activeReservations = store1.getActiveReservations();
    assertEquals(allIds, activeReservations.keySet());
    activeReservations = store2.getActiveReservations();
    assertEquals(allIds, activeReservations.keySet());

    // Test setting/getting the TStatus and unreserving the transactions
    for (int i = 0; i < allIds.size(); i++) {
      var reservation = reservations.get(i);
      assertEquals(ReadOnlyFateStore.TStatus.NEW, reservation.getStatus());
      reservation.setStatus(ReadOnlyFateStore.TStatus.SUBMITTED);
      assertEquals(ReadOnlyFateStore.TStatus.SUBMITTED, reservation.getStatus());
      reservation.delete();
      reservation.unreserve(Duration.ofMillis(0));
      // Attempt to set a status on a tx that has been unreserved (should throw exception)
      assertThrows(IllegalStateException.class,
          () -> reservation.setStatus(ReadOnlyFateStore.TStatus.NEW));
    }
    assertTrue(store1.getActiveReservations().isEmpty());
    assertTrue(store2.getActiveReservations().isEmpty());
  }

  @Test
  public void testReserveNonExistentTxn() throws Exception {
    executeSleepingEnvTest(this::testReserveNonExistentTxn);
  }

  private void testReserveNonExistentTxn(TestStoreFactory<SleepingTestEnv> testStoreFactory)
      throws Exception {
    // Tests that reserve() doesn't hang indefinitely and instead throws an error
    // on reserve() a non-existent transaction.
    final ZooUtil.LockID lock = new ZooUtil.LockID("/locks", "L1", 50);
    final FateStore<SleepingTestEnv> store = testStoreFactory.create(lock, null);
    final FateId fakeFateId = FateId.from(store.type(), UUID.randomUUID());

    var err = assertThrows(IllegalStateException.class, () -> store.reserve(fakeFateId));
    assertTrue(err.getMessage().contains(fakeFateId.canonical()));
  }

  @Test
  public void testReserveReservedAndUnreserveUnreserved() throws Exception {
    executeSleepingEnvTest(this::testReserveReservedAndUnreserveUnreserved);
  }

  private void testReserveReservedAndUnreserveUnreserved(
      TestStoreFactory<SleepingTestEnv> testStoreFactory) throws Exception {
    final int numFateIds = 500;
    final Set<FateId> allIds = new HashSet<>();
    final List<FateStore.FateTxStore<SleepingTestEnv>> reservations = new ArrayList<>();
    final ZooUtil.LockID lock = new ZooUtil.LockID("/locks", "L1", 50);
    final FateStore<SleepingTestEnv> store = testStoreFactory.create(lock, null);

    // Create some FateIds and ensure that they can be reserved
    for (int i = 0; i < numFateIds; i++) {
      FateId fateId = store.create();
      assertTrue(allIds.add(fateId));
      var reservation = store.tryReserve(fateId);
      assertFalse(reservation.isEmpty());
      reservations.add(reservation.orElseThrow());
    }
    assertEquals(numFateIds, allIds.size());

    // Try to reserve again, should not reserve
    for (FateId fateId : allIds) {
      assertTrue(store.tryReserve(fateId).isEmpty());
    }

    // Unreserve all the FateIds
    for (var reservation : reservations) {
      reservation.unreserve(Duration.ofMillis(0));
    }
    // Try to unreserve again (should throw exception)
    for (var reservation : reservations) {
      assertThrows(IllegalStateException.class, () -> reservation.unreserve(Duration.ofMillis(0)));
    }
  }

  @Test
  public void testReserveAfterUnreserveAndReserveAfterDeleted() throws Exception {
    executeSleepingEnvTest(this::testReserveAfterUnreserveAndReserveAfterDeleted);
  }

  private void testReserveAfterUnreserveAndReserveAfterDeleted(
      TestStoreFactory<SleepingTestEnv> testStoreFactory) throws Exception {
    final int numFateIds = 500;
    final Set<FateId> allIds = new HashSet<>();
    final List<FateStore.FateTxStore<SleepingTestEnv>> reservations = new ArrayList<>();
    final ZooUtil.LockID lock = new ZooUtil.LockID("/locks", "L1", 50);
    final FateStore<SleepingTestEnv> store = testStoreFactory.create(lock, null);

    // Create some FateIds and ensure that they can be reserved
    for (int i = 0; i < numFateIds; i++) {
      FateId fateId = store.create();
      assertTrue(allIds.add(fateId));
      var reservation = store.tryReserve(fateId);
      assertFalse(reservation.isEmpty());
      reservations.add(reservation.orElseThrow());
    }
    assertEquals(numFateIds, allIds.size());

    // Unreserve all
    for (var reservation : reservations) {
      reservation.unreserve(Duration.ofMillis(0));
    }

    // Ensure they can be reserved again, and delete and unreserve this time
    for (FateId fateId : allIds) {
      // Verify that the tx status is still NEW after unreserving since it hasn't been deleted
      assertEquals(ReadOnlyFateStore.TStatus.NEW, store.read(fateId).getStatus());
      var reservation = store.tryReserve(fateId);
      assertFalse(reservation.isEmpty());
      reservation.orElseThrow().delete();
      reservation.orElseThrow().unreserve(Duration.ofMillis(0));
    }

    for (FateId fateId : allIds) {
      // Verify that the tx is now unknown since it has been deleted
      assertEquals(ReadOnlyFateStore.TStatus.UNKNOWN, store.read(fateId).getStatus());
      // Attempt to reserve a deleted txn, should throw an exception and not wait indefinitely
      var err = assertThrows(IllegalStateException.class, () -> store.reserve(fateId));
      assertTrue(err.getMessage().contains(fateId.canonical()));
    }
  }

  @Test
  public void testMultipleFateInstances() throws Exception {
    executeSleepingEnvTest(this::testMultipleFateInstances);
  }

  private void testMultipleFateInstances(TestStoreFactory<SleepingTestEnv> testStoreFactory)
      throws Exception {
    final int numFateIds = 500;
    final Set<FateId> allIds = new HashSet<>();
    final SleepingTestEnv testEnv1 = new SleepingTestEnv(50);
    final SleepingTestEnv testEnv2 = new SleepingTestEnv(50);
    final ZooUtil.LockID lock1 = new ZooUtil.LockID("/locks", "L1", 50);
    final ZooUtil.LockID lock2 = new ZooUtil.LockID("/locks", "L2", 52);
    final Set<ZooUtil.LockID> liveLocks = new HashSet<>();
    final Predicate<ZooUtil.LockID> isLockHeld = liveLocks::contains;
    final FateStore<SleepingTestEnv> store1 = testStoreFactory.create(lock1, isLockHeld);
    final FateStore<SleepingTestEnv> store2 = testStoreFactory.create(lock2, isLockHeld);

    liveLocks.add(lock1);
    liveLocks.add(lock2);

    Fate<SleepingTestEnv> fate1 =
        new Fate<>(testEnv1, store1, true, Object::toString, DefaultConfiguration.getInstance());
    Fate<SleepingTestEnv> fate2 =
        new Fate<>(testEnv2, store2, false, Object::toString, DefaultConfiguration.getInstance());

    for (int i = 0; i < numFateIds; i++) {
      FateId fateId;
      // Start half the txns using fate1, and the other half using fate2
      if (i % 2 == 0) {
        fateId = fate1.startTransaction();
        fate1.seedTransaction(TEST_FATE_OP, fateId, new SleepingTestRepo(), true, "test");
      } else {
        fateId = fate2.startTransaction();
        fate2.seedTransaction(TEST_FATE_OP, fateId, new SleepingTestRepo(), true, "test");
      }
      allIds.add(fateId);
    }
    assertEquals(numFateIds, allIds.size());

    // Should be able to wait for completion on any fate instance
    for (FateId fateId : allIds) {
      fate2.waitForCompletion(fateId);
    }
    // Ensure that all txns have been executed and have only been executed once
    assertTrue(Collections.disjoint(testEnv1.executedOps, testEnv2.executedOps));
    assertEquals(allIds, Sets.union(testEnv1.executedOps, testEnv2.executedOps));

    fate1.shutdown(1, TimeUnit.MINUTES);
    fate2.shutdown(1, TimeUnit.MINUTES);
  }

  @Test
  public void testDeadReservationsCleanup() throws Exception {
    executeLatchEnvTest(this::testDeadReservationsCleanup);
  }

  private void testDeadReservationsCleanup(TestStoreFactory<LatchTestEnv> testStoreFactory)
      throws Exception {
    // Tests reserving some transactions, then simulating that the Manager died by creating
    // a new Fate instance and store with a new LockID. The transactions which were
    // reserved using the old LockID should be cleaned up by Fate's DeadReservationCleaner,
    // then picked up by the new Fate/store.

    // One transaction for each FATE worker thread
    final int numFateIds =
        Integer.parseInt(Property.MANAGER_FATE_THREADPOOL_SIZE.getDefaultValue());
    final Set<FateId> allIds = new HashSet<>();
    final LatchTestEnv testEnv1 = new LatchTestEnv();
    final LatchTestEnv testEnv2 = new LatchTestEnv();
    final ZooUtil.LockID lock1 = new ZooUtil.LockID("/locks", "L1", 50);
    final ZooUtil.LockID lock2 = new ZooUtil.LockID("/locks", "L2", 52);
    final Set<ZooUtil.LockID> liveLocks = new HashSet<>();
    final Predicate<ZooUtil.LockID> isLockHeld = liveLocks::contains;
    Map<FateId,FateStore.FateReservation> reservations;

    final FateStore<LatchTestEnv> store1 = testStoreFactory.create(lock1, isLockHeld);
    liveLocks.add(lock1);

    FastFate<LatchTestEnv> fate1 = new FastFate<>(testEnv1, store1, true, Object::toString,
        DefaultConfiguration.getInstance());

    // Ensure nothing is reserved yet
    assertTrue(store1.getActiveReservations().isEmpty());

    // Create transactions
    for (int i = 0; i < numFateIds; i++) {
      FateId fateId;
      fateId = fate1.startTransaction();
      fate1.seedTransaction(TEST_FATE_OP, fateId, new LatchTestRepo(), true, "test");
      allIds.add(fateId);
    }
    assertEquals(numFateIds, allIds.size());

    // Wait for all the fate worker threads to start working on the transactions
    Wait.waitFor(() -> testEnv1.numWorkers.get() == numFateIds);
    // Each fate worker will be hung up working (IN_PROGRESS) on a single transaction

    // Verify store1 has the transactions reserved and that they were reserved with lock1
    reservations = store1.getActiveReservations();
    assertEquals(allIds, reservations.keySet());
    reservations.values().forEach(res -> assertEquals(lock1, res.getLockID()));

    final FateStore<LatchTestEnv> store2 = testStoreFactory.create(lock2, isLockHeld);

    // Verify store2 can see the reserved transactions even though they were reserved using
    // store1
    reservations = store2.getActiveReservations();
    assertEquals(allIds, reservations.keySet());
    reservations.values().forEach(res -> assertEquals(lock1, res.getLockID()));

    // Simulate what would happen if the Manager using the Fate object (fate1) died.
    // isLockHeld would return false for the LockId of the Manager that died (in this case, lock1)
    // and true for the new Manager's lock (lock2)
    liveLocks.remove(lock1);
    liveLocks.add(lock2);

    // Create the new Fate/start the Fate threads (the work finder and the workers).
    // Don't run another dead reservation cleaner since we already have one running from fate1.
    Fate<LatchTestEnv> fate2 =
        new Fate<>(testEnv2, store2, false, Object::toString, DefaultConfiguration.getInstance());

    // Wait for the "dead" reservations to be deleted and picked up again (reserved using
    // fate2/store2/lock2 now).
    // They are considered "dead" if they are held by lock1 in this test. We don't have to worry
    // about fate1/store1/lock1 being used to reserve the transactions again since all
    // the workers for fate1 are hung up
    Wait.waitFor(() -> {
      Map<FateId,FateStore.FateReservation> store2Reservations = store2.getActiveReservations();
      boolean allReservedWithLock2 =
          store2Reservations.values().stream().allMatch(entry -> entry.getLockID().equals(lock2));
      return store2Reservations.keySet().equals(allIds) && allReservedWithLock2;
    }, fate1.getDeadResCleanupDelay().toMillis() * 2);

    // Finish work and shutdown
    testEnv1.workersLatch.countDown();
    testEnv2.workersLatch.countDown();
    fate1.shutdown(1, TimeUnit.MINUTES);
    fate2.shutdown(1, TimeUnit.MINUTES);
  }

  public static class SleepingTestRepo implements Repo<SleepingTestEnv> {
    private static final long serialVersionUID = 1L;

    @Override
    public long isReady(FateId fateId, SleepingTestEnv environment) {
      return 0;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Repo<SleepingTestEnv> call(FateId fateId, SleepingTestEnv environment) throws Exception {
      environment.executedOps.add(fateId);
      LOG.debug("Thread " + Thread.currentThread() + " in SleepingTestRepo.call() sleeping for "
          + environment.sleepTimeMs + " millis");
      Thread.sleep(environment.sleepTimeMs); // Simulate some work
      LOG.debug("Thread " + Thread.currentThread() + " finished SleepingTestRepo.call()");
      return null;
    }

    @Override
    public void undo(FateId fateId, SleepingTestEnv environment) {

    }

    @Override
    public String getReturn() {
      return null;
    }
  }

  public static class SleepingTestEnv extends MultipleStoresTestEnv {
    public final Set<FateId> executedOps = Collections.synchronizedSet(new HashSet<>());
    public final int sleepTimeMs;

    public SleepingTestEnv(int sleepTimeMs) {
      this.sleepTimeMs = sleepTimeMs;
    }
  }

  public static class LatchTestRepo implements Repo<LatchTestEnv> {
    private static final long serialVersionUID = 1L;

    @Override
    public long isReady(FateId fateId, LatchTestEnv environment) {
      return 0;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Repo<LatchTestEnv> call(FateId fateId, LatchTestEnv environment) throws Exception {
      LOG.debug("Thread " + Thread.currentThread() + " in LatchTestRepo.call()");
      environment.numWorkers.incrementAndGet();
      environment.workersLatch.await();
      LOG.debug("Thread " + Thread.currentThread() + " finished LatchTestRepo.call()");
      environment.numWorkers.decrementAndGet();
      return null;
    }

    @Override
    public void undo(FateId fateId, LatchTestEnv environment) {

    }

    @Override
    public String getReturn() {
      return null;
    }
  }

  public static class LatchTestEnv extends MultipleStoresTestEnv {
    public final AtomicInteger numWorkers = new AtomicInteger(0);
    public final CountDownLatch workersLatch = new CountDownLatch(1);
  }

  protected abstract void executeSleepingEnvTest(
      MultipleStoresTestExecutor<SleepingTestEnv> testMethod) throws Exception;

  protected abstract void executeLatchEnvTest(MultipleStoresTestExecutor<LatchTestEnv> testMethod)
      throws Exception;

  protected interface TestStoreFactory<T extends MultipleStoresTestEnv> {
    FateStore<T> create(ZooUtil.LockID lockID, Predicate<ZooUtil.LockID> isLockHeld)
        throws InterruptedException, KeeperException;
  }

  @FunctionalInterface
  protected interface MultipleStoresTestExecutor<T extends MultipleStoresTestEnv> {
    void execute(TestStoreFactory<T> fateStoreFactory) throws Exception;
  }

  protected static class MultipleStoresTestEnv extends FateTestRunner.TestEnv {}
}

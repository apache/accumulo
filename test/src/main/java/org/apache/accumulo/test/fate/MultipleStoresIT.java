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

import static org.apache.accumulo.test.fate.user.UserFateStoreIT.createFateTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.fate.AbstractFateStore;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.MetaFateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.common.collect.Sets;

// TODO 4131 could potentially have separate classes for testing MetaFateStore and UserFateStore
public class MultipleStoresIT extends SharedMiniClusterBase {

  @TempDir
  private static File tempDir;
  private static ZooKeeperTestingServer szk = null;
  private static ZooReaderWriter zk;
  private static final String FATE_DIR = "/fate";
  private ClientContext client;

  @BeforeEach
  public void beforeEachSetup() {
    client = (ClientContext) Accumulo.newClient().from(getClientProps()).build();
  }

  @AfterEach
  public void afterEachTeardown() {
    client.close();
  }

  @BeforeAll
  public static void beforeAllSetup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
    szk = new ZooKeeperTestingServer(tempDir);
    zk = szk.getZooReaderWriter();
  }

  @AfterAll
  public static void afterAllTeardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
    szk.close();
  }

  @Test
  public void testReserveUnreserve() throws Exception {
    testReserveUnreserve(FateInstanceType.META);
    testReserveUnreserve(FateInstanceType.USER);
  }

  protected void testReserveUnreserve(FateInstanceType storeType) throws Exception {
    // reserving/unreserving a FateId should be reflected across instances of the stores
    String tableName = getUniqueNames(1)[0];
    List<FateId> expReservedList = new ArrayList<>();
    final int numFateIds = 500;
    List<FateStore.FateTxStore<TestEnv>> reservations = new ArrayList<>();
    boolean isUserStore = storeType.equals(FateInstanceType.USER);
    Set<FateId> allIds = new HashSet<>();
    final AbstractFateStore<TestEnv> store1, store2;

    if (isUserStore) {
      createFateTable(client, tableName);
    }

    if (isUserStore) {
      store1 = new UserFateStore<>(client, tableName);
      store2 = new UserFateStore<>(client, tableName);
    } else {
      ZooUtil.LockID lock1 = new ZooUtil.LockID("/locks", "L1", 50);
      ZooUtil.LockID lock2 = new ZooUtil.LockID("/locks", "L2", 52);
      store1 = new MetaFateStore<>(FATE_DIR, zk, lock1);
      store2 = new MetaFateStore<>(FATE_DIR, zk, lock2);
    }

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
      expReservedList.add(fateId);
      if (count % 2 == 0) {
        reservations.add(store1.reserve(fateId));
        assertTrue(store2.tryReserve(fateId).isEmpty());
      } else {
        reservations.add(store2.reserve(fateId));
        assertTrue(store1.tryReserve(fateId).isEmpty());
      }
      count++;
    }
    // Both stores should return the same list of reserved transactions
    assertTrue(expReservedList.containsAll(store1.getReservedTxns())
        && expReservedList.size() == store1.getReservedTxns().size());
    assertTrue(expReservedList.containsAll(store2.getReservedTxns())
        && expReservedList.size() == store2.getReservedTxns().size());

    // Test setting/getting the TStatus and unreserving the transactions
    for (int i = 0; i < allIds.size(); i++) {
      var reservation = reservations.get(i);
      assertEquals(ReadOnlyFateStore.TStatus.NEW, reservation.getStatus());
      reservation.setStatus(ReadOnlyFateStore.TStatus.SUBMITTED);
      assertEquals(ReadOnlyFateStore.TStatus.SUBMITTED, reservation.getStatus());
      reservation.delete();
      reservation.unreserve(0, TimeUnit.MILLISECONDS);
      // Attempt to set a status on a tx that has been unreserved (should throw exception)
      try {
        reservation.setStatus(ReadOnlyFateStore.TStatus.NEW);
        fail();
      } catch (Exception e) {
        // Expected
      }
    }
    assertEquals(List.of(), store1.getReservedTxns());
    assertEquals(List.of(), store2.getReservedTxns());
  }

  @Test
  public void testReserveReservedAndUnreserveUnreserved() throws Exception {
    testReserveReservedAndUnreserveUnreserved(FateInstanceType.META);
    testReserveReservedAndUnreserveUnreserved(FateInstanceType.USER);
  }

  public void testReserveReservedAndUnreserveUnreserved(FateInstanceType storeType)
      throws Exception {
    String tableName = getUniqueNames(1)[0];
    final int numFateIds = 500;
    boolean isUserStore = storeType.equals(FateInstanceType.USER);
    Set<FateId> allIds = new HashSet<>();
    List<FateStore.FateTxStore<TestEnv>> reservations = new ArrayList<>();
    final AbstractFateStore<TestEnv> store;

    if (isUserStore) {
      createFateTable(client, tableName);
    }

    if (isUserStore) {
      store = new UserFateStore<>(client, tableName);
    } else {
      ZooUtil.LockID lock = new ZooUtil.LockID("/locks", "L1", 50);
      store = new MetaFateStore<>(FATE_DIR, zk, lock);
    }

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
      reservation.unreserve(0, TimeUnit.MILLISECONDS);
    }
    // Try to unreserve again (should throw exception)
    for (var reservation : reservations) {
      try {
        reservation.unreserve(0, TimeUnit.MILLISECONDS);
        fail();
      } catch (Exception e) {
        // Expected
      }
    }
  }

  @Test
  public void testReserveAfterUnreserveAndReserveAfterDeleted() throws Exception {
    testReserveAfterUnreserveAndReserveAfterDeleted(FateInstanceType.META);
    testReserveAfterUnreserveAndReserveAfterDeleted(FateInstanceType.USER);
  }

  public void testReserveAfterUnreserveAndReserveAfterDeleted(FateInstanceType storeType)
      throws Exception {
    String tableName = getUniqueNames(1)[0];
    final int numFateIds = 500;
    boolean isUserStore = storeType.equals(FateInstanceType.USER);
    Set<FateId> allIds = new HashSet<>();
    List<FateStore.FateTxStore<TestEnv>> reservations = new ArrayList<>();
    final AbstractFateStore<TestEnv> store;

    if (isUserStore) {
      createFateTable(client, tableName);
    }

    if (isUserStore) {
      store = new UserFateStore<>(client, tableName);
    } else {
      ZooUtil.LockID lock = new ZooUtil.LockID("/locks", "L1", 50);
      store = new MetaFateStore<>(FATE_DIR, zk, lock);
    }

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
      reservation.unreserve(0, TimeUnit.MILLISECONDS);
    }

    // Ensure they can be reserved again, and delete and unreserve this time
    for (FateId fateId : allIds) {
      // Verify that the tx status is still NEW after unreserving since it hasn't been deleted
      assertEquals(ReadOnlyFateStore.TStatus.NEW, store.read(fateId).getStatus());
      var reservation = store.tryReserve(fateId);
      assertFalse(reservation.isEmpty());
      reservation.orElseThrow().delete();
      reservation.orElseThrow().unreserve(0, TimeUnit.MILLISECONDS);
    }

    for (FateId fateId : allIds) {
      // Verify that the tx is now unknown since it has been deleted
      assertEquals(ReadOnlyFateStore.TStatus.UNKNOWN, store.read(fateId).getStatus());
      // Attempt to reserve a deleted txn, should throw an exception and not wait indefinitely
      try {
        store.reserve(fateId);
        fail();
      } catch (Exception e) {
        // Expected
      }
    }
  }

  @Test
  public void testMultipleFateInstances() throws Exception {
    testMultipleFateInstances(FateInstanceType.META);
    testMultipleFateInstances(FateInstanceType.USER);
  }

  public void testMultipleFateInstances(FateInstanceType storeType) throws Exception {
    String tableName = getUniqueNames(1)[0];
    final int numFateIds = 500;
    boolean isUserStore = storeType.equals(FateInstanceType.USER);
    Set<FateId> allIds = new HashSet<>();
    final AbstractFateStore<TestEnv> store1, store2;
    final TestEnv testEnv1 = new TestEnv();
    final TestEnv testEnv2 = new TestEnv();

    if (isUserStore) {
      createFateTable(client, tableName);
    }

    if (isUserStore) {
      store1 = new UserFateStore<>(client, tableName);
      store2 = new UserFateStore<>(client, tableName);
    } else {
      ZooUtil.LockID lock1 = new ZooUtil.LockID("/locks", "L1", 50);
      ZooUtil.LockID lock2 = new ZooUtil.LockID("/locks", "L2", 52);
      store1 = new MetaFateStore<>(FATE_DIR, zk, lock1);
      store2 = new MetaFateStore<>(FATE_DIR, zk, lock2);
    }

    Fate<TestEnv> fate1 =
        new Fate<>(testEnv1, store1, Object::toString, DefaultConfiguration.getInstance());
    Fate<TestEnv> fate2 =
        new Fate<>(testEnv2, store2, Object::toString, DefaultConfiguration.getInstance());

    for (int i = 0; i < numFateIds; i++) {
      FateId fateId;
      // Start half the txns using fate1, and the other half using fate2
      if (i % 2 == 0) {
        fateId = fate1.startTransaction();
        fate1.seedTransaction("op" + i, fateId, new TestRepo(), true, "test");
      } else {
        fateId = fate2.startTransaction();
        fate2.seedTransaction("op" + i, fateId, new TestRepo(), true, "test");
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

  public static class TestRepo implements Repo<TestEnv> {
    private static final long serialVersionUID = 1L;

    @Override
    public long isReady(FateId fateId, TestEnv environment) {
      return 0;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Repo<TestEnv> call(FateId fateId, TestEnv environment) throws Exception {
      environment.executedOps.add(fateId);
      Thread.sleep(50); // Simulate some work
      return null;
    }

    @Override
    public void undo(FateId fateId, TestEnv environment) {

    }

    @Override
    public String getReturn() {
      return null;
    }
  }

  public static class TestEnv {
    public final Set<FateId> executedOps = Collections.synchronizedSet(new HashSet<>());
  }
}

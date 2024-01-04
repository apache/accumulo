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
package org.apache.accumulo.test.fate.zookeeper;

import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.manager.PartitionData;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.google.common.collect.Sets;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class MultipleFateInstancesIT {

  @TempDir
  private static File tempDir;
  private static ZooKeeperTestingServer szk = null;
  private static ZooReaderWriter zk;

  private static final String FATE_DIR = "/fate";

  @BeforeAll
  public static void setup() throws Exception {
    szk = new ZooKeeperTestingServer(tempDir);
    zk = szk.getZooReaderWriter();
  }

  @AfterAll
  public static void teardown() throws Exception {
    szk.close();
  }

  @Test
  @Timeout(30)
  public void testZooStores() throws Exception {
    ZooUtil.LockID lock1 = new ZooUtil.LockID("/locks", "L1", 50);
    ZooUtil.LockID lock2 = new ZooUtil.LockID("/locks", "L2", 52);
    final ZooStore<Manager> zooStore1 = new ZooStore<Manager>(FATE_DIR, zk, lock1);
    final ZooStore<Manager> zooStore2 = new ZooStore<Manager>(FATE_DIR, zk, lock2);

    Set<Long> allIds = new HashSet<>();

    AtomicBoolean keepRunning = new AtomicBoolean(true);

    for (int i = 0; i < 100; i++) {
      assertTrue(allIds.add(zooStore1.create()));
      assertTrue(allIds.add(zooStore2.create()));
    }

    var pd1 = new PartitionData(0, 2);
    var pd2 = new PartitionData(1, 2);

    for (var txid : allIds) {
      if (txid % 2 == 0) {
        var rfo = zooStore1.reserve(txid);
        assertEquals(ReadOnlyFateStore.TStatus.NEW, rfo.getStatus());
        assertTrue(zooStore2.tryReserve(txid).isEmpty());
        rfo.setStatus(ReadOnlyFateStore.TStatus.SUBMITTED);
        rfo.unreserve(0);
      } else {
        var rfo = zooStore2.reserve(txid);
        assertEquals(ReadOnlyFateStore.TStatus.NEW, rfo.getStatus());
        assertTrue(zooStore1.tryReserve(txid).isEmpty());
        rfo.setStatus(ReadOnlyFateStore.TStatus.SUBMITTED);
        rfo.unreserve(0);
      }
    }

    HashSet<Long> runnable1 = new HashSet<>();
    zooStore1.runnable(keepRunning, pd1).forEachRemaining(txid -> assertTrue(runnable1.add(txid)));
    HashSet<Long> runnable2 = new HashSet<>();
    zooStore2.runnable(keepRunning, pd2).forEachRemaining(txid -> assertTrue(runnable2.add(txid)));

    assertFalse(runnable1.isEmpty());
    assertFalse(runnable2.isEmpty());
    assertTrue(Collections.disjoint(runnable1, runnable2));
    assertEquals(allIds, Sets.union(runnable1, runnable2));

    for (var txid : allIds) {
      var rfo = zooStore1.reserve(txid);
      assertTrue(zooStore2.tryReserve(txid).isEmpty());
      rfo.delete();
      rfo.unreserve(0);
    }

    for (var txid : allIds) {
      assertEquals(ReadOnlyFateStore.TStatus.UNKNOWN, zooStore1.read(txid).getStatus());
      assertEquals(ReadOnlyFateStore.TStatus.UNKNOWN, zooStore2.read(txid).getStatus());
      // TODO this will block forever, should probably throw an exception
      // zooStore1.reserve(txid);

    }
  }

  public static class TestEnv {
    public final Set<Long> executedOps = Collections.synchronizedSet(new HashSet<>());

  }

  public static class TestRepo implements Repo<TestEnv> {

    private static final long serialVersionUID = 1L;

    @Override
    public long isReady(long tid, TestEnv environment) throws Exception {
      return 0;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Repo<TestEnv> call(long tid, TestEnv environment) throws Exception {
      environment.executedOps.add(tid);
      return null;
    }

    @Override
    public void undo(long tid, TestEnv environment) throws Exception {

    }

    @Override
    public String getReturn() {
      return null;
    }
  }

  @Test
  public void testMultipleFateInstances() throws Exception {
    ZooUtil.LockID lock1 = new ZooUtil.LockID("/locks", "L1", 50);
    ZooUtil.LockID lock2 = new ZooUtil.LockID("/locks", "L2", 52);
    final ZooStore<TestEnv> zooStore1 = new ZooStore<>(FATE_DIR, zk, lock1);
    final ZooStore<TestEnv> zooStore2 = new ZooStore<>(FATE_DIR, zk, lock2);

    TestEnv testEnv1 = new TestEnv();
    TestEnv testEnv2 = new TestEnv();

    AtomicReference<PartitionData> partitionData1 = new AtomicReference<>(new PartitionData(0, 2));
    AtomicReference<PartitionData> partitionData2 = new AtomicReference<>(new PartitionData(1, 2));

    Fate<TestEnv> fate1 = new Fate<>(testEnv1, zooStore1, r -> "", partitionData1::get,
        DefaultConfiguration.getInstance());
    Fate<TestEnv> fate2 = new Fate<>(testEnv2, zooStore2, r -> "", partitionData2::get,
        DefaultConfiguration.getInstance());

    Set<Long> submittedIds = new HashSet<>();

    // submit 100 operations through fate1 instance. Even though all were submitted through a single
    // instance, both instances should pick them up and run.
    for (int i = 0; i < 100; i++) {
      var id = fate1.startTransaction();
      fate1.seedTransaction("op" + i, id, new TestRepo(), true, "test");
      submittedIds.add(id);
    }

    assertEquals(100, submittedIds.size());

    // should be able to wait for completion on any fate instance
    for (var id : submittedIds) {
      fate2.waitForCompletion(id);
    }

    // verify all fate ops ran
    assertEquals(submittedIds, Sets.union(testEnv1.executedOps, testEnv2.executedOps));
    // verify each op only ran in one fate instance
    assertTrue(Collections.disjoint(testEnv1.executedOps, testEnv2.executedOps));
    // verify both fate instances executed operations
    assertFalse(testEnv1.executedOps.isEmpty());
    assertFalse(testEnv2.executedOps.isEmpty());

    // create a third fate instance
    ZooUtil.LockID lock3 = new ZooUtil.LockID("/locks", "L3", 54);
    final ZooStore<TestEnv> zooStore3 = new ZooStore<>(FATE_DIR, zk, lock3);
    TestEnv testEnv3 = new TestEnv();
    AtomicReference<PartitionData> partitionData3 = new AtomicReference<>(new PartitionData(2, 3));
    Fate<TestEnv> fate3 = new Fate<>(testEnv3, zooStore3, r -> "", partitionData3::get,
        DefaultConfiguration.getInstance());

    // adjust the partition data for the existing fate instances, they should react to this change
    partitionData1.set(new PartitionData(0, 3));
    partitionData2.set(new PartitionData(1, 3));

    // clear the tracking sets from the last batch of operations executed
    submittedIds.clear();
    testEnv1.executedOps.clear();
    testEnv2.executedOps.clear();

    // execute another set of 100 operations, all three fate instances should pick these ops up
    for (int i = 0; i < 100; i++) {
      var id = fate3.startTransaction();
      fate3.seedTransaction("op2." + i, id, new TestRepo(), true, "test");
      submittedIds.add(id);
    }

    // should be able to wait for completion on any fate instance
    for (var id : submittedIds) {
      fate1.waitForCompletion(id);
    }

    // verify all fate ops ran
    assertEquals(submittedIds,
        Sets.union(Sets.union(testEnv1.executedOps, testEnv2.executedOps), testEnv3.executedOps));
    // verify each op only ran in one fate instance
    assertTrue(Collections.disjoint(testEnv1.executedOps, testEnv2.executedOps));
    assertTrue(Collections.disjoint(testEnv1.executedOps, testEnv3.executedOps));
    assertTrue(Collections.disjoint(testEnv2.executedOps, testEnv3.executedOps));
    // verify all fate instances executed operations
    assertFalse(testEnv1.executedOps.isEmpty());
    assertFalse(testEnv2.executedOps.isEmpty());
    assertFalse(testEnv3.executedOps.isEmpty());

    fate1.shutdown();
    fate2.shutdown();
    fate3.shutdown();
  }
}

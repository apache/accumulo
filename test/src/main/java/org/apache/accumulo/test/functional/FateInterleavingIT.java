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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus.SUBMITTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FateInterleavingIT extends SharedMiniClusterBase {

  public static class FirstOp extends ManagerRepo {

    private static final long serialVersionUID = 1L;

    protected static void insertTrackingData(long tid, Manager manager, String step)
        throws TableNotFoundException, MutationsRejectedException {
      try (BatchWriter bw = manager.getContext().createBatchWriter(FATE_TRACKING_TABLE)) {
        Mutation mut = new Mutation(Long.toString(System.currentTimeMillis()));
        mut.put("TXID:" + Long.toString(tid), "", step);
        bw.addMutation(mut);
      }
    }

    @Override
    public long isReady(long tid, Manager manager) throws Exception {
      Thread.sleep(500);
      insertTrackingData(tid, manager, this.getName() + "::isReady");
      return 0;
    }

    @Override
    public Repo<Manager> call(long tid, Manager manager) throws Exception {
      Thread.sleep(500);
      insertTrackingData(tid, manager, this.getName() + "::call");
      return new SecondOp();
    }

  }

  public static class SecondOp extends FirstOp {
    private static final long serialVersionUID = 1L;

    @Override
    public Repo<Manager> call(long tid, Manager environment) throws Exception {
      super.call(tid, environment);
      return new LastOp();
    }

  }

  public static class LastOp extends FirstOp {
    private static final long serialVersionUID = 1L;

    @Override
    public Repo<Manager> call(long tid, Manager environment) throws Exception {
      super.call(tid, environment);
      return null;
    }
  }

  public static class FateInterleavingConfig implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setProperty(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    }
  }

  private static final String FATE_TRACKING_TABLE = "fate_tracking";

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new FateInterleavingConfig());
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withInitialHostingGoal(TabletHostingGoal.ALWAYS);
      client.tableOperations().create(FATE_TRACKING_TABLE, ntc);
    }
  }

  @AfterAll
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @BeforeEach
  public void before() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().deleteRows(FATE_TRACKING_TABLE, null, null);
    }
  }

  private void waitFor(ZooStore<Manager> store, long txid) throws Exception {
    TStatus status;
    do {
      store.reserve(txid);
      try {
        status = store.getStatus(txid);
      } finally {
        store.unreserve(txid, 0);
      }
      if (status != TStatus.SUCCESSFUL) {
        Thread.sleep(50);
      }
    } while (status != TStatus.SUCCESSFUL);
  }

  @Test
  public void testDefaultInterleaving() throws Exception {

    // Connect to the ZooKeeper that MAC is using and insert FATE operations
    final String path = getCluster().getServerContext().getZooKeeperRoot() + Constants.ZFATE;
    final ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    final ZooStore<Manager> store = new org.apache.accumulo.core.fate.ZooStore<>(path, zrw);

    long txid = store.create();
    store.reserve(txid);
    try {
      store.push(txid, new FirstOp());
      store.setTransactionInfo(txid, TxInfo.TX_NAME, "TEST_ONE");
      store.setStatus(txid, SUBMITTED);
    } finally {
      store.unreserve(txid, 0);
    }

    long txid2 = store.create();
    store.reserve(txid2);
    try {
      store.push(txid2, new FirstOp());
      store.setTransactionInfo(txid2, TxInfo.TX_NAME, "TEST_TWO");
      store.setStatus(txid2, SUBMITTED);
    } finally {
      store.unreserve(txid2, 0);
    }

    long txid3 = store.create();
    store.reserve(txid3);
    try {
      store.push(txid3, new FirstOp());
      store.setTransactionInfo(txid3, TxInfo.TX_NAME, "TEST_THREE");
      store.setStatus(txid3, SUBMITTED);
    } finally {
      store.unreserve(txid3, 0);
    }

    waitFor(store, txid);
    waitFor(store, txid2);
    waitFor(store, txid3);

    // The execution order of the transactions is not according to their insertion
    // order. However, we do know that the first step of each transaction will be
    // executed before the second steps.
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Scanner scanner = client.createScanner(FATE_TRACKING_TABLE);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      SortedMap<Key,Value> subset = new TreeMap<>();
      AtomicInteger remaining = new AtomicInteger(6);

      Stream.generate(() -> null)
          .takeWhile(x -> (iter.hasNext() && remaining.getAndDecrement() > 0)).map(n -> iter.next())
          .forEach(e -> subset.put(e.getKey(), e.getValue()));

      assertTrue(
          subset.values().stream().allMatch(v -> new String(v.get(), UTF_8).startsWith("FirstOp")));
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid)).count());
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid2)).count());
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid3)).count());

      subset.clear();
      remaining.set(6);

      Stream.generate(() -> null)
          .takeWhile(x -> (iter.hasNext() && remaining.getAndDecrement() > 0)).map(n -> iter.next())
          .forEach(e -> subset.put(e.getKey(), e.getValue()));

      assertTrue(subset.values().stream()
          .allMatch(v -> new String(v.get(), UTF_8).startsWith("SecondOp")));
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid)).count());
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid2)).count());
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid3)).count());

      subset.clear();
      remaining.set(6);

      Stream.generate(() -> null)
          .takeWhile(x -> (iter.hasNext() && remaining.getAndDecrement() > 0)).map(n -> iter.next())
          .forEach(e -> subset.put(e.getKey(), e.getValue()));

      assertTrue(
          subset.values().stream().allMatch(v -> new String(v.get(), UTF_8).startsWith("LastOp")));
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid)).count());
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid2)).count());
      assertEquals(2, subset.keySet().stream()
          .filter(k -> k.getColumnFamily().toString().equals("TXID:" + txid3)).count());

      assertFalse(iter.hasNext());

    }
  }

  public static class FirstNonInterleavingOp extends FirstOp {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean interleave() {
      return false;
    }

    @Override
    public Repo<Manager> call(long tid, Manager manager) throws Exception {
      Thread.sleep(500);
      insertTrackingData(tid, manager, this.getName() + "::call");
      return new SecondNonInterleavingOp();
    }

  }

  public static class SecondNonInterleavingOp extends SecondOp {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean interleave() {
      return false;
    }

    @Override
    public Repo<Manager> call(long tid, Manager environment) throws Exception {
      super.call(tid, environment);
      return new LastNonInterleavingOp();
    }

  }

  public static class LastNonInterleavingOp extends LastOp {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean interleave() {
      return false;
    }

  }

  @Test
  public void testNonInterleaving() throws Exception {

    // Connect to the ZooKeeper that MAC is using and insert FATE operations
    final String path = getCluster().getServerContext().getZooKeeperRoot() + Constants.ZFATE;
    final ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    final ZooStore<Manager> store = new org.apache.accumulo.core.fate.ZooStore<>(path, zrw);

    long txid = store.create();
    store.reserve(txid);
    try {
      store.push(txid, new FirstNonInterleavingOp());
      store.setTransactionInfo(txid, TxInfo.TX_NAME, "TEST_ONE");
      store.setStatus(txid, SUBMITTED);
    } finally {
      store.unreserve(txid, 0);
    }

    long txid2 = store.create();
    store.reserve(txid2);
    try {
      store.push(txid2, new FirstNonInterleavingOp());
      store.setTransactionInfo(txid2, TxInfo.TX_NAME, "TEST_TWO");
      store.setStatus(txid2, SUBMITTED);
    } finally {
      store.unreserve(txid2, 0);
    }

    long txid3 = store.create();
    store.reserve(txid3);
    try {
      store.push(txid3, new FirstNonInterleavingOp());
      store.setTransactionInfo(txid3, TxInfo.TX_NAME, "TEST_THREE");
      store.setStatus(txid3, SUBMITTED);
    } finally {
      store.unreserve(txid3, 0);
    }

    waitFor(store, txid);
    waitFor(store, txid2);
    waitFor(store, txid3);

    // The execution order of the transactions is not according to their insertion
    // order. In this case, without interleaving, a transaction will run start to finish
    // before moving on to the next transaction
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Scanner scanner = client.createScanner(FATE_TRACKING_TABLE);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      SortedMap<Key,Value> subset = new TreeMap<>();
      AtomicInteger remaining = new AtomicInteger(6);

      Stream.generate(() -> null)
          .takeWhile(x -> (iter.hasNext() && remaining.getAndDecrement() > 0)).map(n -> iter.next())
          .forEach(e -> subset.put(e.getKey(), e.getValue()));

      Text firstTransactionId = subset.keySet().iterator().next().getColumnFamily();
      assertTrue(
          subset.keySet().stream().allMatch(k -> k.getColumnFamily().equals(firstTransactionId)));

      subset.clear();
      remaining.set(6);

      Stream.generate(() -> null)
          .takeWhile(x -> (iter.hasNext() && remaining.getAndDecrement() > 0)).map(n -> iter.next())
          .forEach(e -> subset.put(e.getKey(), e.getValue()));

      Text secondTransactionId = subset.keySet().iterator().next().getColumnFamily();
      assertTrue(
          subset.keySet().stream().allMatch(k -> k.getColumnFamily().equals(secondTransactionId)));

      subset.clear();
      remaining.set(6);

      Stream.generate(() -> null)
          .takeWhile(x -> (iter.hasNext() && remaining.getAndDecrement() > 0)).map(n -> iter.next())
          .forEach(e -> subset.put(e.getKey(), e.getValue()));

      Text thirdTransactionId = subset.keySet().iterator().next().getColumnFamily();
      assertTrue(
          subset.keySet().stream().allMatch(k -> k.getColumnFamily().equals(thirdTransactionId)));

      assertFalse(iter.hasNext());

    }
  }

}

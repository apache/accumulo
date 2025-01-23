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

import java.time.Duration;
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
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FateExecutionOrderIT extends SharedMiniClusterBase {

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

  public static class FateExecutionOrderITConfig implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setProperty(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    }
  }

  private static final String FATE_TRACKING_TABLE = "fate_tracking";

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new FateExecutionOrderITConfig());
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(FATE_TRACKING_TABLE);
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
        store.unreserve(txid, Duration.ZERO);
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
    final ZooSession zs = getCluster().getServerContext().getZooSession();
    final ZooStore<Manager> store = new org.apache.accumulo.core.fate.ZooStore<>(path, zs);

    long txid = store.create();
    store.reserve(txid);
    try {
      store.push(txid, new FirstOp());
      store.setTransactionInfo(txid, TxInfo.TX_NAME, "TEST_ONE");
      store.setStatus(txid, SUBMITTED);
    } finally {
      store.unreserve(txid, Duration.ZERO);
    }

    long txid2 = store.create();
    store.reserve(txid2);
    try {
      store.push(txid2, new FirstOp());
      store.setTransactionInfo(txid2, TxInfo.TX_NAME, "TEST_TWO");
      store.setStatus(txid2, SUBMITTED);
    } finally {
      store.unreserve(txid2, Duration.ZERO);
    }

    long txid3 = store.create();
    store.reserve(txid3);
    try {
      store.push(txid3, new FirstOp());
      store.setTransactionInfo(txid3, TxInfo.TX_NAME, "TEST_THREE");
      store.setStatus(txid3, SUBMITTED);
    } finally {
      store.unreserve(txid3, Duration.ZERO);
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

      // Print the order to the console
      scanner.iterator()
          .forEachRemaining((e) -> System.out.println(e.getKey() + " -> " + e.getValue()));

    }
  }

}

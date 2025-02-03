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

import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUBMITTED;
import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.SUCCESSFUL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateTestRunner.TestEnv;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public abstract class FateExecutionOrderIT extends SharedMiniClusterBase
    implements FateTestRunner<FateExecutionOrderIT.FeoTestEnv> {

  public static class FeoTestEnv extends TestEnv {
    private final AccumuloClient client;

    public FeoTestEnv(AccumuloClient client) {
      this.client = client;
    }

    AccumuloClient getClient() {
      return client;
    }
  }

  public static class FirstOp implements Repo<FateExecutionOrderIT.FeoTestEnv> {

    private static final long serialVersionUID = 1L;

    protected boolean isTrackingDataSet(FateId tid, FeoTestEnv env, String step) throws Exception {
      try (Scanner scanner = env.getClient().createScanner(FATE_TRACKING_TABLE)) {
        return scanner.stream()
            .anyMatch(e -> e.getKey().getColumnFamily().toString().equals(tid.canonical())
                && e.getValue().toString().equals(step));
      }
    }

    protected static void insertTrackingData(FateId tid, FeoTestEnv env, String step)
        throws TableNotFoundException, MutationsRejectedException {
      try (BatchWriter bw = env.getClient().createBatchWriter(FATE_TRACKING_TABLE)) {
        Mutation mut = new Mutation(Long.toString(System.currentTimeMillis()));
        mut.put(tid.canonical(), "", step);
        bw.addMutation(mut);
      }
    }

    @Override
    public long isReady(FateId tid, FeoTestEnv env) throws Exception {
      // First call to isReady will return that it's not ready (defer time of 100ms), inserting
      // the data 'isReady1' so we know isReady was called once. The second attempt (after the
      // deferral time) will pass as ready (return 0) and insert the data 'isReady2' so we know
      // the second call to isReady was made
      Thread.sleep(50);
      var step = this.getName() + "::isReady";
      if (isTrackingDataSet(tid, env, step + "1")) {
        insertTrackingData(tid, env, step + "2");
        return 0;
      } else {
        insertTrackingData(tid, env, step + "1");
        return 100;
      }
    }

    @Override
    public String getName() {
      return this.getClass().getSimpleName();
    }

    @Override
    public Repo<FeoTestEnv> call(FateId tid, FeoTestEnv env) throws Exception {
      Thread.sleep(50);
      insertTrackingData(tid, env, this.getName() + "::call");
      return new SecondOp();
    }

    @Override
    public void undo(FateId fateId, FeoTestEnv environment) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getReturn() {
      return "";
    }
  }

  public static class SecondOp extends FirstOp {
    private static final long serialVersionUID = 1L;

    @Override
    public Repo<FeoTestEnv> call(FateId tid, FeoTestEnv environment) throws Exception {
      super.call(tid, environment);
      return new LastOp();
    }

  }

  public static class LastOp extends FirstOp {
    private static final long serialVersionUID = 1L;

    @Override
    public Repo<FeoTestEnv> call(FateId tid, FeoTestEnv environment) throws Exception {
      super.call(tid, environment);
      return null;
    }
  }

  private static final String FATE_TRACKING_TABLE = "fate_tracking";

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withInitialTabletAvailability(TabletAvailability.HOSTED);
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

  private void waitFor(FateStore<FeoTestEnv> store, FateId txid) throws Exception {
    while (store.read(txid).getStatus() != SUCCESSFUL) {
      Thread.sleep(50);
    }
  }

  protected Fate<FeoTestEnv> initializeFate(AccumuloClient client, FateStore<FeoTestEnv> store) {
    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_THREADPOOL_SIZE, "2");
    config.set(Property.MANAGER_FATE_THREADPOOL_SIZE, "1");
    return new Fate<>(new FeoTestEnv(client), store, false, r -> r + "", config);
  }

  private static Entry<FateId,String> toIdStep(Entry<Key,Value> e) {
    return new AbstractMap.SimpleImmutableEntry<>(
        FateId.from(e.getKey().getColumnFamily().toString()), e.getValue().toString());
  }

  @Test
  public void testInterleaving() throws Exception {
    executeTest(this::testInterleaving);
  }

  protected void testInterleaving(FateStore<FeoTestEnv> store, ServerContext sctx)
      throws Exception {

    // This test verifies that FATE will interleave at least once between fate operations when
    // their isReady() returns > 0. Interleaving is not guaranteed, so we just check for one
    // occurrence which is highly unlikely to fail unless something is broken with FATE.
    // This test also ensures that the expected order of operations occurs per fate op.
    // Interleaving should have no effect on this.

    final int numFateIds = 3;
    FateId[] fateIds = new FateId[numFateIds];

    for (int i = 0; i < numFateIds; i++) {
      fateIds[i] = store.create();
      var txStore = store.reserve(fateIds[i]);
      try {
        txStore.push(new FirstOp());
        txStore.setTransactionInfo(TxInfo.TX_NAME, "TEST_" + i);
        txStore.setStatus(SUBMITTED);
      } finally {
        txStore.unreserve(Duration.ZERO);
      }
    }

    Fate<FeoTestEnv> fate = null;

    // The execution order of the transactions is not according to their insertion
    // order. However, we do know that the first step of each transaction will be
    // executed before the second steps.
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      fate = initializeFate(client, store);

      for (var fateId : fateIds) {
        waitFor(store, fateId);
      }

      Scanner scanner = client.createScanner(FATE_TRACKING_TABLE);
      var iter = scanner.stream().map(FateExecutionOrderIT::toIdStep).iterator();

      // we should see the following execution order for all fate ids:
      // FirstOp::isReady1, FirstOp::isReady2, FirstOp::call,
      // SecondOp::isReady1, SecondOp::isReady2, SecondOp::call,
      // LastOp::isReady1, LastOp::isReady2, LastOp::call
      // the first isReady of each op will defer the op to be executed later, allowing for the FATE
      // thread to interleave and work on another fate id, but may not always interleave.
      // It is unlikely that the FATE will not interleave at least once in a run, so we will check
      // for at least one occurrence.
      int interleaves = 0;
      int i = 0;
      Map.Entry<FateId,String> prevOp = null;
      var expRunOrder = List.of("FirstOp::isReady1", "FirstOp::isReady2", "FirstOp::call",
          "SecondOp::isReady1", "SecondOp::isReady2", "SecondOp::call", "LastOp::isReady1",
          "LastOp::isReady2", "LastOp::call");
      var fateIdsToExpRunOrder = Map.of(fateIds[0], new ArrayList<>(expRunOrder), fateIds[1],
          new ArrayList<>(expRunOrder), fateIds[2], new ArrayList<>(expRunOrder));

      while (iter.hasNext()) {
        var currOp = iter.next();
        FateId fateId = currOp.getKey();
        String currStep = currOp.getValue();
        var expRunOrderFateId = fateIdsToExpRunOrder.get(fateId);

        boolean passedFirstStep = !currStep.equals(expRunOrder.get(0));
        boolean prevFateIdDiffered = prevOp != null && !prevOp.getKey().equals(fateId);
        if (passedFirstStep && prevFateIdDiffered) {
          interleaves++;
        }
        assertEquals(currStep, expRunOrderFateId.remove(0));
        prevOp = currOp;
        i++;
      }

      assertTrue(interleaves > 0);
      assertEquals(i, expRunOrder.size() * numFateIds);
      assertEquals(numFateIds, fateIdsToExpRunOrder.size());
      for (var expRunOrderFateId : fateIdsToExpRunOrder.values()) {
        assertTrue(expRunOrderFateId.isEmpty());
      }

    } finally {
      if (fate != null) {
        fate.shutdown(10, TimeUnit.MINUTES);
      }
    }
  }

  public static class FirstNonInterleavingOp extends FirstOp {

    private static final long serialVersionUID = 1L;

    @Override
    public long isReady(FateId tid, FeoTestEnv env) throws Exception {
      Thread.sleep(50);
      insertTrackingData(tid, env, this.getName() + "::isReady");
      return 0;
    }

    @Override
    public Repo<FeoTestEnv> call(FateId tid, FeoTestEnv manager) throws Exception {
      Thread.sleep(50);
      insertTrackingData(tid, manager, this.getName() + "::call");
      return new SecondNonInterleavingOp();
    }
  }

  public static class SecondNonInterleavingOp extends FirstNonInterleavingOp {

    private static final long serialVersionUID = 1L;

    @Override
    public Repo<FeoTestEnv> call(FateId tid, FeoTestEnv environment) throws Exception {
      super.call(tid, environment);
      return new LastNonInterleavingOp();
    }

  }

  public static class LastNonInterleavingOp extends FirstNonInterleavingOp {

    private static final long serialVersionUID = 1L;

    @Override
    public Repo<FeoTestEnv> call(FateId tid, FeoTestEnv environment) throws Exception {
      super.call(tid, environment);
      return null;
    }

  }

  @Test
  public void testNonInterleaving() throws Exception {
    executeTest(this::testNonInterleaving);
  }

  protected void testNonInterleaving(FateStore<FeoTestEnv> store, ServerContext sctx)
      throws Exception {

    // This test ensures that when isReady() always returns zero that all the fate steps will
    // execute immediately

    final int numFateIds = 3;
    FateId[] fateIds = new FateId[numFateIds];

    for (int i = 0; i < numFateIds; i++) {
      fateIds[i] = store.create();
      var txStore = store.reserve(fateIds[i]);
      try {
        txStore.push(new FirstNonInterleavingOp());
        txStore.setTransactionInfo(TxInfo.TX_NAME, "TEST_" + i);
        txStore.setStatus(SUBMITTED);
      } finally {
        txStore.unreserve(Duration.ZERO);
      }
    }

    Fate<FeoTestEnv> fate = null;

    // The execution order of the transactions is not according to their insertion
    // order. In this case, without interleaving, a transaction will run start to finish
    // before moving on to the next transaction
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      fate = initializeFate(client, store);

      for (var fateId : fateIds) {
        waitFor(store, fateId);
      }

      Scanner scanner = client.createScanner(FATE_TRACKING_TABLE);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      SortedMap<Key,Value> subset = new TreeMap<>();

      // should see one fate op execute all of it steps
      var seenId1 = verifySameIds(iter, subset);
      // should see another fate op execute all of it steps
      var seenId2 = verifySameIds(iter, subset);
      // should see another fate op execute all of it steps
      var seenId3 = verifySameIds(iter, subset);

      assertEquals(Set.of(fateIds[0], fateIds[1], fateIds[2]), Set.of(seenId1, seenId2, seenId3));

      assertFalse(iter.hasNext());

    } finally {
      if (fate != null) {
        fate.shutdown(10, TimeUnit.MINUTES);
      }
    }
  }

  private FateId verifySameIds(Iterator<Entry<Key,Value>> iter, SortedMap<Key,Value> subset) {
    subset.clear();
    Iterators.limit(iter, 6).forEachRemaining(e -> subset.put(e.getKey(), e.getValue()));

    Text fateId = subset.keySet().iterator().next().getColumnFamily();
    assertTrue(subset.keySet().stream().allMatch(k -> k.getColumnFamily().equals(fateId)));

    // list is used to ensure correct operations and correct order of operations
    var expectedVals = List.of("FirstNonInterleavingOp::isReady", "FirstNonInterleavingOp::call",
        "SecondNonInterleavingOp::isReady", "SecondNonInterleavingOp::call",
        "LastNonInterleavingOp::isReady", "LastNonInterleavingOp::call");
    var actualVals = subset.values().stream().map(Value::toString).collect(Collectors.toList());
    assertEquals(expectedVals, actualVals);

    return FateId.from(fateId.toString());
  }

}

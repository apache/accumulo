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
package org.apache.accumulo.test.fate.meta;

import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.apache.accumulo.test.fate.TestLock.createDummyLockID;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.fate.AbstractFateStore.FateIdGenerator;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateStoreIT;
import org.apache.accumulo.test.fate.FateStoreUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class MetaFateStoreFateIT extends FateStoreIT {
  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setup() throws Exception {
    FateStoreUtil.MetaFateZKSetup.setup(tempDir);
  }

  @AfterAll
  public static void teardown() throws Exception {
    FateStoreUtil.MetaFateZKSetup.teardown();
  }

  @Override
  public void executeTest(FateTestExecutor<TestEnv> testMethod, int maxDeferred,
      FateIdGenerator fateIdGenerator) throws Exception {
    ServerContext sctx = createMock(ServerContext.class);
    expect(sctx.getZooKeeperRoot()).andReturn(FateStoreUtil.MetaFateZKSetup.getZkRoot()).anyTimes();
    expect(sctx.getZooSession()).andReturn(FateStoreUtil.MetaFateZKSetup.getZk()).anyTimes();
    replay(sctx);
    MetaFateStore<TestEnv> store = new MetaFateStore<>(
        FateStoreUtil.MetaFateZKSetup.getZkFatePath(), FateStoreUtil.MetaFateZKSetup.getZk(),
        createDummyLockID(), null, maxDeferred, fateIdGenerator);

    // Check that the store has no transactions before and after each test
    assertEquals(0, store.list().count());
    testMethod.execute(store, sctx);
    assertEquals(0, store.list().count());
  }

  @Override
  protected void deleteKey(FateId fateId, ServerContext sctx) {
    try {
      // We have to use reflection since the FateData is internal to the store

      Class<?> fateDataClass = Class.forName(MetaFateStore.class.getName() + "$FateData");
      // Constructor for constructing FateData
      Constructor<?> fateDataCons = fateDataClass.getDeclaredConstructor(TStatus.class,
          FateStore.FateReservation.class, FateKey.class, Deque.class, Map.class);
      // Constructor for constructing FateData from a byte array (the serialized form of FateData)
      Constructor<?> serializedCons = fateDataClass.getDeclaredConstructor(byte[].class);
      fateDataCons.setAccessible(true);
      serializedCons.setAccessible(true);

      // Get the status, reservation, repoDeque, txInfo fields so that they can be read and get the
      // serialize method
      Field status = fateDataClass.getDeclaredField("status");
      Field reservation = fateDataClass.getDeclaredField("reservation");
      Field repoDeque = fateDataClass.getDeclaredField("repoDeque");
      Field txInfo = fateDataClass.getDeclaredField("txInfo");
      Method nodeSerialize = fateDataClass.getDeclaredMethod("serialize");
      status.setAccessible(true);
      reservation.setAccessible(true);
      repoDeque.setAccessible(true);
      txInfo.setAccessible(true);
      nodeSerialize.setAccessible(true);

      // Gather the existing fields, create a new FateData object with those existing fields
      // (excluding the FateKey in the new object), and replace the zk node with this new FateData
      String txPath =
          FateStoreUtil.MetaFateZKSetup.getZkFatePath() + "/tx_" + fateId.getTxUUIDStr();
      Object currentNode = serializedCons.newInstance(
          new Object[] {FateStoreUtil.MetaFateZKSetup.getZk().asReader().getData(txPath)});
      TStatus currentStatus = (TStatus) status.get(currentNode);
      Optional<FateStore.FateReservation> currentReservation =
          getCurrentReservation(reservation, currentNode);
      @SuppressWarnings("unchecked")
      Deque<Repo<TestEnv>> currentRepoDeque = (Deque<Repo<TestEnv>>) repoDeque.get(currentNode);
      @SuppressWarnings("unchecked")
      Map<Fate.TxInfo,Serializable> currentTxInfo =
          (Map<Fate.TxInfo,Serializable>) txInfo.get(currentNode);
      Object newNode = fateDataCons.newInstance(currentStatus, currentReservation.orElse(null),
          null, currentRepoDeque, currentTxInfo);

      FateStoreUtil.MetaFateZKSetup.getZk().asReaderWriter().putPersistentData(txPath,
          (byte[]) nodeSerialize.invoke(newNode), NodeExistsPolicy.OVERWRITE);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private Optional<FateStore.FateReservation> getCurrentReservation(Field nodeReservation,
      Object currentNode) throws Exception {
    Object currentResAsObject = nodeReservation.get(currentNode);
    Optional<FateStore.FateReservation> currentReservation = Optional.empty();
    if (currentResAsObject instanceof Optional) {
      Optional<?> currentResAsOptional = (Optional<?>) currentResAsObject;
      if (currentResAsOptional.isPresent()
          && currentResAsOptional.orElseThrow() instanceof FateStore.FateReservation) {
        currentReservation =
            Optional.of((FateStore.FateReservation) currentResAsOptional.orElseThrow());
      }
    }
    return currentReservation;
  }
}

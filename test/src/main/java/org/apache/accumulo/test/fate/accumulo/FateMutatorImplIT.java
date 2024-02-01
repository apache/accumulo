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
package org.apache.accumulo.test.fate.accumulo;

import static org.apache.accumulo.core.fate.accumulo.FateMutator.Status.ACCEPTED;
import static org.apache.accumulo.core.fate.accumulo.FateMutator.Status.REJECTED;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.accumulo.FateMutatorImpl;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.fate.FateIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateMutatorImplIT extends SharedMiniClusterBase {

  Logger log = LoggerFactory.getLogger(FateMutatorImplIT.class);
  final NewTableConfiguration ntc =
      new NewTableConfiguration().withInitialHostingGoal(TabletHostingGoal.ALWAYS);

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void tearDown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Test
  public void putRepo() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table, ntc);

      ClientContext context = (ClientContext) client;

      final long tid = RANDOM.get().nextLong() & 0x7fffffffffffffffL;
      FateId fateId = FateId.from(FateInstanceType.USER, tid);

      // add some repos in order
      FateMutatorImpl<FateIT.TestEnv> fateMutator = new FateMutatorImpl<>(context, table, fateId);
      fateMutator.putRepo(100, new FateIT.TestRepo("test")).mutate();
      FateMutatorImpl<FateIT.TestEnv> fateMutator1 = new FateMutatorImpl<>(context, table, fateId);
      fateMutator1.putRepo(99, new FateIT.TestRepo("test")).mutate();
      FateMutatorImpl<FateIT.TestEnv> fateMutator2 = new FateMutatorImpl<>(context, table, fateId);
      fateMutator2.putRepo(98, new FateIT.TestRepo("test")).mutate();

      // make sure we cant add a repo that has already been added
      FateMutatorImpl<FateIT.TestEnv> fateMutator3 = new FateMutatorImpl<>(context, table, fateId);
      assertThrows(IllegalStateException.class,
          () -> fateMutator3.putRepo(98, new FateIT.TestRepo("test")).mutate(),
          "Repo in position 98 already exists. Expected to not be able to add it again.");
      FateMutatorImpl<FateIT.TestEnv> fateMutator4 = new FateMutatorImpl<>(context, table, fateId);
      assertThrows(IllegalStateException.class,
          () -> fateMutator4.putRepo(99, new FateIT.TestRepo("test")).mutate(),
          "Repo in position 99 already exists. Expected to not be able to add it again.");
    }
  }

  @Test
  public void requireStatus() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table, ntc);

      ClientContext context = (ClientContext) client;

      final long tid = RANDOM.get().nextLong() & 0x7fffffffffffffffL;
      FateId fateId = FateId.from(FateInstanceType.USER, tid);

      // use require status passing all statuses. without the status column present this should fail
      assertThrows(IllegalStateException.class,
          () -> new FateMutatorImpl<>(context, table, fateId)
              .requireStatus(ReadOnlyFateStore.TStatus.values())
              .putStatus(ReadOnlyFateStore.TStatus.NEW).mutate());
      assertEquals(0, client.createScanner(table).stream().count());
      var status = new FateMutatorImpl<>(context, table, fateId)
          .requireStatus(ReadOnlyFateStore.TStatus.values())
          .putStatus(ReadOnlyFateStore.TStatus.NEW).tryMutate();
      assertEquals(REJECTED, status);
      assertEquals(0, client.createScanner(table).stream().count());

      // use require status without passing any statuses to require that the status column is absent
      status = new FateMutatorImpl<>(context, table, fateId).requireStatus()
          .putStatus(ReadOnlyFateStore.TStatus.NEW).tryMutate();
      assertEquals(ACCEPTED, status);

      // try again with requiring an absent status column. this time it should fail because we just
      // put status NEW
      assertThrows(IllegalStateException.class,
          () -> new FateMutatorImpl<>(context, table, fateId).requireStatus()
              .putStatus(ReadOnlyFateStore.TStatus.NEW).mutate(),
          "Expected to not be able to use requireStatus() without passing any statuses");
      status = new FateMutatorImpl<>(context, table, fateId).requireStatus()
          .putStatus(ReadOnlyFateStore.TStatus.NEW).tryMutate();
      assertEquals(REJECTED, status,
          "Expected to not be able to use requireStatus() without passing any statuses");

      // now use require same with the current status, NEW passed in
      status =
          new FateMutatorImpl<>(context, table, fateId).requireStatus(ReadOnlyFateStore.TStatus.NEW)
              .putStatus(ReadOnlyFateStore.TStatus.SUBMITTED).tryMutate();
      assertEquals(ACCEPTED, status);

      // use require same with an array of statuses, none of which are the current status
      // (SUBMITTED)
      assertThrows(IllegalStateException.class,
          () -> new FateMutatorImpl<>(context, table, fateId)
              .requireStatus(ReadOnlyFateStore.TStatus.NEW, ReadOnlyFateStore.TStatus.UNKNOWN)
              .putStatus(ReadOnlyFateStore.TStatus.SUBMITTED).mutate(),
          "Expected to not be able to use requireStatus() with statuses that do not match the current status");
      status = new FateMutatorImpl<>(context, table, fateId)
          .requireStatus(ReadOnlyFateStore.TStatus.NEW, ReadOnlyFateStore.TStatus.UNKNOWN)
          .putStatus(ReadOnlyFateStore.TStatus.SUBMITTED).tryMutate();
      assertEquals(REJECTED, status,
          "Expected to not be able to use requireStatus() with statuses that do not match the current status");

      // use require same with an array of statuses, one of which is the current status (SUBMITTED)
      status = new FateMutatorImpl<>(context, table, fateId)
          .requireStatus(ReadOnlyFateStore.TStatus.UNKNOWN, ReadOnlyFateStore.TStatus.SUBMITTED)
          .putStatus(ReadOnlyFateStore.TStatus.IN_PROGRESS).tryMutate();
      assertEquals(ACCEPTED, status);

      // one more time check that we can use require same with the current status (IN_PROGRESS)
      status = new FateMutatorImpl<>(context, table, fateId)
          .requireStatus(ReadOnlyFateStore.TStatus.IN_PROGRESS)
          .putStatus(ReadOnlyFateStore.TStatus.FAILED_IN_PROGRESS).tryMutate();
      assertEquals(ACCEPTED, status);

    }

  }

  void logAllEntriesInTable(String tableName, AccumuloClient client) throws Exception {
    client.createScanner(tableName).forEach(e -> log.info(e.getKey() + " " + e.getValue()));
  }
}

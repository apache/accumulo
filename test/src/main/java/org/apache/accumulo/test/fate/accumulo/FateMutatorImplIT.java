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

import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
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

      // add some repos in order
      FateMutatorImpl<FateIT.TestEnv> fateMutator = new FateMutatorImpl<>(context, table, tid);
      fateMutator.putRepo(100, new FateIT.TestRepo("test")).mutate();
      FateMutatorImpl<FateIT.TestEnv> fateMutator1 = new FateMutatorImpl<>(context, table, tid);
      fateMutator1.putRepo(99, new FateIT.TestRepo("test")).mutate();
      FateMutatorImpl<FateIT.TestEnv> fateMutator2 = new FateMutatorImpl<>(context, table, tid);
      fateMutator2.putRepo(98, new FateIT.TestRepo("test")).mutate();

      // make sure we cant add a repo that has already been added
      FateMutatorImpl<FateIT.TestEnv> fateMutator3 = new FateMutatorImpl<>(context, table, tid);
      assertThrows(IllegalStateException.class,
          () -> fateMutator3.putRepo(98, new FateIT.TestRepo("test")).mutate(),
          "Repo in position 98 already exists. Expected to not be able to add it again.");
      FateMutatorImpl<FateIT.TestEnv> fateMutator4 = new FateMutatorImpl<>(context, table, tid);
      assertThrows(IllegalStateException.class,
          () -> fateMutator4.putRepo(99, new FateIT.TestRepo("test")).mutate(),
          "Repo in position 99 already exists. Expected to not be able to add it again.");
    }
  }

  void logAllEntriesInTable(String tableName, AccumuloClient client) throws Exception {
    client.createScanner(tableName).forEach(e -> log.info(e.getKey() + " " + e.getValue()));
  }
}

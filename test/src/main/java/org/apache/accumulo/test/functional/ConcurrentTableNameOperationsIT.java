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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ConcurrentTableNameOperationsIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterEach
  public void cleanUpTables() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      for (String table : client.tableOperations().list()) {
        if (!SystemTables.containsTableName(table)) {
          client.tableOperations().delete(table);
        }
      }
    }
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  /**
   * Test concurrent creation of tables with the same name.
   */
  @Test
  public void createTable() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final int numTasks = 10;
      final int numIterations = 10;
      ExecutorService pool = Executors.newFixedThreadPool(numTasks);
      final Set<String> tablesBefore = client.tableOperations().list();
      String[] expectedTableNames = getUniqueNames(numIterations);

      for (String tableName : expectedTableNames) {
        int successCount = runConcurrentOperation(pool, numTasks, (index) -> {
          client.tableOperations().create(tableName);
          return true;
        });

        assertEquals(1, successCount, "Expected only one create operation to succeed");
      }

      Set<String> tablesAfter = client.tableOperations().list();
      Set<String> expectedTables = Set.of(expectedTableNames);
      Set<String> actualNewTables = new HashSet<>(tablesAfter);
      actualNewTables.removeAll(tablesBefore);

      assertEquals(expectedTables, actualNewTables);

      pool.shutdown();
    }
  }

  /**
   * Test concurrent cloning of tables with the same target name.
   */
  @Test
  public void cloneTable() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final int numTasks = 10;
      final int numIterations = 10;
      ExecutorService pool = Executors.newFixedThreadPool(numTasks);

      for (String targetTableName : getUniqueNames(numIterations)) {
        List<String> sourceTableNames = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
          String sourceTable = targetTableName + "_source_" + i;
          client.tableOperations().create(sourceTable);
          sourceTableNames.add(sourceTable);
        }

        int tableCountBefore = client.tableOperations().list().size();

        int successCount = runConcurrentOperation(pool, numTasks, (index) -> {
          client.tableOperations().clone(sourceTableNames.get(index), targetTableName, true,
              Map.of(), Set.of());
          return true;
        });

        assertEquals(1, successCount, "Expected only one clone operation to succeed");
        assertTrue(client.tableOperations().exists(targetTableName),
            "Expected target table " + targetTableName + " to exist");
        assertEquals(tableCountBefore + 1, client.tableOperations().list().size(),
            "Expected only one new table after clone");
      }

      pool.shutdown();
    }
  }

  /**
   * Test concurrent renaming of tables to the same target name.
   */
  @Test
  public void renameTable() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final int numTasks = 10;
      final int numIterations = 10;
      ExecutorService pool = Executors.newFixedThreadPool(numTasks);

      for (String targetTableName : getUniqueNames(numIterations)) {
        List<String> sourceTableNames = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
          String sourceTable = targetTableName + "_rename_source_" + i;
          client.tableOperations().create(sourceTable);
          sourceTableNames.add(sourceTable);
        }

        int tableCountBefore = client.tableOperations().list().size();

        int successCount = runConcurrentOperation(pool, numTasks, (index) -> {
          client.tableOperations().rename(sourceTableNames.get(index), targetTableName);
          return true;
        });

        assertEquals(1, successCount, "Expected only one rename operation to succeed");
        assertTrue(client.tableOperations().exists(targetTableName),
            "Expected target table " + targetTableName + " to exist");
        assertEquals(tableCountBefore, client.tableOperations().list().size());
      }

      pool.shutdown();
    }
  }

  /**
   * Test that when several operations all target the same table name, only one operation
   * successfully creates that table.
   */
  @Test
  public void testMixedOperationsSameTargetName() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final int operationsPerType = 10;
      final int numTasks = operationsPerType * 3;
      final int numIterations = 3;
      ExecutorService pool = Executors.newFixedThreadPool(numTasks);
      String[] expectedTableNames = getUniqueNames(numIterations);

      for (String targetTableName : expectedTableNames) {
        List<String> cloneSourceTables = new ArrayList<>();
        List<String> renameSourceTables = new ArrayList<>();
        for (int i = 0; i < operationsPerType; i++) {
          String cloneSource = targetTableName + "_clone_src_" + i;
          client.tableOperations().create(cloneSource);
          cloneSourceTables.add(cloneSource);

          String renameSource = targetTableName + "_rename_src_" + i;
          client.tableOperations().create(renameSource);
          renameSourceTables.add(renameSource);
        }

        int tableCountBefore = client.tableOperations().list().size();

        List<Future<Boolean>> futures = new ArrayList<>();
        CountDownLatch startSignal = new CountDownLatch(1);
        AtomicInteger numTasksRunning = new AtomicInteger(0);
        AtomicReference<String> successfulOperation = new AtomicReference<>();

        for (int i = 0; i < operationsPerType; i++) {
          futures.add(pool.submit(() -> {
            try {
              numTasksRunning.incrementAndGet();
              startSignal.await();
              client.tableOperations().create(targetTableName);
              successfulOperation.set("create");
              return true;
            } catch (TableExistsException e) {
              return false;
            }
          }));

          final int index = i;

          futures.add(pool.submit(() -> {
            try {
              numTasksRunning.incrementAndGet();
              startSignal.await();
              client.tableOperations().rename(renameSourceTables.get(index), targetTableName);
              successfulOperation.set("rename");
              return true;
            } catch (TableExistsException e) {
              return false;
            }
          }));

          futures.add(pool.submit(() -> {
            try {
              numTasksRunning.incrementAndGet();
              startSignal.await();
              client.tableOperations().clone(cloneSourceTables.get(index), targetTableName, true,
                  Map.of(), Set.of());
              successfulOperation.set("clone");
              return true;
            } catch (TableExistsException e) {
              return false;
            }
          }));
        }

        Wait.waitFor(() -> numTasksRunning.get() == numTasks);
        startSignal.countDown();

        int successCount = 0;
        for (Future<Boolean> future : futures) {
          if (future.get()) {
            successCount++;
          }
        }

        assertEquals(1, successCount, "Expected only one operation to succeed");

        int tableCountAfter = client.tableOperations().list().size();
        assertTrue(client.tableOperations().exists(targetTableName),
            "Expected target table " + targetTableName + " to exist");

        String operation = successfulOperation.get();
        if ("create".equals(operation) || "clone".equals(operation)) {
          assertEquals(tableCountBefore + 1, tableCountAfter,
              "Expected +1 table count for " + operation);
        } else if ("rename".equals(operation)) {
          assertEquals(tableCountBefore, tableCountAfter, "Expected same table count for rename");
        }
      }

      pool.shutdown();
    }
  }

  private int runConcurrentOperation(ExecutorService pool, int numTasks,
      ConcurrentOperation operation) throws ExecutionException, InterruptedException {
    CountDownLatch startSignal = new CountDownLatch(1);
    AtomicInteger numTasksRunning = new AtomicInteger(0);
    List<Future<Boolean>> futures = new ArrayList<>();

    for (int i = 0; i < numTasks; i++) {
      final int index = i;
      futures.add(pool.submit(() -> {
        try {
          numTasksRunning.incrementAndGet();
          startSignal.await();
          return operation.execute(index);
        } catch (TableExistsException e) {
          return false;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    Wait.waitFor(() -> numTasksRunning.get() == numTasks); // wait for all tasks to be running
    startSignal.countDown(); // start all tasks at once

    int successCount = 0;
    for (Future<Boolean> future : futures) {
      if (future.get()) {
        successCount++;
      }
    }

    return successCount;
  }

  @FunctionalInterface
  private interface ConcurrentOperation {
    boolean execute(int index) throws Exception;
  }
}

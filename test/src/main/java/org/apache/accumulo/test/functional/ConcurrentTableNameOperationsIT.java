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

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;

public class ConcurrentTableNameOperationsIT extends AccumuloClusterHarness {

  /**
   * Test concurrent creation of tables with the same name.
   */
  @Test
  public void createTable() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final int numTasks = 10;
      final int numIterations = 30;
      ExecutorService pool = Executors.newFixedThreadPool(numTasks);
      final Set<String> tablesBefore = client.tableOperations().list();
      String[] expectedTableNames = getUniqueNames(numIterations);

      for (String tablename : expectedTableNames) {
        int successCount = runConcurrentOperation(pool, numTasks, (index) -> {
          client.tableOperations().create(tablename);
          return true;
        });

        assertEquals(1, successCount, "Expected exactly one create operation to succeed");
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
      final int numIterations = 5;
      ExecutorService pool = Executors.newFixedThreadPool(numTasks);

      for (String targetTableName : getUniqueNames(numIterations)) {
        List<String> sourceTableNames = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
          String sourceTable = targetTableName + "_source_" + i;
          client.tableOperations().create(sourceTable);
          sourceTableNames.add(sourceTable);
        }

        int successCount = runConcurrentOperation(pool, numTasks, (index) -> {
          client.tableOperations().clone(sourceTableNames.get(index), targetTableName, true,
              Map.of(), Set.of());
          return true;
        });

        assertEquals(1, successCount, "Expected exactly one clone operation to succeed");
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
      final int numIterations = 5;
      ExecutorService pool = Executors.newFixedThreadPool(numTasks);

      for (String targetTableName : getUniqueNames(numIterations)) {
        List<String> sourceTableNames = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
          String sourceTable = targetTableName + "_rename_source_" + i;
          client.tableOperations().create(sourceTable);
          sourceTableNames.add(sourceTable);
        }

        int successCount = runConcurrentOperation(pool, numTasks, (index) -> {
          client.tableOperations().rename(sourceTableNames.get(index), targetTableName);
          return true;
        });

        assertEquals(1, successCount, "Expected exactly one rename operation to succeed");
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

      for (String targetTableName : getUniqueNames(numIterations)) {
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

        List<Future<Boolean>> futures = new ArrayList<>();
        CountDownLatch startSignal = new CountDownLatch(1);
        AtomicInteger numTasksRunning = new AtomicInteger(0);

        for (int i = 0; i < operationsPerType; i++) {
          futures.add(pool.submit(() -> {
            try {
              numTasksRunning.incrementAndGet();
              startSignal.await();
              client.tableOperations().create(targetTableName);
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

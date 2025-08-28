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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Ensure that concurrent table and namespace operations that target the same name are handled
 * correctly.
 */
public class ConcurrentTableNameOperationsIT extends SharedMiniClusterBase {

  static AccumuloClient client;

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
    client = Accumulo.newClient().from(getClientProps()).build();
  }

  @AfterEach
  public void cleanUpTables() throws Exception {
    for (String table : client.tableOperations().list()) {
      if (!SystemTables.containsTableName(table)) {
        client.tableOperations().delete(table);
      }
    }
  }

  @AfterAll
  public static void teardown() {
    client.close();
    SharedMiniClusterBase.stopMiniCluster();
  }

  /**
   * Test concurrent cloning of tables with the same target name.
   */
  @Test
  public void cloneTable() throws Exception {
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

      int successCount = runConcurrentTableOperation(pool, numTasks, (index) -> {
        client.tableOperations().clone(sourceTableNames.get(index), targetTableName, true, Map.of(),
            Set.of());
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

  /**
   * Test concurrent renaming of tables to the same target name.
   */
  @Test
  public void renameTable() throws Exception {
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

      int successCount = runConcurrentTableOperation(pool, numTasks, (index) -> {
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

  /**
   * Test that when several threads attempt to import to the same table name simultaneously, only
   * one import succeeds.
   */
  @Test
  public void importTable() throws Exception {
    final int numTasks = 10;
    final int numIterations = 3;
    ExecutorService pool = Executors.newFixedThreadPool(numTasks);
    String[] targetTableNames = getUniqueNames(numIterations);
    var ntc = new NewTableConfiguration().createOffline();

    for (String importTableName : targetTableNames) {
      // Create separate source tables and export directories for each thread
      List<String> exportDirs = new ArrayList<>(numTasks);
      for (int i = 0; i < numTasks; i++) {
        String sourceTableName = importTableName + "_export_source_" + i;
        client.tableOperations().create(sourceTableName, ntc);
        String exportDir = getCluster().getTemporaryPath() + "/export_" + sourceTableName;
        client.tableOperations().exportTable(sourceTableName, exportDir);
        exportDirs.add(exportDir);
      }

      int tableCountBefore = client.tableOperations().list().size();

      // All threads attempt to import to the same target table name
      int successCount = runConcurrentTableOperation(pool, numTasks, (index) -> {
        client.tableOperations().importTable(importTableName, exportDirs.get(index));
        return true;
      });

      assertEquals(1, successCount, "Expected only one import operation to succeed");
      assertTrue(client.tableOperations().exists(importTableName),
          "Expected import table " + importTableName + " to exist");
      assertEquals(tableCountBefore + 1, client.tableOperations().list().size(),
          "Expected +1 table count for import operation");
    }

    pool.shutdown();
  }

  /**
   * Test that when several operations all target the same table name, only one operation
   * successfully creates that table.
   */
  @Test
  public void mixedTableOperations() throws Exception {
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
      CountDownLatch startSignal = new CountDownLatch(numTasks);
      AtomicReference<String> successfulOperation = new AtomicReference<>();

      for (int i = 0; i < operationsPerType; i++) {
        futures.add(pool.submit(() -> {
          try {
            startSignal.countDown();
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
            startSignal.countDown();
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
            startSignal.countDown();
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

  /**
   * Test that when several threads attempt to create the same namespace simultaneously, only one
   * creation succeeds.
   */
  @Test
  public void createNamespace() throws Exception {
    final int numTasks = 10;
    final int numIterations = 3;
    ExecutorService pool = Executors.newFixedThreadPool(numTasks);
    String[] targetNamespaceNames = getUniqueNames(numIterations);

    for (String namespaceName : targetNamespaceNames) {
      Set<String> namespacesBefore = client.namespaceOperations().list();

      int successCount = runConcurrentNamespaceOperation(pool, numTasks, (index) -> {
        client.namespaceOperations().create(namespaceName);
        return true;
      });

      assertEquals(1, successCount, "Expected only one create operation to succeed");
      assertTrue(client.namespaceOperations().exists(namespaceName),
          "Expected namespace " + namespaceName + " to exist");

      Set<String> namespacesAfter = client.namespaceOperations().list();
      Set<String> newNamespaces = new HashSet<>(namespacesAfter);
      newNamespaces.removeAll(namespacesBefore);
      assertEquals(Set.of(namespaceName), newNamespaces,
          "Expected exactly one new namespace: " + namespaceName);

      client.namespaceOperations().delete(namespaceName);
    }

    pool.shutdown();
  }

  /**
   * Test that when several threads attempt to rename to the same namespace name simultaneously,
   * only one rename succeeds.
   */
  @Test
  public void renameNamespace() throws Exception {
    final int numTasks = 10;
    final int numIterations = 3;
    ExecutorService pool = Executors.newFixedThreadPool(numTasks);
    String[] targetNamespaceNames = getUniqueNames(numIterations);

    for (String targetNamespaceName : targetNamespaceNames) {
      // multiple source namespaces for rename ops
      List<String> sourceNamespaces = new ArrayList<>();
      for (int i = 0; i < numTasks; i++) {
        String sourceNamespace = targetNamespaceName + "_source_" + i;
        client.namespaceOperations().create(sourceNamespace);
        sourceNamespaces.add(sourceNamespace);
      }

      Set<String> namespacesBefore = client.namespaceOperations().list();

      int successCount = runConcurrentNamespaceOperation(pool, numTasks, (index) -> {
        client.namespaceOperations().rename(sourceNamespaces.get(index), targetNamespaceName);
        return true;
      });

      assertEquals(1, successCount, "Expected only one rename operation to succeed");
      assertTrue(client.namespaceOperations().exists(targetNamespaceName),
          "Expected target namespace " + targetNamespaceName + " to exist");

      Set<String> namespacesAfter = client.namespaceOperations().list();
      assertEquals(namespacesBefore.size(), namespacesAfter.size(),
          "Expected same namespace count (rename operation)");
      assertTrue(namespacesAfter.contains(targetNamespaceName),
          "Expected target namespace in final list");

      for (String sourceNamespace : sourceNamespaces) {
        if (client.namespaceOperations().exists(sourceNamespace)) {
          client.namespaceOperations().delete(sourceNamespace);
        }
      }
      if (client.namespaceOperations().exists(targetNamespaceName)) {
        client.namespaceOperations().delete(targetNamespaceName);
      }
    }

    pool.shutdown();
  }

  private int runConcurrentTableOperation(ExecutorService pool, int numTasks,
      ConcurrentOperation operation) throws ExecutionException, InterruptedException {
    return runConcurrentOperation(pool, numTasks, operation, TableExistsException.class);
  }

  private int runConcurrentNamespaceOperation(ExecutorService pool, int numTasks,
      ConcurrentOperation operation) throws ExecutionException, InterruptedException {
    return runConcurrentOperation(pool, numTasks, operation, NamespaceExistsException.class);
  }

  private int runConcurrentOperation(ExecutorService pool, int numTasks,
      ConcurrentOperation operation, Class<? extends Exception> expectedExceptionType)
      throws ExecutionException, InterruptedException {
    CountDownLatch startSignal = new CountDownLatch(numTasks);
    List<Future<Boolean>> futures = new ArrayList<>(numTasks);

    for (int i = 0; i < numTasks; i++) {
      final int index = i;
      futures.add(pool.submit(() -> {
        try {
          startSignal.countDown();
          startSignal.await();
          return operation.execute(index);
        } catch (Exception e) {
          if (expectedExceptionType.isInstance(e)) {
            return false;
          }
          throw new RuntimeException(e);
        }
      }));
    }

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

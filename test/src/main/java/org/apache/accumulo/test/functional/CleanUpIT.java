/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Ensures that all threads spawned for ZooKeeper and Thrift connectivity are reaped after calling
 * CleanUp.shutdown().
 *
 * Because this is destructive across the current context classloader, the normal teardown methods
 * will fail (because they attempt to create a Connector). Until the ZooKeeperInstance and Connector
 * are self-contained WRT resource management, we can't leverage the AccumuloClusterBase.
 */
@Timeout(30)
public class CleanUpIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(CleanUpIT.class);

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testerTimeourer() throws InterruptedException {
    Thread.sleep(10_000L);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void run() throws Exception {

    // CleanUp for Connectors will not work if there are active AccumuloClients
    assertEquals(0, SingletonManager.getReservationCount());

    // CleanUp was created to clean up after connectors. This test intentionally creates a connector
    // instead of an AccumuloClient
    org.apache.accumulo.core.client.Connector conn =
        new org.apache.accumulo.core.client.ZooKeeperInstance(getCluster().getInstanceName(),
            getCluster().getZooKeepers()).getConnector(getPrincipal(), getToken());

    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "cq1", 1, "5");

    bw.addMutation(m1);

    bw.flush();

    try (Scanner scanner = conn.createScanner(tableName, new Authorizations())) {

      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        count++;
        if (!entry.getValue().toString().equals("5")) {
          fail("Unexpected value " + entry.getValue());
        }
      }

      assertEquals(1, count, "Unexpected count");

      int threadCount = countThreads();
      if (threadCount < 2) {
        printThreadNames();
        fail("Not seeing expected threads. Saw " + threadCount);
      }

      org.apache.accumulo.core.util.CleanUp.shutdownNow(conn);

      Mutation m2 = new Mutation("r2");
      m2.put("cf1", "cq1", 1, "6");

      bw.addMutation(m1);
      assertThrows(MutationsRejectedException.class, bw::flush);

      // expect this to fail also, want to clean up batch writer threads
      assertThrows(MutationsRejectedException.class, bw::close);

      assertThrows(IllegalStateException.class, () -> Iterables.size(scanner));

      threadCount = countThreads();
      if (threadCount > 0) {
        printThreadNames();
        fail("Threads did not go away. Saw " + threadCount);
      }
    }
  }

  private void printThreadNames() {
    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    Exception e = new Exception();
    for (Thread thread : threads) {
      e.setStackTrace(thread.getStackTrace());
      log.info("thread name: " + thread.getName(), e);
    }
  }

  /**
   * count threads that should be cleaned up
   *
   */
  private int countThreads() {
    int count = 0;
    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    for (Thread thread : threads) {

      if (thread.getName().toLowerCase().contains("sendthread")
          || thread.getName().toLowerCase().contains("eventthread"))
        count++;

      if (thread.getName().toLowerCase().contains("thrift")
          && thread.getName().toLowerCase().contains("pool"))
        count++;
    }

    return count;
  }
}

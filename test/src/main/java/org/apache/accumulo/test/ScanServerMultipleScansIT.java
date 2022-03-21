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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ScanServerMultipleScansIT extends SharedMiniClusterBase {

  private static class ScanServerITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumScanServers(1);
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");
    }
  }

  private static final int NUM_SCANS = 4;

  @BeforeAll
  public static void start() throws Exception {
    ScanServerITConfiguration c = new ScanServerITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
    SharedMiniClusterBase.getCluster().getClusterControl().start(ServerType.SCAN_SERVER,
        "localhost");

    String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    String scanServerRoot = zooRoot + Constants.ZSSERVERS;

    while (zrw.getChildren(scanServerRoot).size() == 0) {
      Thread.sleep(500);
    }
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testMutipleScansSameTablet() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      Thread[] threads = new Thread[NUM_SCANS];
      for (int i = 0; i < NUM_SCANS; i++) {
        Thread t = new Thread(() -> {
          try {
            latch.await();
          } catch (InterruptedException e1) {
            fail("InterruptedException waiting for latch");
          }
          try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
            scanner.setRange(new Range());
            scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
            int count = 0;
            for (@SuppressWarnings("unused")
            Entry<Key,Value> entry : scanner) {
              count++;
            }
            assertEquals(100, count);
          } catch (TableNotFoundException e) {
            fail("Table not found");
          }
        });
        t.start();
        threads[i] = t;
      }
      latch.countDown();
      for (int i = 0; i < NUM_SCANS; i++) {
        threads[i].join();
      }
    }
  }

  @Test
  public void testSingleScanDifferentTablets() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      SortedSet<Text> splitPoints = new TreeSet<>();
      splitPoints.add(new Text("row_0000000002\\0"));
      splitPoints.add(new Text("row_0000000005\\0"));
      splitPoints.add(new Text("row_0000000008\\0"));
      client.tableOperations().addSplits(tableName, splitPoints);

      @SuppressWarnings("deprecation")
      Collection<Text> splits = client.tableOperations().getSplits(tableName);
      assertEquals(3, splits.size());

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        int count = 0;
        for (@SuppressWarnings("unused")
        Entry<Key,Value> entry : scanner) {
          count++;
        }
        assertEquals(100, count);
      }
    }
  }

  @Test
  public void testMultipleScansDifferentTablets() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      SortedSet<Text> splitPoints = new TreeSet<>();
      splitPoints.add(new Text("row_0000000002\\0"));
      splitPoints.add(new Text("row_0000000005\\0"));
      splitPoints.add(new Text("row_0000000008\\0"));
      client.tableOperations().addSplits(tableName, splitPoints);

      @SuppressWarnings("deprecation")
      Collection<Text> splits = client.tableOperations().getSplits(tableName);
      assertEquals(3, splits.size());
      System.out.println(splits);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      final AtomicInteger counter = new AtomicInteger(0);

      Thread[] threads = new Thread[NUM_SCANS];
      for (int i = 0; i < NUM_SCANS; i++) {
        final int threadNum = i;
        Thread t = new Thread(() -> {
          try {
            latch.await();
          } catch (InterruptedException e1) {
            fail("InterruptedException waiting for latch");
          }
          try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
            switch (threadNum) {
              case 0:
                scanner.setRange(new Range("row_0000000000", "row_0000000002"));
                break;
              case 1:
                scanner.setRange(new Range("row_0000000003", "row_0000000005"));
                break;
              case 2:
                scanner.setRange(new Range("row_0000000006", "row_0000000008"));
                break;
              case 3:
                scanner.setRange(new Range("row_0000000009"));
                break;
              default:
                fail("Invalid threadNum");
            }
            scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
            for (@SuppressWarnings("unused")
            Entry<Key,Value> entry : scanner) {
              counter.incrementAndGet();
            }
          } catch (TableNotFoundException e) {
            fail("Table not found");
          }
        });
        t.start();
        threads[i] = t;
      }
      latch.countDown();
      for (int i = 0; i < NUM_SCANS; i++) {
        threads[i].join();
      }
      assertEquals(100, counter.get());
    }
  }

  @Test
  public void testMutipleBatchScansSameTablet() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      Thread[] threads = new Thread[NUM_SCANS];
      for (int i = 0; i < NUM_SCANS; i++) {
        Thread t = new Thread(() -> {
          try {
            latch.await();
          } catch (InterruptedException e1) {
            fail("InterruptedException waiting for latch");
          }
          try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
            scanner.setRanges(Collections.singletonList(new Range()));
            scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
            int count = 0;
            for (@SuppressWarnings("unused")
            Entry<Key,Value> entry : scanner) {
              count++;
            }
            assertEquals(100, count);
          } catch (TableNotFoundException e) {
            fail("Table not found");
          }
        });
        t.start();
        threads[i] = t;
      }
      latch.countDown();
      for (int i = 0; i < NUM_SCANS; i++) {
        threads[i].join();
      }
    }

  }

  @Test
  public void testSingleBatchScanDifferentTablets() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      SortedSet<Text> splitPoints = new TreeSet<>();
      splitPoints.add(new Text("row_0000000002\\0"));
      splitPoints.add(new Text("row_0000000005\\0"));
      splitPoints.add(new Text("row_0000000008\\0"));
      client.tableOperations().addSplits(tableName, splitPoints);

      @SuppressWarnings("deprecation")
      Collection<Text> splits = client.tableOperations().getSplits(tableName);
      assertEquals(3, splits.size());

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      try (BatchScanner scanner =
          client.createBatchScanner(tableName, Authorizations.EMPTY, NUM_SCANS)) {
        scanner.setRanges(Collections.singletonList(new Range()));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        int count = 0;
        for (@SuppressWarnings("unused")
        Entry<Key,Value> entry : scanner) {
          count++;
        }
        assertEquals(100, count);
      }
    }
  }

  @Test
  public void testMultipleBatchScansDifferentTablets() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      SortedSet<Text> splitPoints = new TreeSet<>();
      splitPoints.add(new Text("row_0000000002\\0"));
      splitPoints.add(new Text("row_0000000005\\0"));
      splitPoints.add(new Text("row_0000000008\\0"));
      client.tableOperations().addSplits(tableName, splitPoints);

      @SuppressWarnings("deprecation")
      Collection<Text> splits = client.tableOperations().getSplits(tableName);
      assertEquals(3, splits.size());
      System.out.println(splits);

      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      final CountDownLatch latch = new CountDownLatch(1);

      final AtomicInteger counter = new AtomicInteger(0);

      Thread[] threads = new Thread[NUM_SCANS];
      for (int i = 0; i < NUM_SCANS; i++) {
        final int threadNum = i;
        Thread t = new Thread(() -> {
          try {
            latch.await();
          } catch (InterruptedException e1) {
            fail("InterruptedException waiting for latch");
          }
          try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
            switch (threadNum) {
              case 0:
                scanner.setRanges(
                    Collections.singletonList(new Range("row_0000000000", "row_0000000002")));
                break;
              case 1:
                scanner.setRanges(
                    Collections.singletonList(new Range("row_0000000003", "row_0000000005")));
                break;
              case 2:
                scanner.setRanges(
                    Collections.singletonList(new Range("row_0000000006", "row_0000000008")));
                break;
              case 3:
                scanner.setRanges(Collections.singletonList(new Range("row_0000000009")));
                break;
              default:
                fail("Invalid threadNum");
            }
            scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
            for (@SuppressWarnings("unused")
            Entry<Key,Value> entry : scanner) {
              counter.incrementAndGet();
            }
          } catch (TableNotFoundException e) {
            fail("Table not found");
          }
        });
        t.start();
        threads[i] = t;
      }
      latch.countDown();
      for (int i = 0; i < NUM_SCANS; i++) {
        threads[i].join();
      }
      assertEquals(100, counter.get());
    }

  }
}

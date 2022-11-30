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

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public class BatchWriterFlushIT extends AccumuloClusterHarness {

  private static final int NUM_TO_FLUSH = 100000;
  private static final int NUM_THREADS = 3;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(90);
  }

  @Test
  public void run() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = getUniqueNames(2);
      String bwft = tableNames[0];
      c.tableOperations().create(bwft);
      String bwlt = tableNames[1];
      c.tableOperations().create(bwlt);
      runFlushTest(c, bwft);
      runLatencyTest(c, bwlt);
    }
  }

  private void runLatencyTest(AccumuloClient client, String tableName) throws Exception {
    // should automatically flush after 2 seconds
    try (
        BatchWriter bw = client.createBatchWriter(tableName,
            new BatchWriterConfig().setMaxLatency(1000, TimeUnit.MILLISECONDS));
        Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {

      Mutation m = new Mutation(new Text(String.format("r_%10d", 1)));
      m.put("cf", "cq", "1");
      bw.addMutation(m);

      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

      int count = Iterators.size(scanner.iterator());

      if (count != 0) {
        throw new Exception("Flushed too soon");
      }

      sleepUninterruptibly(1500, TimeUnit.MILLISECONDS);

      count = Iterators.size(scanner.iterator());

      if (count != 1) {
        throw new Exception("Did not flush");
      }
    }
  }

  private void runFlushTest(AccumuloClient client, String tableName) throws Exception {
    BatchWriter bw = client.createBatchWriter(tableName);
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {

      for (int i = 0; i < 4; i++) {
        for (int j = 0; j < NUM_TO_FLUSH; j++) {
          int row = i * NUM_TO_FLUSH + j;

          Mutation m = new Mutation(new Text(String.format("r_%10d", row)));
          m.put("cf", "cq", "" + row);
          bw.addMutation(m);
        }

        bw.flush();

        // do a few random lookups into the data just flushed

        for (int k = 0; k < 10; k++) {
          int rowToLookup = random.nextInt(NUM_TO_FLUSH) + i * NUM_TO_FLUSH;

          scanner.setRange(new Range(new Text(String.format("r_%10d", rowToLookup))));

          Iterator<Entry<Key,Value>> iter = scanner.iterator();

          if (!iter.hasNext()) {
            throw new Exception(" row " + rowToLookup + " not found after flush");
          }

          Entry<Key,Value> entry = iter.next();

          if (iter.hasNext()) {
            throw new Exception("Scanner returned too much");
          }

          verifyEntry(rowToLookup, entry);
        }

        // scan all data just flushed
        scanner.setRange(new Range(new Text(String.format("r_%10d", i * NUM_TO_FLUSH)), true,
            new Text(String.format("r_%10d", (i + 1) * NUM_TO_FLUSH)), false));
        Iterator<Entry<Key,Value>> iter = scanner.iterator();

        for (int j = 0; j < NUM_TO_FLUSH; j++) {
          int row = i * NUM_TO_FLUSH + j;

          if (!iter.hasNext()) {
            throw new Exception("Scan stopped prematurely at " + row);
          }

          Entry<Key,Value> entry = iter.next();

          verifyEntry(row, entry);
        }

        if (iter.hasNext()) {
          throw new Exception("Scanner returned too much");
        }

      }

      bw.close();

      // test adding a mutation to a closed batch writer
      boolean caught = false;
      try {
        bw.addMutation(new Mutation(new Text("foobar")));
      } catch (IllegalStateException ise) {
        caught = true;
      }

      if (!caught) {
        throw new Exception("Adding to closed batch writer did not fail");
      }
    }
  }

  @Test
  public void runMultiThreadedBinningTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] tableNames = getUniqueNames(1);
      String tableName = tableNames[0];
      c.tableOperations().create(tableName);
      for (int x = 0; x < NUM_THREADS; x++) {
        c.tableOperations().addSplits(tableName,
            new TreeSet<>(Collections.singleton(new Text(Integer.toString(x * NUM_TO_FLUSH)))));
      }
      c.instanceOperations().waitForBalance();

      // Logger.getLogger(TabletServerBatchWriter.class).setLevel(Level.TRACE);
      final List<Set<Mutation>> allMuts = new LinkedList<>();
      List<Mutation> data = new ArrayList<>();
      for (int i = 0; i < NUM_THREADS; i++) {
        for (int j = 0; j < NUM_TO_FLUSH; j++) {
          int row = i * NUM_TO_FLUSH + j;
          Mutation m = new Mutation(new Text(String.format("%10d", row)));
          m.put("cf" + i, "cq", "" + row);
          data.add(m);
        }
      }
      assertEquals(NUM_THREADS * NUM_TO_FLUSH, data.size());
      Collections.shuffle(data);
      for (int n = 0; n < (NUM_THREADS * NUM_TO_FLUSH); n += NUM_TO_FLUSH) {
        Set<Mutation> muts = new HashSet<>(data.subList(n, n + NUM_TO_FLUSH));
        allMuts.add(muts);
      }

      ThreadPoolExecutor threads = ThreadPools.getServerThreadPools()
          .createFixedThreadPool(NUM_THREADS, "ClientThreads", false);
      threads.allowCoreThreadTimeOut(false);
      threads.prestartAllCoreThreads();

      BatchWriterConfig cfg = new BatchWriterConfig();
      cfg.setMaxLatency(10, TimeUnit.SECONDS);
      cfg.setMaxMemory(1 * 1024 * 1024);
      cfg.setMaxWriteThreads(NUM_THREADS);
      final BatchWriter bw = c.createBatchWriter(tableName, cfg);

      for (int k = 0; k < NUM_THREADS; k++) {
        final int idx = k;
        threads.execute(new Runnable() {
          @Override
          public void run() {
            try {
              bw.addMutations(allMuts.get(idx));
              bw.flush();
            } catch (MutationsRejectedException e) {
              fail("Error adding mutations to batch writer");
            }
          }
        });
      }
      threads.shutdown();
      threads.awaitTermination(3, TimeUnit.MINUTES);
      bw.close();
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        for (Entry<Key,Value> e : scanner) {
          Mutation m = new Mutation(e.getKey().getRow());
          m.put(e.getKey().getColumnFamily(), e.getKey().getColumnQualifier(), e.getValue());
          boolean found = false;
          for (int l = 0; l < NUM_THREADS; l++) {
            if (allMuts.get(l).contains(m)) {
              found = true;
              allMuts.get(l).remove(m);
              break;
            }
          }
          assertTrue(found, "Mutation not found: " + m);
        }

        for (int m = 0; m < NUM_THREADS; m++) {
          assertEquals(0, allMuts.get(m).size());
        }
      }
    }
  }

  private void verifyEntry(int row, Entry<Key,Value> entry) throws Exception {
    if (!entry.getKey().getRow().toString().equals(String.format("r_%10d", row))) {
      throw new Exception("Unexpected key returned, expected " + row + " got " + entry.getKey());
    }

    if (!entry.getValue().toString().equals("" + row)) {
      throw new Exception("Unexpected value, expected " + row + " got " + entry.getValue());
    }
  }

}

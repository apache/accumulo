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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ConcurrentDeleteTableIT extends AccumuloClusterHarness {

  private final NewTableConfiguration ntc = new NewTableConfiguration().withSplits(createSplits());
  private final int NUM_TABLES = 2;
  private final int NUM_SPLITS = 1_000;
  private final int NUM_MUTATIONS = 1_000;

  @Override
  protected int defaultTimeoutSeconds() {
    return 7 * 60;
  }

  @Test
  public void testConcurrentDeleteTablesOps() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tables = getUniqueNames(NUM_TABLES);

      int numDeleteOps = 20;

      ExecutorService es = Executors.newFixedThreadPool(numDeleteOps);

      int count = 0;
      for (final String table : tables) {
        c.tableOperations().create(table, ntc);
        writeData(c, table);
        // flush last table
        if (count == tables.length - 1) {
          c.tableOperations().flush(table, null, null, true);
        }
        count++;

        final CountDownLatch cdl = new CountDownLatch(numDeleteOps);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < numDeleteOps; i++) {
          futures.add(es.submit(() -> {
            try {
              cdl.countDown();
              cdl.await();
              c.tableOperations().delete(table);
            } catch (TableNotFoundException e) {
              // expected
            } catch (InterruptedException | AccumuloException | AccumuloSecurityException e) {
              throw new RuntimeException(e);
            }
          }));
        }

        assertEquals(numDeleteOps, futures.size());

        for (Future<?> future : futures) {
          future.get();
        }

        try {
          c.createScanner(table, Authorizations.EMPTY);
          fail("Expected table " + table + " to be gone.");
        } catch (TableNotFoundException tnfe) {
          // expected
        }

        FunctionalTestUtils.assertNoDanglingFateLocks((ClientContext) c, getCluster());
      }

      es.shutdown();
    }
  }

  @Test
  public void testConcurrentFateOpsWithDelete() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String[] tables = getUniqueNames(NUM_TABLES);

      int numOperations = 8;

      ExecutorService es = Executors.newFixedThreadPool(numOperations);

      int count = 0;
      for (final String table : tables) {
        c.tableOperations().create(table, ntc);
        writeData(c, table);
        // flush last table
        if (count == tables.length - 1) {
          c.tableOperations().flush(table, null, null, true);
        }
        count++;

        // increment this for each test
        final CountDownLatch cdl = new CountDownLatch(numOperations);

        List<Future<?>> futures = new ArrayList<>();

        futures.add(es.submit(() -> {
          try {
            cdl.countDown();
            cdl.await();
            c.tableOperations().delete(table);
          } catch (TableNotFoundException | TableOfflineException e) {
            // expected
          } catch (InterruptedException | AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
          }
        }));

        futures.add(es.submit(new DelayedTableOp(cdl) {
          @Override
          protected void doTableOp() throws Exception {
            c.tableOperations().compact(table, new CompactionConfig());
          }
        }));

        futures.add(es.submit(new DelayedTableOp(cdl) {
          @Override
          protected void doTableOp() throws Exception {
            c.tableOperations().merge(table, null, null);
          }
        }));

        futures.add(es.submit(new DelayedTableOp(cdl) {
          @Override
          protected void doTableOp() throws Exception {
            Map<String,String> m = Collections.emptyMap();
            Set<String> s = Collections.emptySet();
            c.tableOperations().clone(table, table + "_clone", true, m, s);
          }
        }));

        futures.add(es.submit(new DelayedTableOp(cdl) {
          @Override
          protected void doTableOp() throws Exception {
            c.tableOperations().deleteRows(table, null, null);
          }
        }));

        futures.add(es.submit(new DelayedTableOp(cdl) {
          @Override
          protected void doTableOp() throws Exception {
            c.tableOperations().cancelCompaction(table);
          }
        }));

        futures.add(es.submit(new DelayedTableOp(cdl) {
          @Override
          protected void doTableOp() throws Exception {
            c.tableOperations().rename(table, table + "_renamed");
          }
        }));

        futures.add(es.submit(new DelayedTableOp(cdl) {
          @Override
          protected void doTableOp() throws Exception {
            c.tableOperations().offline(table);
          }
        }));

        assertEquals(numOperations, futures.size());

        for (Future<?> future : futures) {
          future.get();
        }

        try {
          c.createScanner(table, Authorizations.EMPTY);
          fail("Expected table " + table + " to be gone.");
        } catch (TableNotFoundException tnfe) {
          // expected
        }

        FunctionalTestUtils.assertNoDanglingFateLocks((ClientContext) c, getCluster());
      }

      es.shutdown();
    }
  }

  private abstract static class DelayedTableOp implements Runnable {
    private final CountDownLatch cdl;

    DelayedTableOp(CountDownLatch cdl) {
      this.cdl = cdl;
    }

    @Override
    public void run() {
      try {
        cdl.countDown();
        cdl.await();
        Thread.sleep(10);
        doTableOp();
      } catch (TableNotFoundException | TableOfflineException e) {
        // expected
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    protected abstract void doTableOp() throws Exception;
  }

  private TreeSet<Text> createSplits() {
    TreeSet<Text> splits = new TreeSet<>();

    for (int i = 0; i < NUM_SPLITS; i++) {
      Text split = new Text(String.format("%09x", i * 100_000));
      splits.add(split);
    }
    return splits;
  }

  private void writeData(AccumuloClient c, String table)
      throws TableNotFoundException, MutationsRejectedException {
    try (BatchWriter bw = c.createBatchWriter(table)) {
      Random rand = new SecureRandom();
      for (int i = 0; i < NUM_MUTATIONS; i++) {
        Mutation m = new Mutation(String.format("%09x", rand.nextInt(100_000 * 1_000)));
        m.put("m", "order", "" + i);
        bw.addMutation(m);
      }
    }
  }
}

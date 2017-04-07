/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.test.functional;

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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentDeleteTableIT extends AccumuloClusterHarness {

  @Test
  public void testConcurrentDeleteTablesOps() throws Exception {
    final Connector c = getConnector();
    String[] tables = getUniqueNames(2);

    TreeSet<Text> splits = createSplits();

    ExecutorService es = Executors.newFixedThreadPool(20);

    int count = 0;
    for (final String table : tables) {
      c.tableOperations().create(table);
      c.tableOperations().addSplits(table, splits);
      writeData(c, table);
      if (count == 1) {
        c.tableOperations().flush(table, null, null, true);
      }
      count++;

      int numDeleteOps = 20;
      final CountDownLatch cdl = new CountDownLatch(numDeleteOps);

      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < numDeleteOps; i++) {
        Future<?> future = es.submit(new Runnable() {

          @Override
          public void run() {
            try {
              cdl.countDown();
              cdl.await();
              c.tableOperations().delete(table);
            } catch (TableNotFoundException e) {
              // expected
            } catch (InterruptedException | AccumuloException | AccumuloSecurityException e) {
              throw new RuntimeException(e);
            }
          }
        });

        futures.add(future);
      }

      for (Future<?> future : futures) {
        future.get();
      }

      try {
        c.createScanner(table, Authorizations.EMPTY);
        Assert.fail("Expected table " + table + " to be gone.");
      } catch (TableNotFoundException tnfe) {
        // expected
      }

      FunctionalTestUtils.assertNoDanglingFateLocks(getConnector().getInstance(), getCluster());
    }

    es.shutdown();
  }

  private TreeSet<Text> createSplits() {
    TreeSet<Text> splits = new TreeSet<>();

    for (int i = 0; i < 1000; i++) {
      Text split = new Text(String.format("%09x", i * 100000));
      splits.add(split);
    }
    return splits;
  }

  private static abstract class DelayedTableOp implements Runnable {
    private CountDownLatch cdl;

    DelayedTableOp(CountDownLatch cdl) {
      this.cdl = cdl;
    }

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

  @Test
  public void testConcurrentFateOpsWithDelete() throws Exception {
    final Connector c = getConnector();
    String[] tables = getUniqueNames(2);

    TreeSet<Text> splits = createSplits();

    int numOperations = 8;

    ExecutorService es = Executors.newFixedThreadPool(numOperations);

    int count = 0;
    for (final String table : tables) {
      c.tableOperations().create(table);
      c.tableOperations().addSplits(table, splits);
      writeData(c, table);
      if (count == 1) {
        c.tableOperations().flush(table, null, null, true);
      }
      count++;

      // increment this for each test
      final CountDownLatch cdl = new CountDownLatch(numOperations);

      List<Future<?>> futures = new ArrayList<>();

      futures.add(es.submit(new Runnable() {
        @Override
        public void run() {
          try {
            cdl.countDown();
            cdl.await();
            c.tableOperations().delete(table);
          } catch (TableNotFoundException | TableOfflineException e) {
            // expected
          } catch (InterruptedException | AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
          }
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

      Assert.assertEquals(numOperations, futures.size());

      for (Future<?> future : futures) {
        future.get();
      }

      try {
        c.createScanner(table, Authorizations.EMPTY);
        Assert.fail("Expected table " + table + " to be gone.");
      } catch (TableNotFoundException tnfe) {
        // expected
      }

      FunctionalTestUtils.assertNoDanglingFateLocks(getConnector().getInstance(), getCluster());
    }

    es.shutdown();
  }

  private void writeData(Connector c, String table) throws TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig());
    try {
      Random rand = new Random();
      for (int i = 0; i < 1000; i++) {
        Mutation m = new Mutation(String.format("%09x", rand.nextInt(100000 * 1000)));
        m.put("m", "order", "" + i);
        bw.addMutation(m);
      }
    } finally {
      bw.close();
    }
  }
}

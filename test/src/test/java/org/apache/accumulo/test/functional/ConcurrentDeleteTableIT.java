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
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.AdminUtil.FateStatus;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentDeleteTableIT extends AccumuloClusterIT {

  private static final String SCHEME = "digest";

  private static final String USER = "accumulo";

  @Test
  public void testConcurrentDeleteTablesOps() throws Exception {
    final Connector c = getConnector();
    String[] tables = getUniqueNames(2);

    TreeSet<Text> splits = new TreeSet<>();

    for (int i = 0; i < 1000; i++) {
      Text split = new Text(String.format("%09x", i * 100000));
      splits.add(split);
    }

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

      final CountDownLatch cdl = new CountDownLatch(20);

      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 20; i++) {
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

      FateStatus fateStatus = getFateStatus();

      // ensure there are no dangling locks... before ACCUMULO-4575 was fixed concurrent delete tables could fail and leave dangling locks.
      Assert.assertEquals(0, fateStatus.getDanglingHeldLocks().size());
      Assert.assertEquals(0, fateStatus.getDanglingWaitingLocks().size());
    }

    es.shutdown();
  }

  private FateStatus getFateStatus() throws KeeperException, InterruptedException {
    Instance instance = getConnector().getInstance();
    AdminUtil<String> admin = new AdminUtil<>(false);
    String secret = "DONTTELL";
    IZooReaderWriter zk = new ZooReaderWriter(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), SCHEME, (USER + ":" + secret).getBytes());
    ZooStore<String> zs = new ZooStore<String>(ZooUtil.getRoot(instance) + Constants.ZFATE, zk);
    FateStatus fateStatus = admin.getStatus(zs, zk, ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS, null, null);
    return fateStatus;
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

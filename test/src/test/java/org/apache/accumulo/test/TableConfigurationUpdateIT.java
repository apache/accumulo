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
package org.apache.accumulo.test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TableConfigurationUpdateIT extends AccumuloClusterIT {
  private static final Logger log = Logger.getLogger(TableConfigurationUpdateIT.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void test() throws Exception {
    Connector conn = getConnector();
    Instance inst = conn.getInstance();

    String table = getUniqueNames(1)[0];
    conn.tableOperations().create(table);

    final NamespaceConfiguration defaultConf = new NamespaceConfiguration(Namespaces.DEFAULT_NAMESPACE_ID, inst,
        AccumuloConfiguration.getDefaultConfiguration());

    // Cache invalidates 25% of the time
    int randomMax = 4;
    // Number of threads
    int numThreads = 2;
    // Number of iterations per thread
    int iterations = 100000;
    AccumuloConfiguration tableConf = new TableConfiguration(inst.getInstanceID(), inst, table, defaultConf);

    long start = System.currentTimeMillis();
    ExecutorService svc = Executors.newFixedThreadPool(numThreads);
    CountDownLatch countDown = new CountDownLatch(numThreads);
    ArrayList<Future<Exception>> futures = new ArrayList<Future<Exception>>(numThreads);

    for (int i = 0; i < numThreads; i++) {
      futures.add(svc.submit(new TableConfRunner(randomMax, iterations, tableConf, countDown)));
    }

    svc.shutdown();
    Assert.assertTrue(svc.awaitTermination(60, TimeUnit.MINUTES));

    for (Future<Exception> fut : futures) {
      Exception e = fut.get();
      if (null != e) {
        Assert.fail("Thread failed with exception " + e);
      }
    }

    long end = System.currentTimeMillis();
    log.debug(tableConf + " with " + iterations + " iterations and " + numThreads + " threads and cache invalidates " + ((1. / randomMax) * 100.) + "% took "
        + (end - start) / 1000 + " second(s)");
  }

  public static class TableConfRunner implements Callable<Exception> {
    private static final Property prop = Property.TABLE_SPLIT_THRESHOLD;
    private AccumuloConfiguration tableConf;
    private CountDownLatch countDown;
    private int iterations, randMax;

    public TableConfRunner(int randMax, int iterations, AccumuloConfiguration tableConf, CountDownLatch countDown) {
      this.randMax = randMax;
      this.iterations = iterations;
      this.tableConf = tableConf;
      this.countDown = countDown;
    }

    @Override
    public Exception call() {
      Random r = new Random();
      countDown.countDown();
      try {
        countDown.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return e;
      }

      String t = Thread.currentThread().getName() + " ";
      try {
        for (int i = 0; i < iterations; i++) {
          // if (i % 10000 == 0) {
          // log.info(t + " " + i);
          // }
          int choice = r.nextInt(randMax);
          if (choice < 1) {
            tableConf.invalidateCache();
          } else {
            tableConf.get(prop);
          }
        }
      } catch (Exception e) {
        log.error(t, e);
        return e;
      }

      return null;
    }

  }

}

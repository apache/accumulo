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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ZombieScanIT extends ConfigurableMacBase {

  private static TestStatsDSink sink;

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);

    // Make sessions time out much more quickly. This will cause a session to be classified as a
    // zombie scan much sooner.
    cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "6s");
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.setProperty("general.custom.metrics.opts.logging.step", "1s");
    String clazzList = LoggingMeterRegistryFactory.class.getName() + ","
        + TestStatsDRegistryFactory.class.getName();
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, clazzList);
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
    cfg.setNumTservers(1);
  }

  /**
   * An iterator that should get stuck forever when used
   */
  public static class ZombieIterator extends WrappingIterator {
    @Override
    public boolean hasTop() {
      // must call super.hasTop() before blocking as that will run accumulo code to setup iterator
      boolean ht = super.hasTop();
      Semaphore semaphore = new Semaphore(10);
      semaphore.acquireUninterruptibly(5);
      // this should block forever
      semaphore.acquireUninterruptibly(6);
      return ht;
    }
  }

  /**
   * An iterator that should get stuck but can be interrupted
   */
  public static class StuckIterator extends WrappingIterator {
    @Override
    public boolean hasTop() {
      try {
        // must call super.hasTop() before blocking as that will run accumulo code to setup iterator
        boolean ht = super.hasTop();
        Semaphore semaphore = new Semaphore(10);
        semaphore.acquire(5);
        // this should block forever
        semaphore.acquire(6);
        return ht;
      }catch (InterruptedException ie){
        throw new IllegalStateException(ie);
      }
    }
  }

  /**
   * Create some zombie scans and ensure metrics for them show up.
   */
  @Test
  public void testMetrics() throws Exception {

    Wait.waitFor(() -> { var zsmc = getZombieScansMetric(); return zsmc == -1 || zsmc == 0;});

    String table = getUniqueNames(1)[0];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      c.tableOperations().create(table);

      var executor = Executors.newCachedThreadPool();

      // start four stuck scans that should never return data
      List<Future<String>> futures = new ArrayList<>();
      for (var row : List.of("2", "4")) {
        // start a scan with an iterator that gets stuck and can not be interrupted
        futures.add(startStuckScan(c, table, executor, row, false));
        // start a scan with an iterator that gets stuck and can be interrupted
        futures.add(startStuckScan(c, table, executor, row, true));
      }

      // start four stuck scans, using a batch scanner, that should never return data
      for (var row : List.of("6", "8")) {
        // start a scan with an iterator that gets stuck and can not be interrupted
        futures.add(startStuckBatchScan(c, table, executor, row, false));
        // start a scan with an iterator that gets stuck and can be interrupted
        futures.add(startStuckBatchScan(c, table, executor, row, true));
      }

      // should eventually see the eight stuck scans running
      Wait.waitFor(() -> countScansForTable(table, c) == 8);

      // Cancel the scan threads. This will cause the sessions on the server side to timeout and
      // become inactive.  The stuck threads on the server side related to the timed out sessions will be interrupted.
      Wait.waitFor(() -> {
        futures.forEach(future -> future.cancel(true));
        return futures.stream().allMatch(Future::isDone);
      });

      // Four of the eight running scans should respond to thread interrupts and exit
      Wait.waitFor(() -> countScansForTable(table, c) == 4);

      Wait.waitFor(() -> getZombieScansMetric() == 4);

      assertEquals(4, countScansForTable(table, c));

      // start four more stuck scans with two that will ignore interrupts
      futures.clear();
      futures.add(startStuckScan(c, table, executor, "0", false));
      futures.add(startStuckScan(c, table, executor, "0", true));
      futures.add(startStuckBatchScan(c, table, executor, "99", false));
      futures.add(startStuckBatchScan(c, table, executor, "0", true));

      Wait.waitFor(() -> countScansForTable(table, c) == 8);

      // Cancel the client side scan threads.  Should cause the server side threads to be interrupted.
      Wait.waitFor(() -> {
        futures.forEach(future -> future.cancel(true));
        return futures.stream().allMatch(Future::isDone);
      });

      // Two of the stuck threads should respond to interrupts on the server side and exit.
      Wait.waitFor(() -> countScansForTable(table, c) == 6);

      Wait.waitFor(() -> getZombieScansMetric() == 6);

      assertEquals(6, countScansForTable(table, c));

      executor.shutdownNow();
    }

  }

  private Future<String> startStuckScan(AccumuloClient c, String table, ExecutorService executor,
                                        String row, boolean canInterrupt) {
    return executor.submit(() -> {
      try (var scanner = c.createScanner(table)) {
        String className;
        if(canInterrupt) {
          className = StuckIterator.class.getName();
        } else {
          className = ZombieIterator.class.getName();
        }
        IteratorSetting iter = new IteratorSetting(100, "Z",className);
        scanner.addScanIterator(iter);
        scanner.setRange(new Range(row));
        return scanner.stream().findFirst().map(e -> e.getKey().getRowData().toString())
            .orElse("none");
      }
    });
  }

  private Future<String> startStuckBatchScan(AccumuloClient c, String table,
                                             ExecutorService executor, String row, boolean canInterrupt) {
    return executor.submit(() -> {
      try (var scanner = c.createBatchScanner(table)) {
        String className;
        if(canInterrupt) {
          className = StuckIterator.class.getName();
        } else {
          className = ZombieIterator.class.getName();
        }

        IteratorSetting iter = new IteratorSetting(100, "Z", className);
        scanner.addScanIterator(iter);
        scanner.setRanges(List.of(new Range(row)));
        return scanner.stream().findFirst().map(e -> e.getKey().getRowData().toString())
            .orElse("none");
      }
    });
  }

  private int getZombieScansMetric() {
    return sink.getLines().stream().map(TestStatsDSink::parseStatsDMetric)
        .filter(metric -> metric.getName().equals(MetricsProducer.METRICS_SCAN_ZOMBIE_THREADS))
        .mapToInt(metric -> Integer.parseInt(metric.getValue())).max().orElse(-1);
  }

  private static long countScansForTable(String table, AccumuloClient client)
      throws Exception {
    var tservers = client.instanceOperations().getTabletServers();
    long count = 0;
    for (String tserver : tservers) {
      count += client.instanceOperations().getActiveScans(tserver).stream()
          .filter(activeScan -> activeScan.getTable().equals(table))
          .count();
    }
    return count;
  }

}

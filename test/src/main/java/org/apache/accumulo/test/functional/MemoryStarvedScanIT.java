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

import static org.apache.accumulo.core.metrics.MetricsProducer.METRICS_LOW_MEMORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MemoryStarvedScanIT extends SharedMiniClusterBase {

  public static class MemoryStarvedITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setNumTservers(1);
      cfg.setMemory(ServerType.TABLET_SERVER, 256, MemoryUnit.MEGABYTE);
      // Configure the LowMemoryDetector in the TabletServer
      // check on 1s intervals and set low mem condition if more than 80% of
      // the heap is used.
      cfg.setProperty(Property.GENERAL_LOW_MEM_DETECTOR_INTERVAL, "5s");
      cfg.setProperty(Property.GENERAL_LOW_MEM_DETECTOR_THRESHOLD,
          Double.toString(FREE_MEMORY_THRESHOLD));
      cfg.setProperty(Property.GENERAL_LOW_MEM_SCAN_PROTECTION, "true");
      // Tell the server processes to use a StatsDMeterRegistry that will be configured
      // to push all metrics to the sink we started.
      cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
      cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY,
          TestStatsDRegistryFactory.class.getName());
      Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
          TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
      cfg.setSystemProperties(sysProps);
    }
  }

  public static final double FREE_MEMORY_THRESHOLD = 0.20D;

  private static final DoubleAdder SCAN_START_DELAYED = new DoubleAdder();
  private static final DoubleAdder SCAN_RETURNED_EARLY = new DoubleAdder();
  private static final AtomicInteger LOW_MEM_DETECTED = new AtomicInteger(0);
  private static TestStatsDSink sink;
  private static Thread metricConsumer;

  @BeforeAll
  public static void start() throws Exception {
    sink = new TestStatsDSink();
    metricConsumer = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        List<String> statsDMetrics = sink.getLines();
        for (String line : statsDMetrics) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
          if (line.startsWith("accumulo")) {
            Metric metric = TestStatsDSink.parseStatsDMetric(line);
            if (MetricsProducer.METRICS_SCAN_PAUSED_FOR_MEM.equals(metric.getName())) {
              double val = Double.parseDouble(metric.getValue());
              SCAN_START_DELAYED.add(val);
            } else if (MetricsProducer.METRICS_SCAN_RETURN_FOR_MEM.equals(metric.getName())) {
              double val = Double.parseDouble(metric.getValue());
              SCAN_RETURNED_EARLY.add(val);
            } else if (metric.getName().equals(METRICS_LOW_MEMORY)) {
              String process = metric.getTags().get("process.name");
              if (process != null && process.contains("tserver")) {
                int val = Integer.parseInt(metric.getValue());
                LOW_MEM_DETECTED.set(val);
              }
            }
          }
        }
      }
    });
    metricConsumer.start();

    SharedMiniClusterBase.startMiniClusterWithConfig(new MemoryStarvedITConfiguration());
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
    sink.close();
    metricConsumer.interrupt();
    metricConsumer.join();
  }

  @BeforeEach
  public void beforeEach() {
    // Reset the client side counters
    SCAN_START_DELAYED.reset();
    SCAN_START_DELAYED.reset();
    LOW_MEM_DETECTED.set(0);
  }

  static void consumeServerMemory(Scanner scanner) {
    // This iterator will attempt to consume all free memory in the TabletServer
    scanner.addScanIterator(new IteratorSetting(11, MemoryConsumingIterator.class, Map.of()));
    scanner.setBatchSize(1);
    // Set the ReadaheadThreshold to a large number so that another background thread
    // that performs read-ahead of KV pairs is not started.
    scanner.setReadaheadThreshold(Long.MAX_VALUE);
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    // This should block until the GarbageCollectionLogger runs and notices that the
    // VM is low on memory.
    assertTrue(iter.hasNext());
  }

  private void consumeServerMemory(BatchScanner scanner) {
    // This iterator will attempt to consume all free memory in the TabletServer
    scanner.addScanIterator(new IteratorSetting(11, MemoryConsumingIterator.class, Map.of()));
    scanner.setRanges(Collections.singletonList(new Range()));
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    // This should block until the GarbageCollectionLogger runs and notices that the
    // VM is low on memory.
    assertTrue(iter.hasNext());
  }

  static void freeServerMemory(AccumuloClient client, String table) throws Exception {
    try (Scanner scanner = client.createScanner(table)) {
      scanner.addScanIterator(new IteratorSetting(11, MemoryFreeingIterator.class, Map.of()));
      @SuppressWarnings("unused")
      Iterator<Entry<Key,Value>> iter = scanner.iterator(); // init'ing the iterator should be
                                                            // enough to free the memory
    }
  }

  @Test
  public void testScanReturnsEarlyDueToLowMemory() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      TableOperations to = client.tableOperations();
      to.create(table);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);

      try (Scanner scanner = client.createScanner(table)) {
        double returned = SCAN_RETURNED_EARLY.doubleValue();
        double paused = SCAN_START_DELAYED.doubleValue();

        consumeServerMemory(scanner);

        // Wait for longer than the memory check interval
        Thread.sleep(6_000);

        // The metric that indicates a scan was returned early due to low memory should
        // have been incremented.
        assertTrue(SCAN_RETURNED_EARLY.doubleValue() > returned);
        assertTrue(SCAN_START_DELAYED.doubleValue() >= paused);
        freeServerMemory(client, table);
      } finally {
        to.delete(table);
      }
    }
  }

  @Test
  public void testScanPauses() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      TableOperations to = client.tableOperations();
      to.create(table);

      ReadWriteIT.ingest(client, 10, 3, 10, 0, table);

      try (Scanner dataConsumingScanner = client.createScanner(table);
          Scanner memoryConsumingScanner = client.createScanner(table)) {

        dataConsumingScanner.addScanIterator(
            new IteratorSetting(11, SlowIterator.class, Map.of("sleepTime", "500")));
        dataConsumingScanner.setBatchSize(1);
        dataConsumingScanner.setReadaheadThreshold(Long.MAX_VALUE);
        Iterator<Entry<Key,Value>> iter = dataConsumingScanner.iterator();
        AtomicInteger fetched = new AtomicInteger(0);
        Thread t = new Thread(() -> {
          int i = 0;
          while (iter.hasNext()) {
            iter.next();
            fetched.set(++i);
          }
        });

        memoryConsumingScanner
            .addScanIterator(new IteratorSetting(11, MemoryConsumingIterator.class, Map.of()));
        memoryConsumingScanner.setBatchSize(1);
        memoryConsumingScanner.setReadaheadThreshold(Long.MAX_VALUE);

        t.start();

        // Wait until the dataConsumingScanner has started fetching data
        int currentCount = fetched.get();
        while (currentCount == 0) {
          Thread.sleep(500);
          currentCount = fetched.get();
        }

        // This should block until the GarbageCollectionLogger runs and notices that the
        // VM is low on memory.
        Iterator<Entry<Key,Value>> consumingIter = memoryConsumingScanner.iterator();
        assertTrue(consumingIter.hasNext());

        // Confirm that some data was fetched by the memoryConsumingScanner
        currentCount = fetched.get();
        assertTrue(currentCount > 0 && currentCount < 100);

        // Grab the current metric counts, wait
        double returned = SCAN_RETURNED_EARLY.doubleValue();
        double paused = SCAN_START_DELAYED.doubleValue();
        Thread.sleep(1500);
        // One of two conditions could exist here:
        // The number of fetched rows equals the current count before the wait above
        // and the SCAN_START_DELAYED has been incremented OR the number of fetched
        // rows is one more than the current count and the SCAN_RETURNED_EARLY has
        // been incremented.
        assertTrue((currentCount == fetched.get() && SCAN_START_DELAYED.doubleValue() > paused)
            || (currentCount + 1 == fetched.get() && SCAN_RETURNED_EARLY.doubleValue() > returned));
        currentCount = fetched.get();

        // Perform the check again
        paused = SCAN_START_DELAYED.doubleValue();
        returned = SCAN_RETURNED_EARLY.doubleValue();
        Thread.sleep(1500);
        assertEquals(currentCount, fetched.get());

        assertTrue(Wait.waitFor(() -> (LOW_MEM_DETECTED.get() == 1), 20_000L, 1000L));

        // Free the memory which will allow the pausing scanner to continue
        freeServerMemory(client, table);

        t.join();
        assertEquals(30, fetched.get());
      } finally {
        to.delete(table);
      }
    }
  }

  @Test
  public void testBatchScanReturnsEarlyDueToLowMemory() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      TableOperations to = client.tableOperations();
      to.create(table);

      // check memory okay before starting
      assertEquals(0, LOW_MEM_DETECTED.get());

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);

      try (BatchScanner scanner = client.createBatchScanner(table,
          client.securityOperations().getUserAuthorizations(client.whoami()), 1)) {
        double returned = SCAN_RETURNED_EARLY.doubleValue();
        double paused = SCAN_START_DELAYED.doubleValue();

        consumeServerMemory(scanner);

        // Wait for longer than the memory check interval
        Thread.sleep(3000);

        // The metric that indicates a scan was returned early due to low memory should
        // have been incremented.
        assertTrue(
            Wait.waitFor(() -> (SCAN_RETURNED_EARLY.doubleValue() > returned), 20_000L, 1000L));
        assertTrue(
            Wait.waitFor(() -> (SCAN_START_DELAYED.doubleValue() >= paused), 20_000L, 1000L));
        assertTrue(Wait.waitFor(() -> (LOW_MEM_DETECTED.get() == 1), 20_000L, 1000L));

        freeServerMemory(client, table);
      } finally {
        to.delete(table);
      }
    }
  }

  @Test
  public void testBatchScanPauses() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      TableOperations to = client.tableOperations();
      to.create(table);

      // check memory okay before starting
      assertTrue(Wait.waitFor(() -> (LOW_MEM_DETECTED.get() == 0), 20_000L, 1000L));

      ReadWriteIT.ingest(client, 10, 3, 10, 0, table);

      try (BatchScanner dataConsumingScanner = client.createBatchScanner(table);
          Scanner memoryConsumingScanner = client.createScanner(table)) {

        dataConsumingScanner.addScanIterator(
            new IteratorSetting(11, SlowIterator.class, Map.of("sleepTime", "500")));
        dataConsumingScanner.setRanges(Collections.singletonList(new Range()));
        Iterator<Entry<Key,Value>> iter = dataConsumingScanner.iterator();
        AtomicInteger fetched = new AtomicInteger(0);
        Thread t = new Thread(() -> {
          int i = 0;
          while (iter.hasNext()) {
            iter.next();
            fetched.set(++i);
          }
        });

        memoryConsumingScanner
            .addScanIterator(new IteratorSetting(11, MemoryConsumingIterator.class, Map.of()));
        memoryConsumingScanner.setBatchSize(1);
        memoryConsumingScanner.setReadaheadThreshold(Long.MAX_VALUE);

        t.start();

        // Wait until the dataConsumingScanner has started fetching data
        int currentCount = fetched.get();
        while (currentCount == 0) {
          Thread.sleep(500);
          currentCount = fetched.get();
        }

        // This should block until the GarbageCollectionLogger runs and notices that the
        // VM is low on memory.
        Iterator<Entry<Key,Value>> consumingIter = memoryConsumingScanner.iterator();
        assertTrue(consumingIter.hasNext());

        // Confirm that some data was fetched by the dataConsumingScanner
        currentCount = fetched.get();
        assertTrue(currentCount > 0 && currentCount < 100);

        // Grab the current paused count, wait two seconds and then confirm that
        // the number of rows fetched by the memoryConsumingScanner has not increased
        // and that the scan delay counter has increased.
        final double returned = SCAN_RETURNED_EARLY.doubleValue();
        final double paused = SCAN_START_DELAYED.doubleValue();
        Thread.sleep(1500);
        assertEquals(currentCount, fetched.get());

        assertTrue(
            Wait.waitFor(() -> (SCAN_START_DELAYED.doubleValue() >= paused), 20_000L, 1000L));
        assertTrue(
            Wait.waitFor(() -> (SCAN_RETURNED_EARLY.doubleValue() >= returned), 20_000L, 1000L));
        assertTrue(Wait.waitFor(() -> (LOW_MEM_DETECTED.get() == 1), 20_000L, 1000L));

        // Perform the check again
        final double paused2 = SCAN_START_DELAYED.doubleValue();
        final double returned2 = SCAN_RETURNED_EARLY.doubleValue();
        Thread.sleep(1500);
        assertEquals(currentCount, fetched.get());

        assertTrue(
            Wait.waitFor(() -> (SCAN_START_DELAYED.doubleValue() >= paused2), 20_000L, 1000L));
        assertTrue(
            Wait.waitFor(() -> (SCAN_RETURNED_EARLY.doubleValue() == returned2), 20_000L, 1000L));
        assertTrue(Wait.waitFor(() -> (LOW_MEM_DETECTED.get() == 1), 20_000L, 1000L));

        // Free the memory which will allow the pausing scanner to continue
        freeServerMemory(client, table);

        t.join();
        assertEquals(30, fetched.get());
        // allow metic collection to cycle.
        Thread.sleep(3_000);
        assertTrue(Wait.waitFor(() -> (LOW_MEM_DETECTED.get() == 0), 20_000L, 1000L));

      } finally {
        to.delete(table);
      }
    }
  }

}

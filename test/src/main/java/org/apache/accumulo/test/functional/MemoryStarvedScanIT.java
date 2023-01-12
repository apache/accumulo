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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

public class MemoryStarvedScanIT extends ConfigurableMacBase {

  private static TestStatsDSink sink;
  private static Thread metricConsumer;
  private static final AtomicDouble SCAN_START_DELAYED = new AtomicDouble(0.0D);
  private static final AtomicDouble SCAN_RETURNED_EARLY = new AtomicDouble(0.0D);

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setMemory(ServerType.TABLET_SERVER, 256, MemoryUnit.MEGABYTE);
    // Configure the LowMemoryDetector in the TabletServer
    // check on 1s intervals and set low mem condition if more than 80% of
    // the heap is used.
    cfg.setProperty(Property.TSERV_LOW_MEM_DETECTOR_ACTIVE, "true");
    cfg.setProperty(Property.TSERV_LOW_MEM_DETECTOR_INTERVAL, "1s");
    cfg.setProperty(Property.TSERV_LOW_MEM_DETECTOR_THRESHOLD, "0.20");
    // Tell the server processes to use a StatsDMeterRegistry that will be configured
    // to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, TestStatsDRegistryFactory.class.getName());
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

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
              // record expected metric names as seen, along with the value seen
              Double val = Double.parseDouble(metric.getValue());
              if (val > SCAN_START_DELAYED.get()) {
                SCAN_START_DELAYED.set(val);
                LoggerFactory.getLogger(MemoryStarvedScanIT.class).info("PAUSED: {}", val);
              }
            } else if (MetricsProducer.METRICS_SCAN_RETURN_FOR_MEM.equals(metric.getName())) {
              Double val = Double.parseDouble(metric.getValue());
              if (val > SCAN_RETURNED_EARLY.get()) {
                SCAN_RETURNED_EARLY.set(val);
                LoggerFactory.getLogger(MemoryStarvedScanIT.class).info("RETURNED: {}", val);
              }
            }
          }
        }
      }
    });
    metricConsumer.start();
  }

  @AfterAll
  public static void stop() throws Exception {
    sink.close();
    metricConsumer.interrupt();
    metricConsumer.join();
  }

  private void freeServerMemory(String table) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();
        Scanner scanner = client.createScanner(table)) {
      scanner.addScanIterator(new IteratorSetting(11, MemoryFreeingIterator.class, Map.of()));
      scanner.iterator(); // init'ing the iterator should be enough to free the memory
    }
  }

  @Test
  public void testScanReturnsEarlyDueToLowMemory() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      TableOperations to = client.tableOperations();
      to.create(table);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);

      try (Scanner scanner = client.createScanner(table)) {
        Double returned = SCAN_RETURNED_EARLY.get();
        Double paused = SCAN_START_DELAYED.get();
        // This iterator will attempt to consume all free memory in the TabletServer
        IteratorSetting is = new IteratorSetting(11, MemoryConsumingIterator.class, Map.of());
        scanner.addScanIterator(is);
        scanner.setBatchSize(1);
        // Set the ReadaheadThreshold to a large number so that another background thread
        // that performs read-ahead of KV pairs is not started.
        scanner.setReadaheadThreshold(Integer.MAX_VALUE);
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        // This should block until the GarbageCollectionLogger runs and notices that the
        // VM is low on memory.
        assertTrue(iter.hasNext());
        // Wait for MetricsRegistry to get metrics
        Thread.sleep(3000);
        // The metric that indicates a scan was returned early due to low memory should
        // have been incremented.
        assertTrue(SCAN_RETURNED_EARLY.get() > returned);
        assertTrue(SCAN_START_DELAYED.get() == paused);
      } finally {
        client.tableOperations().delete(table);
      }
    }
  }

  @Test
  public void testBatchScanReturnsEarlyDueToLowMemory() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      TableOperations to = client.tableOperations();
      to.create(table);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);

      try (BatchScanner scanner = client.createBatchScanner(table,
          client.securityOperations().getUserAuthorizations(client.whoami()), 1)) {
        Double returned = SCAN_RETURNED_EARLY.get();
        Double paused = SCAN_START_DELAYED.get();
        // This iterator will attempt to consume all free memory in the TabletServer
        IteratorSetting is = new IteratorSetting(11, MemoryConsumingIterator.class, Map.of());
        scanner.addScanIterator(is);
        scanner.setRanges(Collections.singletonList(new Range()));
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        // This should block until the GarbageCollectionLogger runs and notices that the
        // VM is low on memory.
        assertTrue(iter.hasNext());
        // Wait for MetricsRegistry to get metrics
        Thread.sleep(3000);
        // The metric that indicates a scan was returned early due to low memory should
        // have been incremented.
        assertTrue(SCAN_RETURNED_EARLY.get() > returned);
        assertTrue(SCAN_START_DELAYED.get() > paused);
      } finally {
        client.tableOperations().delete(table);
      }
    }
  }

  @Test
  public void testScanPausedDueToLowMemory() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      TableOperations to = client.tableOperations();
      to.create(table);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);

      Scanner scanner = client.createScanner(table);
      Double returned = SCAN_RETURNED_EARLY.get();
      IteratorSetting is = new IteratorSetting(11, MemoryConsumingIterator.class, Map.of());
      scanner.addScanIterator(is);
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      assertTrue(iter.hasNext());
      Thread.sleep(3000);
      assertTrue(SCAN_RETURNED_EARLY.get() > returned);

      Scanner scanner2 = client.createScanner(table);
      IteratorSetting is2 = new IteratorSetting(11, MemoryCheckingIterator.class, Map.of());
      scanner2.addScanIterator(is2);
      Iterator<Entry<Key,Value>> iter2 = scanner2.iterator();
      assertTrue(iter2.hasNext());
      scanner.close();

      Thread.sleep(3000);

      client.tableOperations().delete(table);
    }
  }

}

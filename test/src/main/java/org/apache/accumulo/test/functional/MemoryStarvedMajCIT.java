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

import static org.apache.accumulo.test.util.Wait.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.DoubleAdder;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled // ELASTICITY_TODO
public class MemoryStarvedMajCIT extends SharedMiniClusterBase {

  public static class MemoryStarvedITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.setMemory(ServerType.TABLET_SERVER, 256, MemoryUnit.MEGABYTE);
      // Configure the LowMemoryDetector in the TabletServer
      // check on 1s intervals and set low mem condition if more than 80% of
      // the heap is used.
      cfg.setProperty(Property.GENERAL_LOW_MEM_DETECTOR_INTERVAL, "5s");
      cfg.setProperty(Property.GENERAL_LOW_MEM_DETECTOR_THRESHOLD,
          Double.toString(MemoryStarvedScanIT.FREE_MEMORY_THRESHOLD));
      cfg.setProperty(Property.GENERAL_LOW_MEM_MAJC_PROTECTION, "true");
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

  private static final DoubleAdder MAJC_PAUSED = new DoubleAdder();
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
            if (MetricsProducer.METRICS_MAJC_PAUSED.equals(metric.getName())) {
              double val = Double.parseDouble(metric.getValue());
              MAJC_PAUSED.add(val);
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
    MAJC_PAUSED.reset();
  }

  @Test
  public void testMajCPauses() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      TableOperations to = client.tableOperations();
      to.create(table);

      // Add a small amount of data so that the MemoryConsumingIterator
      // returns true when trying to consume all of the memory.
      ReadWriteIT.ingest(client, 1, 1, 1, 0, table);

      AtomicReference<Throwable> error = new AtomicReference<>();
      Thread compactionThread = new Thread(() -> {
        try {
          to.compact(table, new CompactionConfig().setWait(false));
        } catch (Exception e) {
          error.set(e);
        }
      });

      try (Scanner scanner = client.createScanner(table)) {

        MemoryStarvedScanIT.consumeServerMemory(scanner);

        int paused = MAJC_PAUSED.intValue();
        assertEquals(0, paused);

        ReadWriteIT.ingest(client, 100, 100, 100, 0, table);
        compactionThread.start();

        waitFor(() -> MAJC_PAUSED.intValue() > 0);

        MemoryStarvedScanIT.freeServerMemory(client);
        compactionThread.interrupt();
        compactionThread.join();
        assertNull(error.get());
        assertTrue(client.instanceOperations().getActiveCompactions().stream()
            .anyMatch(ac -> ac.getPausedCount() > 0));
      }
    }

  }
}

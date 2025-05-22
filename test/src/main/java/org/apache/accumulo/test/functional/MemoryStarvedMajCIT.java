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

import static org.apache.accumulo.core.metrics.Metric.MAJC_PAUSED;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.getActiveCompactions;
import static org.apache.accumulo.test.util.Wait.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.DoubleAdder;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class MemoryStarvedMajCIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStarvedMajCIT.class);

  public static class MemoryStarvedITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(0);
      cfg.getClusterServerConfiguration().setNumDefaultCompactors(1);
      cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
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

      // Set a compactor that will consume and free memory when we need it to
      cfg.setServerClass(ServerType.COMPACTOR, MemoryConsumingCompactor.class);
    }
  }

  private static final DoubleAdder MAJC_PAUSED_COUNT = new DoubleAdder();
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
            if (MAJC_PAUSED.getName().equals(metric.getName())) {
              double val = Double.parseDouble(metric.getValue());
              MAJC_PAUSED_COUNT.add(val);
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
    MAJC_PAUSED_COUNT.reset();
  }

  @Test
  public void testMajCPauses() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      ClientContext ctx = (ClientContext) client;

      Wait.waitFor(() -> ctx.getServerPaths()
          .getCompactor(rg -> rg.equals(Constants.DEFAULT_RESOURCE_GROUP_NAME),
              AddressSelector.all(), true)
          .size() == 1, 60_000);

      ServerId csi = ctx.instanceOperations().getServers(ServerId.Type.COMPACTOR).iterator().next();
      HostAndPort compactorAddr = HostAndPort.fromParts(csi.getHost(), csi.getPort());

      TableOperations to = client.tableOperations();
      to.create(table);

      AtomicReference<Throwable> error = new AtomicReference<>();
      Thread compactionThread = new Thread(() -> {
        try {
          to.compact(table, new CompactionConfig().setWait(false));
        } catch (Exception e) {
          error.set(e);
        }
      });

      int paused = MAJC_PAUSED_COUNT.intValue();
      assertEquals(0, paused);

      // Calling getRunningCompaction on the MemoryConsumingCompactor
      // will consume the free memory
      LOG.info("Calling getRunningCompaction on {}", compactorAddr);
      boolean success = false;
      while (!success) {
        try {
          ExternalCompactionUtil.getRunningCompaction(compactorAddr, ctx);
          success = true;
        } catch (Exception e) {
          UtilWaitThread.sleep(3000);
        }
      }

      ReadWriteIT.ingest(client, 100, 100, 100, 0, table);
      compactionThread.start();

      waitFor(() -> MAJC_PAUSED_COUNT.intValue() > 0);

      // Calling cancel on the MemoryConsumingCompactor will free
      // the consumed memory
      LOG.info("Calling cancel on {}", compactorAddr);
      ExternalCompactionUtil.cancelCompaction(ctx, compactorAddr, "fakeECID");

      compactionThread.interrupt();
      compactionThread.join();
      assertNull(error.get());
      assertTrue(getActiveCompactions(client.instanceOperations()).stream()
          .anyMatch(ac -> ac.getPausedCount() > 0));
    }

  }
}

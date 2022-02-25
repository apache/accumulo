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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ExternalCompactionMetricsIT extends AccumuloClusterHarness
    implements MiniClusterConfigurationCallback {

  private static TestStatsDSink sink;

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    if (sink != null) {
      sink.close();
    }
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    cfg.setNumCompactors(2);

    // Tell the server processes to use a StatsDMeterRegistry that will be configured
    // to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, TestStatsDRegistryFactory.class.getName());
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

  @Test
  public void testMetrics() throws Exception {
    Collection<ProcessReference> tservers =
        ((MiniAccumuloClusterImpl) getCluster()).getProcesses().get(ServerType.TABLET_SERVER);
    assertEquals(2, tservers.size());
    // kill one tserver so that queue metrics are not spread across tservers
    ((MiniAccumuloClusterImpl) getCluster()).killProcess(TABLET_SERVER, tservers.iterator().next());
    String[] names = getUniqueNames(2);
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      String table1 = names[0];
      createTable(client, table1, "cs1", 5);

      String table2 = names[1];
      createTable(client, table2, "cs2", 10);

      writeData(client, table1);
      writeData(client, table2);

      final LinkedBlockingQueue<Metric> queueMetrics = new LinkedBlockingQueue<>();
      final AtomicBoolean shutdownTailer = new AtomicBoolean(false);

      Thread thread = Threads.createThread("metric-tailer", () -> {
        while (!shutdownTailer.get()) {
          List<String> statsDMetrics = sink.getLines();
          for (String s : statsDMetrics) {
            if (shutdownTailer.get()) {
              break;
            }
            if (s.startsWith(MetricsProducer.METRICS_MAJC_QUEUED)) {
              queueMetrics.add(TestStatsDSink.parseStatsDMetric(s));
            }
          }
        }
      });
      thread.start();

      compact(client, table1, 7, "DCQ1", false);
      compact(client, table2, 13, "DCQ2", false);

      boolean sawDCQ1_5 = false;
      boolean sawDCQ2_10 = false;

      // wait until expected number of queued are seen in metrics
      while (!sawDCQ1_5 || !sawDCQ2_10) {
        Metric qm = queueMetrics.take();
        sawDCQ1_5 |= match(qm, "DCQ1", "5");
        sawDCQ2_10 |= match(qm, "DCQ2", "10");
      }

      cluster.getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);
      cluster.getClusterControl().startCompactors(Compactor.class, 1, QUEUE2);
      cluster.getClusterControl().startCoordinator(CompactionCoordinator.class);

      boolean sawDCQ1_0 = false;
      boolean sawDCQ2_0 = false;

      // wait until queued goes to zero in metrics
      while (!sawDCQ1_0 || !sawDCQ2_0) {
        Metric qm = queueMetrics.take();
        sawDCQ1_0 |= match(qm, "DCQ1", "0");
        sawDCQ2_0 |= match(qm, "DCQ2", "0");
      }

      shutdownTailer.set(true);
      thread.join();

      // Wait for all external compactions to complete
      long count;
      do {
        UtilWaitThread.sleep(100);
        try (TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
            .forLevel(DataLevel.USER).fetch(ColumnType.ECOMP).build()) {
          count = tm.stream().flatMap(t -> t.getExternalCompactions().keySet().stream()).count();
        }
      } while (count > 0);

      verify(client, table1, 7);
      verify(client, table2, 13);

    } finally {
      // We stopped the TServer and started our own, restart the original TabletServers
      // Uncomment this if other tests are added.
      //
      // cluster.getClusterControl().start(ServerType.TABLET_SERVER);
    }
  }

  private static boolean match(Metric input, String queue, String value) {
    if (input.getTags() != null) {
      String id = input.getTags().get("id");
      if (id != null && id.equals("e." + queue) && input.getValue().equals(value)) {
        return true;
      }
    }
    return false;
  }

}

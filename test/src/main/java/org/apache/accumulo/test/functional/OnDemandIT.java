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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OnDemandIT extends SharedMiniClusterBase {

  private static final int managerTabletGroupWatcherInterval = 5;
  private static final int inactiveOnDemandTabletUnloaderInterval = 30;
  private static TestStatsDSink sink;
  private static Thread metricConsumer;
  private static Long ONDEMAND_ONLINE_COUNT = 0L;

  @BeforeAll
  public static void beforeAll() throws Exception {
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
            if (MetricsProducer.METRICS_TSERVER_TABLETS_ONLINE_ONDEMAND.equals(metric.getName())) {
              Long val = Long.parseLong(metric.getValue());
              ONDEMAND_ONLINE_COUNT = val;
            }
          }
        }
      }
    });
    metricConsumer.start();
    SharedMiniClusterBase.startMiniClusterWithConfig((cfg, core) -> {
      cfg.setNumTservers(1);
      cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL,
          Integer.toString(managerTabletGroupWatcherInterval));
      cfg.setProperty(Property.TABLE_ONDEMAND_UNLOADER_INTERVAL,
          Integer.toString(inactiveOnDemandTabletUnloaderInterval));
      cfg.setProperty("table.custom.ondemand.unloader.inactivity.threshold", "30000");

      // Tell the server processes to use a StatsDMeterRegistry that will be configured
      // to push all metrics to the sink we started.
      cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
      cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY,
          TestStatsDRegistryFactory.class.getName());
      Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
          TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
      cfg.setSystemProperties(sysProps);
    });
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
    metricConsumer.interrupt();
    metricConsumer.join();
  }

  @BeforeEach
  public void before() {
    ONDEMAND_ONLINE_COUNT = 0L;
  }

  @Test
  public void testLoadUnload() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = super.getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      ManagerAssignmentIT.loadDataForScan(c, tableName);
      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("f"));
      splits.add(new Text("m"));
      splits.add(new Text("t"));
      c.tableOperations().addSplits(tableName, splits);
      // Need to offline the table first so that the tablets
      // are unloaded.
      c.tableOperations().offline(tableName, true);
      c.tableOperations().onDemand(tableName, true);
      assertTrue(c.tableOperations().isOnDemand(tableName));

      List<TabletStats> stats = ManagerAssignmentIT.getTabletStats(c, tableId);
      // There should be no tablets online
      assertEquals(0, stats.size());
      assertEquals(0, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());
      assertEquals(0, ONDEMAND_ONLINE_COUNT);

      c.tableOperations().clearLocatorCache(tableName);

      // load the same data again, this will cause the tablets to be brought online.
      ManagerAssignmentIT.loadDataForScan(c, tableName);

      while (ONDEMAND_ONLINE_COUNT != 4) {
        Thread.sleep(500);
      }
      stats = ManagerAssignmentIT.getTabletStats(c, tableId);
      assertEquals(4, stats.size());
      assertEquals(4, TabletLocator.getLocator((ClientContext) c, TableId.of(tableId))
          .onDemandTabletsOnlined());

      // wait for the inactivity timeout to cause the tablets to be unloaded from the
      // TabletServer
      while (ONDEMAND_ONLINE_COUNT != 0) {
        Thread.sleep(500);
      }
      stats = ManagerAssignmentIT.getTabletStats(c, tableId);
      assertEquals(0, stats.size());

    }
  }

}

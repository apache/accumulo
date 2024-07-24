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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdleProcessMetricsIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(IdleProcessMetricsIT.class);

  static final Duration idleProcessInterval = Duration.ofSeconds(10);

  public static final String IDLE_RESOURCE_GROUP = "IDLE_PROCESS_TEST";

  public static class IdleStopITConfig implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {

      // Verify expectations about the default config. Want to ensure there no other resource groups
      // configured.
      assertEquals(Map.of(Constants.DEFAULT_COMPACTION_SERVICE_NAME, 1),
          cfg.getClusterServerConfiguration().getCompactorConfiguration());

      // Disable the default scan servers and compactors, just start 1
      // tablet server in the default group to host the root and metadata
      // tables
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(0);
      cfg.getClusterServerConfiguration().setNumDefaultCompactors(0);

      // Add servers in a resource group that will not get any work. These
      // are the servers that should stop because they are idle.
      cfg.getClusterServerConfiguration().addTabletServerResourceGroup(IDLE_RESOURCE_GROUP, 1);
      cfg.getClusterServerConfiguration().addScanServerResourceGroup(IDLE_RESOURCE_GROUP, 1);
      cfg.getClusterServerConfiguration().addCompactorResourceGroup(IDLE_RESOURCE_GROUP, 1);

      cfg.setProperty(Property.GENERAL_IDLE_PROCESS_INTERVAL,
          idleProcessInterval.toSeconds() + "s");

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

  private static TestStatsDSink sink;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
    SharedMiniClusterBase.startMiniClusterWithConfig(new IdleStopITConfig());
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testIdleStopMetrics() throws Exception {

    // should emit the idle metric after the configured duration of GENERAL_IDLE_PROCESS_INTERVAL
    Thread.sleep(idleProcessInterval.toMillis());

    AtomicBoolean sawCompactor = new AtomicBoolean(false);
    AtomicBoolean sawSServer = new AtomicBoolean(false);
    AtomicBoolean sawTServer = new AtomicBoolean(false);

    Wait.waitFor(() -> {
      List<String> statsDMetrics = sink.getLines();
      statsDMetrics.stream().filter(line -> line.startsWith(MetricsProducer.METRICS_SERVER_IDLE))
          .peek(log::info).map(TestStatsDSink::parseStatsDMetric)
          .filter(a -> a.getTags().get("resource.group").equals(IDLE_RESOURCE_GROUP)).forEach(a -> {
            String processName = a.getTags().get("process.name");
            int value = Integer.parseInt(a.getValue());
            assertTrue(value == 0 || value == 1 || value == -1, "Unexpected value " + value);
            // check that the idle metric was emitted for each
            if ("tserver".equals(processName) && value == 1) {
              sawTServer.set(true);
            } else if ("sserver".equals(processName) && value == 1) {
              sawSServer.set(true);
            } else if ("compactor".equals(processName) && value == 1) {
              sawCompactor.set(true);
            }

          });
      return sawCompactor.get() && sawSServer.get() && sawTServer.get();
    });
  }

}

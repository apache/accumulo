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
package org.apache.accumulo.test.metrics;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.fate.FateTestUtil;
import org.apache.accumulo.test.fate.SlowFateSplitManager;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;

public class MetricsDisabledIT extends ConfigurableMacBase implements MetricsProducer {
  private static TestStatsDSink sink;
  private static final int numFateThreadsPool1 = 5;
  private static final String allOpsFateExecutorName = "pool1";

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

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
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL, "1s");
    // Tell the server processes to use a StatsDMeterRegistry and the simple logging registry
    // that will be configured to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_USER_TAGS, "tag1=value1,tag2=value2");
    cfg.setProperty(Property.GENERAL_MICROMETER_CACHE_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.setProperty("general.custom.metrics.opts.logging.step", "10s");
    String clazzList = LoggingMeterRegistryFactory.class.getName() + ","
        + TestStatsDRegistryFactory.class.getName();
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, clazzList);
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
    // custom config for the fate thread pools.
    var fatePoolsConfig = FateTestUtil.updateFateConfig(new ConfigurationCopy(),
        numFateThreadsPool1, allOpsFateExecutorName);
    cfg.setProperty(Property.MANAGER_FATE_USER_CONFIG.getKey(),
        fatePoolsConfig.get(Property.MANAGER_FATE_USER_CONFIG));
    cfg.setProperty(Property.MANAGER_FATE_META_CONFIG.getKey(),
        fatePoolsConfig.get(Property.MANAGER_FATE_META_CONFIG));
    // Make splits run slowly, used for testing the fate metrics
    cfg.setServerClass(ServerType.MANAGER, r -> SlowFateSplitManager.class);
  }

  @Test
  public void confirmNoMetricsPublished() throws Exception {

    AtomicReference<Exception> error = new AtomicReference<>();
    Thread workerThread = new Thread(() -> {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
        MetricsIT.doWorkToGenerateMetrics(client, getClass());
      } catch (Exception e) {
        error.set(e);
      }
    });
    workerThread.start();

    Timer t = Timer.startNew();

    // GENERAL_MICROMETER_ENABLED is set to false in MiniAccumuloConfigImpl
    // We should not see any metrics.
    while (t.elapsed(TimeUnit.SECONDS) < 60) {
      List<String> lines = sink.getLines();
      assertTrue(lines.isEmpty(), "Encountered the following metrics when disabled: " + lines);
    }
    workerThread.join();
    assertNull(error.get());
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // unused; this class only extends MetricsProducer to easily reference its methods/constants
  }
}

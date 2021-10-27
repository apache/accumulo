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
package org.apache.accumulo.test.metrics;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.micrometer.core.instrument.MeterRegistry;

public class MetricsIT extends ConfigurableMacBase implements MetricsProducer {

  private static TestStatsDSink sink;

  @BeforeClass
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterClass
  public static void after() throws Exception {
    if (sink != null) {
      sink.close();
    }
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL, "1s");

    // Tell the server processes to use a StatsDMeterRegistry that will be configured
    // to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, TestStatsDRegistryFactory.class.getName());
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void confirmMetricsPublished() throws Exception {
    Set<String> flakyMetricNames = new HashSet<>();
    flakyMetricNames.add(METRICS_GC_WAL_ERRORS);

    Map<String,String> expectedMetricNames = this.getMetricFields();
    // We might not see these in the course of normal operations
    expectedMetricNames.remove(METRICS_SCAN_YIELDS);
    expectedMetricNames.remove(METRICS_UPDATE_ERRORS);
    expectedMetricNames.remove(METRICS_REPLICATION_QUEUE);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = this.getClass().getSimpleName();
      client.tableOperations().create(tableName);
      BatchWriterConfig config = new BatchWriterConfig();
      config.setMaxMemory(0);
      try (BatchWriter writer = client.createBatchWriter(tableName, config)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", new Value("value"));
        writer.addMutation(m);
      }
      client.tableOperations().flush(tableName);
      try (BatchWriter writer = client.createBatchWriter(tableName, config)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", new Value("value"));
        writer.addMutation(m);
      }
      client.tableOperations().flush(tableName);
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      client.tableOperations().delete(tableName);
      while (client.tableOperations().exists(tableName)) {
        Thread.sleep(1000);
      }
    }

    Thread.sleep(30000);
    cluster.stop();

    Map<String,String> seenMetricNames = new HashMap<>();

    List<String> statsDMetrics = sink.getLines();
    while (!statsDMetrics.isEmpty()) {
      for (String s : statsDMetrics) {
        if (!s.startsWith("accumulo")) {
          continue;
        }
        Metric m = TestStatsDSink.parseStatsDMetric(s);
        boolean hasBeenSeen = seenMetricNames.containsKey(m.getName());
        boolean isExpectedMetricName = expectedMetricNames.containsKey(m.getName());
        if (!hasBeenSeen && isExpectedMetricName) {
          String expectedValue = expectedMetricNames.remove(m.getName());
          seenMetricNames.put(m.getName(), expectedValue);
        } else if (!hasBeenSeen && !isExpectedMetricName) {
          if (!flakyMetricNames.contains(m.getName())) {
            fail("Found accumulo metric not in expectedMetricNames: " + m.getName());
          }
        }
        if (expectedMetricNames.isEmpty()) {
          break;
        }
      }
      statsDMetrics = sink.getLines();
    }
    if (!expectedMetricNames.isEmpty()) {
      fail("Did not see all metric names, missing: " + expectedMetricNames.values());
    }

  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // unused
  }

}

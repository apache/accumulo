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
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.micrometer.core.instrument.MeterRegistry;

public class MetricsIT extends ConfigurableMacBase implements MetricsProducer {

  private static class Metric {
    private final String name;
    private final String value;
    private final String type;
    private final Map<String,String> tags = new HashMap<>();

    public Metric(String name, String value, String type) {
      super();
      this.name = name;
      this.value = value;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    @SuppressWarnings("unused")
    public String getValue() {
      return value;
    }

    @SuppressWarnings("unused")
    public String getType() {
      return type;
    }

    public Map<String,String> getTags() {
      return tags;
    }

    @Override
    public String toString() {
      return "Metric [name=" + name + ", value=" + value + ", type=" + type + ", tags=" + tags
          + "]";
    }
  }

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
    @SuppressWarnings("deprecation")
    Property p = Property.GC_METRICS_ENABLED;
    cfg.setProperty(p, "true");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL, "5s");

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
    }

    Thread.sleep(5000);

    Map<String,String> seenMetricNames = new HashMap<>();
    List<String> statsDMetrics = sink.getLines();
    for (String s : statsDMetrics) {
      Metric m = parseStatsDMetric(s);
      boolean hasBeenSeen = seenMetricNames.containsKey(m.getName());
      boolean isExpectedMetricName = expectedMetricNames.containsKey(m.getName());
      if (!hasBeenSeen && isExpectedMetricName) {
        String expectedValue = expectedMetricNames.remove(m.getName());
        seenMetricNames.put(m.getName(), expectedValue);
      } else if (!hasBeenSeen && !isExpectedMetricName) {
        if (m.getName().startsWith("accumulo")) {
          fail("Found accumulo metric not in expectedMetricNames: " + m.getName());
        } else {
          // We are likely getting other metrics (jvm, thread pools, etc.) that
          // are set up in MicrometerMetricsFactory
        }
      }
      if (expectedMetricNames.isEmpty()) {
        break;
      }
    }
    if (!expectedMetricNames.isEmpty()) {
      fail("Did not see all metric names, missing: " + expectedMetricNames.values());
    }

  }

  private Metric parseStatsDMetric(String line) {
    int idx = line.indexOf(':');
    String name = line.substring(0, idx);
    int idx2 = line.indexOf('|');
    String value = line.substring(idx + 1, idx2);
    int idx3 = line.indexOf('|', idx2 + 1);
    String type = line.substring(idx2 + 1, idx3);
    int idx4 = line.indexOf('#');
    String tags = line.substring(idx4 + 1);
    Metric m = new Metric(name, value, type);
    String[] tag = tags.split(",");
    for (String t : tag) {
      String[] p = t.split(":");
      m.getTags().put(p[0], p[1]);
    }
    return m;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // unused
  }

}

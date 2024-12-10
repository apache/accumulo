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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.core.Constants.DEFAULT_COMPACTION_SERVICE_NAME;
import static org.apache.accumulo.core.metrics.Metric.COMPACTION_SVC_ERRORS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.spi.compaction.RatioBasedCompactionPlanner;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MoreCollectors;

public class BadCompactionServiceConfigIT extends AccumuloClusterHarness {

  private static final Logger LOG = LoggerFactory.getLogger(BadCompactionServiceConfigIT.class);
  private static final String CSP = Property.COMPACTION_SERVICE_PREFIX.getKey();
  private static TestStatsDSink sink;

  @BeforeAll
  public static void beforeTests() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteCfg = new HashMap<>();
    siteCfg.put(CSP + DEFAULT_COMPACTION_SERVICE_NAME + ".planner",
        RatioBasedCompactionPlanner.class.getName());
    siteCfg.put(CSP + DEFAULT_COMPACTION_SERVICE_NAME + ".planner.opts.groups",
        "[{\"group\":\"default\"}]");
    siteCfg.put(CSP + "cs1.planner", RatioBasedCompactionPlanner.class.getName());
    // place invalid json in the planners config
    siteCfg.put(CSP + "cs1.planner.opts.groups", "{{'group]");
    cfg.setSiteConfig(siteCfg);
    cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "3s");
    // Tell the server processes to use a StatsDMeterRegistry and the simple logging registry
    // that will be configured to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
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
  }

  public static class EverythingFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      return false;
    }
  }

  private ExecutorService executorService;

  @BeforeEach
  public void setup() {
    executorService = Executors.newCachedThreadPool();
  }

  @AfterEach
  public void teardown() {
    executorService.shutdownNow();
  }

  @Test
  public void testUsingMisconfiguredService() throws Exception {

    final AtomicBoolean shutdownTailer = new AtomicBoolean(false);
    final AtomicBoolean serviceMisconfigured = new AtomicBoolean(false);
    final Thread thread = Threads.createThread("metric-tailer", () -> {
      while (!shutdownTailer.get()) {
        List<String> statsDMetrics = sink.getLines();
        for (String s : statsDMetrics) {
          if (shutdownTailer.get()) {
            break;
          }
          if (s.startsWith(COMPACTION_SVC_ERRORS.getName())) {
            Metric m = TestStatsDSink.parseStatsDMetric(s);
            Integer value = Integer.parseInt(m.getValue());
            if (value == 0) {
              serviceMisconfigured.set(false);
            } else if (value == 1) {
              serviceMisconfigured.set(true);
            } else {
              LOG.error("Invalid value received: " + m.getValue());
            }
          }
        }
      }
    });
    thread.start();

    String table = getUniqueNames(1)[0];

    // Create a table that is configured to use a compaction service with bad configuration.
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(
          Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "cs1"));
      client.tableOperations().create(table, ntc);

      Wait.waitFor(() -> serviceMisconfigured.get() == true);

      try (var writer = client.createBatchWriter(table)) {
        writer.addMutation(new Mutation("0").at().family("f").qualifier("q").put("v"));
      }

      client.tableOperations().flush(table, null, null, true);

      try (var scanner = client.createScanner(table)) {
        assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(MoreCollectors.onlyElement()));
      }

      Future<?> fixerFuture = executorService.submit(() -> {
        try {
          Thread.sleep(2000);

          // Verify the compaction has not run yet, it should not be able to with the bad config.
          try (var scanner = client.createScanner(table)) {
            assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
                .collect(MoreCollectors.onlyElement()));
          }

          var value = "[{'group':'cs1q1'}]".replaceAll("'", "\"");
          client.instanceOperations().setProperty(CSP + "cs1.planner.opts.groups", value);

          // start the compactor, it was not started initially because of bad config
          ((MiniAccumuloClusterImpl) getCluster()).getConfig().getClusterServerConfiguration()
              .addCompactorResourceGroup("cs1q1", 1);
          ((MiniAccumuloClusterImpl) getCluster()).getClusterControl().start(ServerType.COMPACTOR);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      Wait.waitFor(() -> serviceMisconfigured.get() == false);

      List<IteratorSetting> iterators =
          Collections.singletonList(new IteratorSetting(100, EverythingFilter.class));
      client.tableOperations().compact(table,
          new CompactionConfig().setIterators(iterators).setWait(true));

      // Verify compaction ran.
      try (var scanner = client.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

      fixerFuture.get();

      // misconfigure the service, test how going from good config to bad config works. The test
      // started with an initial state of bad config.
      client.instanceOperations().setProperty(CSP + "cs1.planner.opts.groups", "]o.o[");
      Wait.waitFor(() -> serviceMisconfigured.get() == true);

      try (var writer = client.createBatchWriter(table)) {
        writer.addMutation(new Mutation("0").at().family("f").qualifier("q").put("v"));
      }
      client.tableOperations().flush(table, null, null, true);
      try (var scanner = client.createScanner(table)) {
        assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(MoreCollectors.onlyElement()));
      }
      fixerFuture = executorService.submit(() -> {
        try {
          Thread.sleep(2000);
          var value = "[{'group':'cs1q1'}]".replaceAll("'", "\"");
          client.instanceOperations().setProperty(CSP + "cs1.planner.opts.groups", value);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      Wait.waitFor(() -> serviceMisconfigured.get() == false);

      client.tableOperations().compact(table,
          new CompactionConfig().setIterators(iterators).setWait(true));

      // Verify compaction ran.
      try (var scanner = client.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

      fixerFuture.get();
    } finally {
      shutdownTailer.set(true);
      thread.join();
    }
  }

  @Test
  public void testUsingNonExistentService() throws Exception {

    final AtomicBoolean shutdownTailer = new AtomicBoolean(false);
    final AtomicBoolean serviceMisconfigured = new AtomicBoolean(false);
    final Thread thread = Threads.createThread("metric-tailer", () -> {
      while (!shutdownTailer.get()) {
        List<String> statsDMetrics = sink.getLines();
        for (String s : statsDMetrics) {
          if (shutdownTailer.get()) {
            break;
          }
          if (s.startsWith(COMPACTION_SVC_ERRORS.getName())) {
            Metric m = TestStatsDSink.parseStatsDMetric(s);
            Integer value = Integer.parseInt(m.getValue());
            if (value == 0) {
              serviceMisconfigured.set(false);
            } else if (value == 1) {
              serviceMisconfigured.set(true);
            } else {
              LOG.error("Invalid value received: " + m.getValue());
            }
          }
        }
      }
    });
    thread.start();

    String table = getUniqueNames(1)[0];

    // Create a table that is configured to use a compaction service that does not exist
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(
          Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "cs5"));
      client.tableOperations().create(table, ntc);

      Wait.waitFor(() -> serviceMisconfigured.get() == true);

      // The setup of this test creates an invalid configuration, fix this first thing.
      var value = "[{'group':'cs1q1'}]".replaceAll("'", "\"");
      client.instanceOperations().setProperty(CSP + "cs1.planner.opts.groups", value);

      Wait.waitFor(() -> serviceMisconfigured.get() == false);

      // Add splits so that the tserver logs can manually be inspected to ensure they are not
      // spammed. Not sure how to check this automatically.
      var splits = IntStream.range(1, 10).mapToObj(i -> new Text(i + ""))
          .collect(Collectors.toCollection(TreeSet::new));
      client.tableOperations().addSplits(table, splits);

      try (var writer = client.createBatchWriter(table)) {
        writer.addMutation(new Mutation("0").at().family("f").qualifier("q").put("v"));
      }

      client.tableOperations().flush(table, null, null, true);

      try (var scanner = client.createScanner(table)) {
        assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(MoreCollectors.onlyElement()));
      }

      // Create a thread to fix the compaction config after a bit.
      Future<?> fixerFuture = executorService.submit(() -> {
        try {
          Thread.sleep(2000);

          // Verify the compaction has not run yet, it should not be able to with the bad config.
          try (var scanner = client.createScanner(table)) {
            assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
                .collect(MoreCollectors.onlyElement()));
          }

          // fix the compaction dispatcher config
          client.tableOperations().setProperty(table,
              Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service",
              DEFAULT_COMPACTION_SERVICE_NAME);

        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      Wait.waitFor(() -> fixerFuture.isDone());
      fixerFuture.get();

      List<IteratorSetting> iterators =
          Collections.singletonList(new IteratorSetting(100, EverythingFilter.class));
      client.tableOperations().compact(table,
          new CompactionConfig().setIterators(iterators).setWait(true));

      // Verify compaction ran.
      try (var scanner = client.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

    } finally {
      shutdownTailer.set(true);
      thread.join();
    }
  }
}

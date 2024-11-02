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

import static org.apache.accumulo.core.metrics.Metric.MINC_QUEUED;
import static org.apache.accumulo.core.metrics.Metric.MINC_RUNNING;
import static org.apache.accumulo.core.metrics.Metric.SCAN_CLOSE;
import static org.apache.accumulo.core.metrics.Metric.SCAN_CONTINUE;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERIES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERY_SCAN_RESULTS;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERY_SCAN_RESULTS_BYTES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_RESULTS;
import static org.apache.accumulo.core.metrics.Metric.SCAN_SCANNED_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_START;
import static org.apache.accumulo.core.metrics.Metric.SCAN_TIMES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_YIELDS;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_INGEST_BYTES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_INGEST_MUTATIONS;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MEM_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MINC_QUEUED;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_MINC_RUNNING;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_FILES;
import static org.apache.accumulo.core.metrics.Metric.TSERVER_TABLETS_ONLINE_ONDEMAND;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_ERRORS;
import static org.apache.accumulo.core.metrics.Metric.UPDATE_MUTATION_ARRAY_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.metrics.PerTableMetrics;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.micrometer.core.instrument.MeterRegistry;

public class PerTableMetricsIT extends ConfigurableMacBase implements MetricsProducer {

  private static TestStatsDSink sink;

  private static final Logger log = LoggerFactory.getLogger(PerTableMetricsIT.class);

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
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_TABLE_METRICS_ENABLED, "true");
    cfg.setProperty("general.custom.metrics.opts.logging.step", "10s");
    String clazzList = LoggingMeterRegistryFactory.class.getName() + ","
        + TestStatsDRegistryFactory.class.getName();
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, clazzList);
    // this will cause more frequent activity on the scan ref table generating metrics for that
    // table
    cfg.setProperty(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION, "3s");
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

  @Test
  public void confirmMetricsPublished() throws Exception {

    // meter names sorted and formatting disabled to make it easier to diff changes
    // @formatter:off

    // all metrics that can have a table id
    Set<Metric> expectedPerTableMetrics = Set.of(
            SCAN_TIMES,
            SCAN_RESULTS,
            SCAN_YIELDS,
            SCAN_START,
            SCAN_CONTINUE,
            SCAN_CLOSE,
            SCAN_QUERIES,
            SCAN_SCANNED_ENTRIES,
            SCAN_QUERY_SCAN_RESULTS,
            SCAN_QUERY_SCAN_RESULTS_BYTES,
            TSERVER_ENTRIES,
            TSERVER_MEM_ENTRIES,
            TSERVER_MINC_RUNNING,
            TSERVER_MINC_QUEUED,
            TSERVER_TABLETS_ONLINE_ONDEMAND,
            TSERVER_TABLETS_FILES,
            TSERVER_INGEST_MUTATIONS,
            TSERVER_INGEST_BYTES,
            MINC_RUNNING,
            MINC_QUEUED,
            UPDATE_ERRORS,
            UPDATE_MUTATION_ARRAY_SIZE);

    // metrics this test may not see
    Set<Metric> tableMetricsOkToNoSee = Set.of(
            SCAN_YIELDS);

    // @formatter:on

    String[] tableNames = getUniqueNames(10);
    Set<String> testTables = Collections.synchronizedSet(new HashSet<>(Arrays.asList(tableNames)));

    var executor = Executors.newCachedThreadPool();

    List<Future<String>> futures = new ArrayList<>();

    for (var tableName : tableNames) {
      futures.add(executor.submit(() -> {
        doWorkToGenerateMetrics(tableName, testTables);
        return tableName;
      }));
    }

    Set<TableId> expectedTableIds = new HashSet<>();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      while (expectedTableIds.size() != tableNames.length) {
        client.tableOperations().tableIdMap().forEach((name, id) -> {
          if (testTables.contains(name)) {
            expectedTableIds.add(TableId.of(id));
          }
        });
        Thread.sleep(10);
      }
    }

    expectedTableIds.addAll(AccumuloTable.allTableIds());

    Map<Metric,Set<TableId>> metricsSeen = new HashMap<>();
    Set<String> processNamesSeen = new HashSet<>();

    while (true) {
      // generate minor compaction metrics for the accumulo system tables
      try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
        for (var accumuloTable : AccumuloTable.values()) {
          client.tableOperations().flush(accumuloTable.tableName());
        }
      }

      for (String meticLine : sink.getLines()) {
        if (meticLine.startsWith("accumulo")) {
          var statsdMetric = TestStatsDSink.parseStatsDMetric(meticLine);
          var metric = Metric.fromName(statsdMetric.getName());
          var tableId = statsdMetric.getTags().get(PerTableMetrics.TABLE_ID_TAG_NAME);
          if (tableId != null) {
            assertTrue(expectedTableIds.contains(TableId.of(tableId)),
                () -> "Unexpected table id seen in tag " + tableId + " " + meticLine);
            // only some metrics are expected to have a table id, ensure the tableId tag does not
            // show up where its not expected
            assertTrue(expectedPerTableMetrics.contains(metric),
                () -> "Saw unexpected table id tag on metric " + metric + " " + tableId + " "
                    + meticLine);
            metricsSeen.computeIfAbsent(metric, m -> new HashSet<>()).add(TableId.of(tableId));
            processNamesSeen.add(statsdMetric.getTags().get("process.name"));
          } else {
            assertFalse(expectedPerTableMetrics.contains(metric),
                () -> "Saw metric without expected tableId tag " + metric + " " + meticLine);
          }
        }
      }

      if (metricsSeen.keySet().equals(expectedPerTableMetrics) || metricsSeen.keySet()
          .equals(Sets.difference(expectedPerTableMetrics, tableMetricsOkToNoSee))) {
        // saw all expected metrics
        if (metricsSeen.values().stream().allMatch(tableIds -> tableIds.equals(expectedTableIds))) {
          if (testTables.size() == tableNames.length) {
            // Now that all tables ids were seen, remove two tables from the set of
            // expectedTableIds. This should cause the background threads to delete the tables and
            // eventually those tables should no longer be seen in metrics.
            var iter = testTables.iterator();
            var tid1 = iter.next();
            iter.remove();
            var tid2 = iter.next();
            iter.remove();
            assertEquals(tableNames.length - 2, testTables.size());
            log.debug("Removed tables {} {}", tid1, tid2);
            // clear metrics seen, going forward should eventually stop seeing the table id from the
            // deleted tables in metrics
            metricsSeen.clear();
          } else {
            // should have seen per table metrics from tablet and scan servers at this point
            assertEquals(Set.of("tserver", "sserver"), processNamesSeen);
            // have seen everything expected, so the test was successful
            break;
          }
        } else {
          metricsSeen.forEach((metricSeen, tableIdsSeen) -> {
            if (!tableIdsSeen.equals(expectedTableIds)) {
              log.debug("tableIds seen for metric not as expected {} {}", metricSeen,
                  Sets.symmetricDifference(expectedTableIds, tableIdsSeen));
            }
          });
        }
      } else {
        for (var m : Sets.difference(expectedPerTableMetrics, metricsSeen.keySet())) {
          log.debug("have not seen metric {}", m);
        }
      }

      Thread.sleep(1000);
    }

    // this will cause the rest of the background threads to stop
    testTables.clear();

    // check for any errors in background threads
    for (var future : futures) {
      future.get();
    }

    executor.shutdown();
    cluster.stop();
  }

  private void doWorkToGenerateMetrics(String tableName, Set<String> testTables) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.tableOperations().create(tableName);
      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("5")));
      client.tableOperations().addSplits(tableName, splits);
      while (testTables.contains(tableName)) {
        Thread.sleep(3_000);
        BatchWriterConfig config = new BatchWriterConfig().setMaxMemory(0);
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
        try (BatchWriter writer = client.createBatchWriter(tableName, config)) {
          for (int i = 0; i < 10; i++) {
            Mutation m = new Mutation(i + "_row");
            m.put("cf", "cq", new Value("value"));
            writer.addMutation(m);
          }
        }
        client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
        try (Scanner scanner = client.createScanner(tableName)) {
          scanner.forEach((k, v) -> {});
        }
        try (Scanner scanner = client.createScanner(tableName)) {
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
          scanner.forEach((k, v) -> {});
        }
      }
      client.tableOperations().delete(tableName);
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // unused; this class only extends MetricsProducer to easily reference its methods/constants
  }
}

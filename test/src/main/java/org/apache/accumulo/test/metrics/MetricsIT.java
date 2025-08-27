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

import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_CANCELLED;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_FAILED;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_FAILURES_CONSECUTIVE;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_FAILURES_TERMINATION;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_STUCK;
import static org.apache.accumulo.core.metrics.Metric.FATE_OPS_THREADS_INACTIVE;
import static org.apache.accumulo.core.metrics.Metric.FATE_OPS_THREADS_TOTAL;
import static org.apache.accumulo.core.metrics.Metric.FATE_TYPE_IN_PROGRESS;
import static org.apache.accumulo.core.metrics.Metric.MANAGER_BALANCER_MIGRATIONS_NEEDED;
import static org.apache.accumulo.core.metrics.Metric.SCAN_BUSY_TIMEOUT_COUNT;
import static org.apache.accumulo.core.metrics.Metric.SCAN_RESERVATION_CONFLICT_COUNTER;
import static org.apache.accumulo.core.metrics.Metric.SCAN_RESERVATION_TOTAL_TIMER;
import static org.apache.accumulo.core.metrics.Metric.SCAN_RESERVATION_WRITEOUT_TIMER;
import static org.apache.accumulo.core.metrics.Metric.SCAN_TABLET_METADATA_CACHE;
import static org.apache.accumulo.core.metrics.Metric.SCAN_YIELDS;
import static org.apache.accumulo.core.metrics.Metric.SERVER_IDLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateExecutorMetrics;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.fate.FateTestUtil;
import org.apache.accumulo.test.fate.SlowFateSplitManager;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;

public class MetricsIT extends ConfigurableMacBase implements MetricsProducer {
  private static final Logger log = LoggerFactory.getLogger(MetricsIT.class);
  private static TestStatsDSink sink;
  private static final int numFateThreadsPool1 = 5;
  private static final int numFateThreadsPool2 = 10;
  private static final int numFateThreadsPool3 = 15;

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
    // starting FATE config for each FATE type (USER and META) will be:
    // {<all fate ops>: numFateThreadsPool1} (one pool for all ops of size numFateThreadsPool1)
    var fatePoolsConfig = FateTestUtil.createTestFateConfig(numFateThreadsPool1);
    cfg.setProperty(Property.MANAGER_FATE_USER_CONFIG.getKey(),
        fatePoolsConfig.get(Property.MANAGER_FATE_USER_CONFIG));
    cfg.setProperty(Property.MANAGER_FATE_META_CONFIG.getKey(),
        fatePoolsConfig.get(Property.MANAGER_FATE_META_CONFIG));
    // Make splits run slowly, used for testing the fate metrics
    cfg.setServerClass(ServerType.MANAGER, r -> SlowFateSplitManager.class);
  }

  @Test
  public void confirmMetricsPublished() throws Exception {

    // meter names sorted and formatting disabled to make it easier to diff changes
    // @formatter:off
    Set<Metric> unexpectedMetrics = Set.of(
            SCAN_YIELDS,
            COMPACTOR_MAJC_CANCELLED,
            COMPACTOR_MAJC_FAILED,
            COMPACTOR_MAJC_FAILURES_CONSECUTIVE,
            COMPACTOR_MAJC_FAILURES_TERMINATION
    );

    // add sserver as flaky until scan server included in mini tests.
    Set<Metric> flakyMetrics = Set.of(
            COMPACTOR_MAJC_STUCK,
            FATE_TYPE_IN_PROGRESS,
            MANAGER_BALANCER_MIGRATIONS_NEEDED,
            SCAN_BUSY_TIMEOUT_COUNT,
            SCAN_RESERVATION_CONFLICT_COUNTER,
            SCAN_RESERVATION_TOTAL_TIMER,
            SCAN_RESERVATION_WRITEOUT_TIMER,
            SCAN_TABLET_METADATA_CACHE,
            SERVER_IDLE
    );
    // @formatter:on

    Set<Metric> expectedMetrics = new HashSet<>(Arrays.asList(Metric.values()));
    expectedMetrics.removeAll(flakyMetrics); // might not see these
    expectedMetrics.removeAll(unexpectedMetrics); // definitely shouldn't see these
    assertFalse(expectedMetrics.isEmpty()); // make sure we didn't remove everything

    Set<Metric> seenMetrics = new HashSet<>();

    List<String> statsDMetrics;

    final int compactionPriorityQueueQueuedBit = 0;
    final int compactionPriorityQueueDequeuedBit = 1;
    final int compactionPriorityQueueRejectedBit = 2;
    final int compactionPriorityQueuePriorityBit = 3;
    final int compactionPriorityQueueSizeBit = 4;

    final BitSet trueSet = new BitSet(5);
    trueSet.set(0, 4, true);

    final BitSet queueMetricsSeen = new BitSet(5);

    AtomicReference<Exception> error = new AtomicReference<>();
    Thread workerThread = new Thread(() -> {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
        doWorkToGenerateMetrics(client, getClass());
      } catch (Exception e) {
        error.set(e);
      }
    });
    workerThread.start();

    // loop until we run out of lines or until we see all expected metrics
    while (!(statsDMetrics = sink.getLines()).isEmpty() && !expectedMetrics.isEmpty()
        && !queueMetricsSeen.intersects(trueSet)) {
      // for each metric name not yet seen, check if it is expected, flaky, or unknown
      statsDMetrics.stream().filter(line -> line.startsWith("accumulo"))
          .map(TestStatsDSink::parseStatsDMetric).map(metric -> Metric.fromName(metric.getName()))
          .filter(metric -> !seenMetrics.contains(metric)).forEach(metric -> {
            if (expectedMetrics.contains(metric)) {
              // record expected Metric as seen
              seenMetrics.add(metric);
              expectedMetrics.remove(metric);
            } else if (flakyMetrics.contains(metric)) {
              // ignore any flaky metric names seen
              // these aren't always expected, but we shouldn't be surprised if we see them
            } else if (metric.getName().startsWith("accumulo.compaction.")) {
              // Compactor queue metrics are not guaranteed to be emitted
              // during the call to doWorkToGenerateMetrics above. This will
              // flip a bit in the BitSet when each metric is seen. The top-level
              // loop will continue to iterate until all the metrics are seen.
              seenMetrics.add(metric);
              expectedMetrics.remove(metric);
              switch (metric) {
                case COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED:
                  queueMetricsSeen.set(compactionPriorityQueueQueuedBit, true);
                  break;
                case COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_DEQUEUED:
                  queueMetricsSeen.set(compactionPriorityQueueDequeuedBit, true);
                  break;
                case COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED:
                  queueMetricsSeen.set(compactionPriorityQueueRejectedBit, true);
                  break;
                case COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_PRIORITY:
                  queueMetricsSeen.set(compactionPriorityQueuePriorityBit, true);
                  break;
                case COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_SIZE:
                  queueMetricsSeen.set(compactionPriorityQueueSizeBit, true);
                  break;
                default:
                  break;
              }
            } else {
              // completely unexpected metric
              fail("Found accumulo metric not in expectedMetricNames or flakyMetricNames: "
                  + metric);
            }
          });
      log.debug("METRICS: metrics expected, but not seen so far: {}", expectedMetrics);
      Thread.sleep(4_000);
    }
    assertTrue(expectedMetrics.isEmpty(),
        "Did not see all expected metric names, missing: " + expectedMetrics);

    workerThread.join();
    assertNull(error.get());
    cluster.stop();
  }

  @Test
  public void testFateExecutorMetrics() throws Exception {
    // Tests metrics for Fate's thread pools. Tests that metrics are seen as expected, and config
    // changes to the thread pools are accurately reflected in the metrics. This includes checking
    // that old thread pool metrics are removed, new ones are created, size changes to thread
    // pools are reflected, and the ops assigned and instance type tags are seen as expected
    final String table = getUniqueNames(1)[0];

    // prevent any system initiated fate operations from running, which may interfere with our
    // metrics gathering
    getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);
    try {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
        client.tableOperations().create(table);

        SortedSet<Text> splits = new TreeSet<>();
        splits.add(new Text("foo"));
        // initiate 1 USER fate op which will execute slowly
        client.tableOperations().addSplits(table, splits);
        // initiate 1 META fate op which will execute slowly
        client.tableOperations().addSplits(SystemTables.METADATA.tableName(), splits);

        // let metrics build up
        Thread.sleep(TestStatsDRegistryFactory.pollingFrequency.toMillis() * 3);

        boolean sawExpectedTotalThreadsUserMetric = false;
        boolean sawExpectedTotalThreadsMetaMetric = false;
        boolean sawExpectedInactiveUserThreads = false;
        boolean sawExpectedInactiveMetaThreads = false;
        // For each FATE instance, should see at least one metric for the following:
        // inactive = configured size - num fate ops initiated = configured size - 1
        // total = configured size
        for (var line : sink.getLines()) {
          TestStatsDSink.Metric metric = TestStatsDSink.parseStatsDMetric(line);
          // if the metric is not one of the fate executor metrics...
          if (!metric.getName().equals(FATE_OPS_THREADS_INACTIVE.getName())
              && !metric.getName().equals(FATE_OPS_THREADS_TOTAL.getName())) {
            continue;
          }
          var tags = metric.getTags();
          var instanceType = FateInstanceType
              .valueOf(tags.get(FateExecutorMetrics.INSTANCE_TYPE_TAG_KEY).toUpperCase());
          var opsAssigned = tags.get(FateExecutorMetrics.OPS_ASSIGNED_TAG_KEY);

          verifyFateMetricTags(opsAssigned, instanceType);

          if (metric.getName().equals(FATE_OPS_THREADS_TOTAL.getName())
              && numFateThreadsPool1 == Integer.parseInt(metric.getValue())) {
            if (instanceType == FateInstanceType.USER) {
              sawExpectedTotalThreadsUserMetric = true;
            } else if (instanceType == FateInstanceType.META) {
              sawExpectedTotalThreadsMetaMetric = true;
            }
          } else if (metric.getName().equals(FATE_OPS_THREADS_INACTIVE.getName())
              && (Integer.parseInt(metric.getValue()) == numFateThreadsPool1 - 1)) {
            if (instanceType == FateInstanceType.USER) {
              sawExpectedInactiveUserThreads = true;
            } else if (instanceType == FateInstanceType.META) {
              sawExpectedInactiveMetaThreads = true;
            }
          }
        }
        assertTrue(sawExpectedInactiveUserThreads);
        assertTrue(sawExpectedInactiveMetaThreads);
        assertTrue(sawExpectedTotalThreadsUserMetric);
        assertTrue(sawExpectedTotalThreadsMetaMetric);

        // Now change the config from:
        // {<all fate ops>: numFateThreadsPool1}
        // ->
        // {<all fate ops except split>: numFateThreadsPool2,
        // <split operation>: numFateThreadsPool3}
        changeFateConfig(client, FateInstanceType.USER);
        changeFateConfig(client, FateInstanceType.META);

        // Allow FATE config changes to be picked up. Will take at most POOL_WATCHER_DELAY to
        // commence, provide a buffer to allow to complete.
        Thread.sleep(Fate.POOL_WATCHER_DELAY.toMillis() + 5_000);
        // sink metrics, expect from this point onward that we will no longer see metrics for the
        // old pool (pool1), only metrics for new pools (pool2 and pool3)
        sink.getLines();

        // let metrics build back up
        Thread.sleep(TestStatsDRegistryFactory.pollingFrequency.toMillis() * 3);

        boolean sawAnyMetricPool1 = false;

        boolean sawExpectedTotalThreadsUserMetricPool2 = false;
        boolean sawExpectedTotalThreadsMetaMetricPool2 = false;
        boolean sawExpectedInactiveUserThreadsPool2 = false;
        boolean sawExpectedInactiveMetaThreadsPool2 = false;

        boolean sawExpectedTotalThreadsUserMetricPool3 = false;
        boolean sawExpectedTotalThreadsMetaMetricPool3 = false;
        boolean sawExpectedInactiveUserThreadsPool3 = false;
        boolean sawExpectedInactiveMetaThreadsPool3 = false;
        for (var line : sink.getLines()) {
          TestStatsDSink.Metric metric = TestStatsDSink.parseStatsDMetric(line);
          // if the metric is not one of the fate executor metrics...
          if (!metric.getName().equals(FATE_OPS_THREADS_INACTIVE.getName())
              && !metric.getName().equals(FATE_OPS_THREADS_TOTAL.getName())) {
            continue;
          }
          var tags = metric.getTags();
          var instanceType = FateInstanceType
              .valueOf(tags.get(FateExecutorMetrics.INSTANCE_TYPE_TAG_KEY).toUpperCase());
          var opsAssigned = tags.get(FateExecutorMetrics.OPS_ASSIGNED_TAG_KEY);

          verifyFateMetricTags(opsAssigned, instanceType);

          Set<Fate.FateOperation> fateOpsFromMetric = gatherFateOpsFromTag(opsAssigned);

          if (fateOpsFromMetric.equals(Fate.FateOperation.getAllUserFateOps())
              || fateOpsFromMetric.equals(Fate.FateOperation.getAllMetaFateOps())) {
            sawAnyMetricPool1 = true;
          } else if (fateOpsFromMetric.equals(Arrays.stream(Fate.FateOperation.values())
              .filter(fo -> !fo.equals(SlowFateSplitManager.SLOW_OP)).collect(Collectors.toSet()))
              && numFateThreadsPool2 == Integer.parseInt(metric.getValue())) {
            // pool2
            // total = inactive = size pool2
            if (metric.getName().equals(FATE_OPS_THREADS_INACTIVE.getName())) {
              if (instanceType == FateInstanceType.USER) {
                sawExpectedInactiveUserThreadsPool2 = true;
              } else if (instanceType == FateInstanceType.META) {
                sawExpectedInactiveMetaThreadsPool2 = true;
              }
            } else if (metric.getName().equals(FATE_OPS_THREADS_TOTAL.getName())) {
              if (instanceType == FateInstanceType.USER) {
                sawExpectedTotalThreadsUserMetricPool2 = true;
              } else if (instanceType == FateInstanceType.META) {
                sawExpectedTotalThreadsMetaMetricPool2 = true;
              }
            }
          } else if (fateOpsFromMetric.equals(Set.of(Fate.FateOperation.TABLE_SPLIT))
              && numFateThreadsPool3 == Integer.parseInt(metric.getValue())) {
            // pool3
            // total = inactive = size pool3
            if (metric.getName().equals(FATE_OPS_THREADS_INACTIVE.getName())) {
              if (instanceType == FateInstanceType.USER) {
                sawExpectedInactiveUserThreadsPool3 = true;
              } else if (instanceType == FateInstanceType.META) {
                sawExpectedInactiveMetaThreadsPool3 = true;
              }
            } else if (metric.getName().equals(FATE_OPS_THREADS_TOTAL.getName())) {
              if (instanceType == FateInstanceType.USER) {
                sawExpectedTotalThreadsUserMetricPool3 = true;
              } else if (instanceType == FateInstanceType.META) {
                sawExpectedTotalThreadsMetaMetricPool3 = true;
              }
            }
          }
        }

        assertFalse(sawAnyMetricPool1);
        assertTrue(sawExpectedTotalThreadsUserMetricPool2);
        assertTrue(sawExpectedTotalThreadsMetaMetricPool2);
        assertTrue(sawExpectedInactiveUserThreadsPool2);
        assertTrue(sawExpectedInactiveMetaThreadsPool2);
        assertTrue(sawExpectedTotalThreadsUserMetricPool3);
        assertTrue(sawExpectedTotalThreadsMetaMetricPool3);
        assertTrue(sawExpectedInactiveUserThreadsPool3);
        assertTrue(sawExpectedInactiveMetaThreadsPool3);
      }
    } finally {
      getCluster().getClusterControl().startAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().startAllServers(ServerType.GARBAGE_COLLECTOR);
    }
  }

  /**
   * Verifies what should always be true for fate metrics tags: The ops assigned tag should include
   * all the ops that are associated with a pool
   */
  private void verifyFateMetricTags(String opsAssignedInMetric, FateInstanceType type) {
    var opsAssignedInConfig =
        Fate.getPoolConfigurations(getCluster().getServerContext().getConfiguration(), type);
    assertNotNull(opsAssignedInConfig);

    var fateOpsFromMetric = gatherFateOpsFromTag(opsAssignedInMetric);

    assertNotNull(opsAssignedInConfig.get(fateOpsFromMetric));
  }

  private void changeFateConfig(AccumuloClient client, FateInstanceType type) throws Exception {
    Set<Fate.FateOperation> allFateOps = null;
    if (type == FateInstanceType.USER) {
      allFateOps = new HashSet<>(Fate.FateOperation.getAllUserFateOps());
    } else if (type == FateInstanceType.META) {
      allFateOps = new HashSet<>(Fate.FateOperation.getAllMetaFateOps());
    }
    assertNotNull(allFateOps);
    allFateOps.remove(SlowFateSplitManager.SLOW_OP);
    String newFateConfig =
        "{'" + allFateOps.stream().map(Enum::name).collect(Collectors.joining(",")) + "': "
            + numFateThreadsPool2 + ",'" + SlowFateSplitManager.SLOW_OP.name() + "': "
            + numFateThreadsPool3 + "}";
    newFateConfig = newFateConfig.replace("'", "\"");

    if (type == FateInstanceType.USER) {
      client.instanceOperations().setProperty(Property.MANAGER_FATE_USER_CONFIG.getKey(),
          newFateConfig);
    } else if (type == FateInstanceType.META) {
      client.instanceOperations().setProperty(Property.MANAGER_FATE_META_CONFIG.getKey(),
          newFateConfig);
    }
  }

  private Set<Fate.FateOperation> gatherFateOpsFromTag(String opsAssigned) {
    String[] ops = opsAssigned.split("\\.");
    Set<Fate.FateOperation> fateOpsFromMetric = new HashSet<>();
    for (var op : ops) {
      fateOpsFromMetric.add(Fate.FateOperation.valueOf(op.toUpperCase()));
    }
    return fateOpsFromMetric;
  }

  static void doWorkToGenerateMetrics(AccumuloClient client, Class<?> testClass) throws Exception {
    String tableName = testClass.getSimpleName();
    client.tableOperations().create(tableName);
    SortedSet<Text> splits = new TreeSet<>(List.of(new Text("5")));
    client.tableOperations().addSplits(tableName, splits);
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
    // Start a compaction with the slow iterator to ensure that the compaction queues
    // are not removed quickly
    CompactionConfig cc = new CompactionConfig();
    IteratorSetting is = new IteratorSetting(100, "slow", SlowIterator.class);
    SlowIterator.setSleepTime(is, 3000);
    cc.setIterators(List.of(is));
    cc.setWait(false);
    client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
    client.tableOperations().delete(tableName);
    while (client.tableOperations().exists(tableName)) {
      Thread.sleep(1000);
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // unused; this class only extends MetricsProducer to easily reference its methods/constants
  }

  @Test
  public void metricTags() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      doWorkToGenerateMetrics(client, getClass());
    }
    cluster.stop();

    List<String> statsDMetrics;

    // loop until we run out of lines or until we see all expected metrics
    while (!(statsDMetrics = sink.getLines()).isEmpty()) {
      statsDMetrics.stream().filter(line -> line.startsWith("accumulo"))
          .map(TestStatsDSink::parseStatsDMetric).forEach(a -> {
            var t = a.getTags();
            log.info("METRICS, received from statsd - name: '{}' num tags: {}, tags: {} = {}",
                a.getName(), t.size(), t, a.getValue());
            // check hostname is always set and is valid
            assertNotEquals(ConfigOpts.BIND_ALL_ADDRESSES,
                a.getTags().get(MetricsInfo.HOST_TAG_KEY));
            assertNotNull(a.getTags().get(MetricsInfo.INSTANCE_NAME_TAG_KEY));

            assertNotNull(a.getTags().get(MetricsInfo.PROCESS_NAME_TAG_KEY));

            // check resource.group tag exists
            assertNotNull(a.getTags().get(MetricsInfo.RESOURCE_GROUP_TAG_KEY));

            // check that the user tags are present
            assertEquals("value1", a.getTags().get("tag1"));
            assertEquals("value2", a.getTags().get("tag2"));

            // check the length of the tag value is sane
            final int MAX_EXPECTED_TAG_LEN = 128;
            a.getTags().forEach((k, v) -> assertTrue(v.length() < MAX_EXPECTED_TAG_LEN));
          });
    }
  }

  @Test
  public void fateMetrics() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      doWorkToGenerateMetrics(client, getClass());
    }
    cluster.stop();

    List<String> statsDMetrics;

    while (!(statsDMetrics = sink.getLines()).isEmpty()) {
      statsDMetrics.stream().filter(line -> line.startsWith("accumulo.fate.tx"))
          .map(TestStatsDSink::parseStatsDMetric).forEach(a -> {
            var t = a.getTags();
            log.debug("METRICS, received from statsd - name: '{}' num tags: {}, tags: {} = {}",
                a.getName(), t.size(), t, a.getValue());

            // Verify the fate metrics contain state and instanceType
            // Checking the value would be hard to test because the metrics are updated on a timer
            // and fate transactions get cleaned up when finished so the current state is a bit
            // non-deterministic
            TStatus status = TStatus.valueOf(a.getTags().get("state").toUpperCase());
            assertNotNull(status);
            FateInstanceType type =
                FateInstanceType.valueOf(a.getTags().get("instanceType").toUpperCase());
            assertNotNull(type);
          });
    }
  }
}

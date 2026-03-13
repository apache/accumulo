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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.metrics.MetricsIT;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class MultipleManagerMetricsIT extends ConfigurableMacBase {

  private static final Logger LOG = LoggerFactory.getLogger(MultipleManagerMetricsIT.class);
  private static TestStatsDSink sink;

  private final AtomicReference<String> currentAssistant = new AtomicReference<>();
  private final AtomicReference<String> currentPrimary = new AtomicReference<>();

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Set this lower so that locks timeout faster
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MIN_COUNT, "2");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MAX_WAIT, "5m");
    cfg.getClusterServerConfiguration().setNumManagers(2);

    // Tell the server processes to use a StatsDMeterRegistry and the simple logging registry
    // that will be configured to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_CACHE_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, TestStatsDRegistryFactory.class.getName());
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);

    super.configure(cfg, hadoopCoreSite);
  }

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
  }

  private String getPrimaryFromZK() {
    Set<ServerId> primaries =
        getCluster().getServerContext().instanceOperations().getServers(ServerId.Type.MANAGER);
    if (primaries == null || primaries.isEmpty()) {
      return null;
    }
    assertEquals(1, primaries.size());
    ServerId primary = primaries.iterator().next();
    return primary.toHostPortString();
  }

  private void killPrimary() throws KeeperException, InterruptedException {
    ServiceLockPath slp = getCluster().getServerContext().getServerPaths().getManager(true);
    ServiceLock.deleteLock(getCluster().getServerContext().getZooSession().asReaderWriter(), slp);
    // We removed the lock in ZooKeeper, we need to refresh the processes in Mini.
    // If we don't do this, then it will still think that 2 are running and a
    // subsequent call to start() will do nothing.
    while (getCluster().getClusterControl().getProcesses(ServerType.MANAGER).size() != 1) {
      Thread.sleep(250);
      getCluster().getClusterControl().refreshProcesses(ServerType.MANAGER);
    }
  }

  private void processMetrics() throws InterruptedException {
    // Dump the stored metrics
    sink.getLines();
    // The TestStatsDSink polling frequency is 3s, wait 10s.
    Thread.sleep(10_000);
    List<String> statsDMetrics = sink.getLines();

    // Figure out current primary
    List<TestStatsDSink.Metric> primaryMetrics = statsDMetrics.stream()
        .filter(line -> line.startsWith("accumulo")).map(TestStatsDSink::parseStatsDMetric)
        .filter(m -> m.getTags().get(MetricsInfo.PROCESS_NAME_TAG_KEY)
            .equals(ServerId.Type.MANAGER.name()))
        .filter(m -> m.getTags().get(Manager.PRIMARY_TAG_KEY).equals("true")).toList();
    if (primaryMetrics.size() > 0) {
      TestStatsDSink.Metric last = primaryMetrics.get(primaryMetrics.size() - 1);
      LOG.info("Last primary metric found: {}", last);
      String host = last.getTags().get(MetricsInfo.HOST_TAG_KEY);
      String port = last.getTags().get(MetricsInfo.PORT_TAG_KEY);
      if (host != null && port != null) {
        currentPrimary.set(HostAndPort.fromParts(host, Integer.parseInt(port)).toString());
      }
    }

    // Figure out current assistant
    List<TestStatsDSink.Metric> assistantMetrics = statsDMetrics.stream()
        .filter(line -> line.startsWith("accumulo")).map(TestStatsDSink::parseStatsDMetric)
        .filter(m -> m.getTags().get(MetricsInfo.PROCESS_NAME_TAG_KEY)
            .equals(ServerId.Type.MANAGER.name()))
        .filter(m -> m.getTags().get(Manager.PRIMARY_TAG_KEY).equals("false")).toList();
    if (assistantMetrics.size() > 0) {
      TestStatsDSink.Metric last = assistantMetrics.get(assistantMetrics.size() - 1);
      LOG.info("Last assistant metric found: {}", last);
      String host = last.getTags().get(MetricsInfo.HOST_TAG_KEY);
      String port = last.getTags().get(MetricsInfo.PORT_TAG_KEY);
      if (host != null && port != null) {
        String assistant = HostAndPort.fromParts(host, Integer.parseInt(port)).toString();
        if (!assistant.equals(currentPrimary.get())) {
          currentAssistant.set(assistant);
        }
      }
    }
  }

  @Test
  public void testPrimaryManagerTagChanges() throws Exception {

    String previousPrimary = null;
    String previousAssistant = null;

    for (int i = 0; i < 5; i++) {
      // Wait for primary lock to be acquired
      LOG.info("Waiting for primary manager lock");
      Wait.waitFor(() -> getPrimaryFromZK() != null, 60_000);

      String primary = getPrimaryFromZK();
      LOG.info("Primary manager: {}", primary);

      // Check that the primary address switched
      if (previousPrimary != null) {
        assertNotEquals(previousPrimary, primary, "Current primary is equal to prior primary");
        LOG.info("Primary manager address has changed, starting killed Manager");
        getCluster().start();
      }

      // Wait for primary metric to match current primary
      final String tmp = primary;
      LOG.info("Waiting for metric to equal current primary");
      Wait.waitFor(() -> {
        processMetrics();
        return currentPrimary.get() != null && currentPrimary.get().equals(tmp);
      }, 60_000, 10_000);

      // Check that the new primary is the prior assistant
      if (previousAssistant != null) {
        final String tmpAssistant = previousAssistant;
        Wait.waitFor(() -> {
          processMetrics();
          return !currentAssistant.get().equals(tmpAssistant);
        }, 60_000, 10_000);
        assertEquals(previousAssistant, primary,
            "Current primary is not equal to the prior assistant");
      } else {
        Wait.waitFor(() -> {
          processMetrics();
          return currentAssistant.get() != null;
        }, 60_000, 10_000);
      }
      previousAssistant = currentAssistant.get();

      // Kill the Primary
      previousPrimary = primary;
      LOG.info("Killing primary manager: {}", primary);
      killPrimary();
    }
  }

  @Test
  public void testMetricsPublishedOnNewPrimary() throws Exception {

    // Wait for primary lock to be acquired
    LOG.info("Waiting for primary manager lock");
    Wait.waitFor(() -> getPrimaryFromZK() != null, 60_000);

    String primary = getPrimaryFromZK();
    LOG.info("Primary manager: {}", primary);

    // Wait for primary metric to match current primary
    final String tmp = primary;
    LOG.info("Waiting for metric to equal current primary");
    Wait.waitFor(() -> {
      processMetrics();
      return currentPrimary.get() != null && currentPrimary.get().equals(tmp);
    }, 60_000, 10_000);

    Wait.waitFor(() -> {
      processMetrics();
      return currentAssistant.get() != null;
    }, 60_000, 10_000);
    String previousAssistant = currentAssistant.get();

    confirmMetricsPublished();

    // Kill the Primary
    LOG.info("Killing primary manager: {}", primary);
    killPrimary();

    // Wait for primary lock to be acquired
    LOG.info("Waiting for primary manager lock");
    Wait.waitFor(() -> getPrimaryFromZK() != null, 60_000);

    primary = getPrimaryFromZK();
    LOG.info("Primary manager: {}", primary);

    // Wait for primary metric to match current primary
    final String tmp2 = primary;
    LOG.info("Waiting for metric to equal current primary");
    Wait.waitFor(() -> {
      processMetrics();
      return currentPrimary.get() != null && currentPrimary.get().equals(tmp2);
    }, 60_000, 10_000);

    getCluster().start();

    final String tmpAssistant = previousAssistant;
    Wait.waitFor(() -> {
      processMetrics();
      return !currentAssistant.get().equals(tmpAssistant);
    }, 60_000, 10_000);
    assertEquals(previousAssistant, primary, "Current primary is not equal to the prior assistant");

    confirmMetricsPublished();

  }

  public void confirmMetricsPublished() throws Exception {

    Set<Metric> expectedMetrics = new HashSet<>(Arrays.asList(Metric.values()));
    expectedMetrics.removeAll(MetricsIT.flakyMetrics); // might not see these
    expectedMetrics.removeAll(MetricsIT.unexpectedMetrics); // definitely shouldn't see these
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
        MetricsIT.doWorkToGenerateMetrics(client, getClass());
      } catch (Exception e) {
        error.set(e);
      }
    });
    workerThread.start();

    // Wait for metrics to build up
    Thread.sleep(30_000);

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
            } else if (MetricsIT.flakyMetrics.contains(metric)) {
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
  }

}

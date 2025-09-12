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

import static org.apache.accumulo.core.conf.Property.TABLE_FILE_MAX;
import static org.apache.accumulo.core.conf.Property.TABLE_MAJC_RATIO;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_CANCELLED;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_COMPLETED;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_FAILED;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_FAILURES_CONSECUTIVE;
import static org.apache.accumulo.core.metrics.Metric.COMPACTOR_MAJC_FAILURES_TERMINATION;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassLoaderContextCompactionIT extends AccumuloClusterHarness {

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderContextCompactionIT.class);
  private static TestStatsDSink sink;

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    // After 1 failure start backing off by 5s.
    // After 3 failures, terminate the Compactor
    cfg.setProperty(Property.COMPACTOR_FAILURE_BACKOFF_THRESHOLD, "1");
    cfg.setProperty(Property.COMPACTOR_FAILURE_BACKOFF_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTOR_FAILURE_BACKOFF_RESET, "10m");
    cfg.setProperty(Property.COMPACTOR_FAILURE_TERMINATION_THRESHOLD, "3");
    cfg.getClusterServerConfiguration().setNumDefaultCompactors(2);
    cfg.getClusterServerConfiguration().addCompactorResourceGroup(GROUP1, 1);
    // Tell the server processes to use a StatsDMeterRegistry and the simple logging registry
    // that will be configured to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.setProperty("general.custom.metrics.opts.logging.step", "1s");
    String clazzList = LoggingMeterRegistryFactory.class.getName() + ","
        + TestStatsDRegistryFactory.class.getName();
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, clazzList);
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

  @Test
  public void testClassLoaderContextErrorKillsCompactor() throws Exception {

    final AtomicBoolean shutdownTailer = new AtomicBoolean(false);
    final AtomicLong cancellations = new AtomicLong(0);
    final AtomicLong completions = new AtomicLong(0);
    final AtomicLong failures = new AtomicLong(0);
    final AtomicLong consecutive = new AtomicLong(0);
    final AtomicLong terminations = new AtomicLong(0);

    final Thread thread = Threads.createNonCriticalThread("metric-tailer", () -> {
      while (!shutdownTailer.get()) {
        List<String> statsDMetrics = sink.getLines();
        for (String s : statsDMetrics) {
          if (shutdownTailer.get()) {
            break;
          }
          if (s.startsWith(COMPACTOR_MAJC_CANCELLED.getName())) {
            Metric m = TestStatsDSink.parseStatsDMetric(s);
            if (m.getTags().containsKey("resource.group")
                && m.getTags().get("resource.group").equals(GROUP1)) {
              LOG.info("{}", m);
              cancellations.set(Long.parseLong(m.getValue()));
            }
          } else if (s.startsWith(COMPACTOR_MAJC_COMPLETED.getName())) {
            Metric m = TestStatsDSink.parseStatsDMetric(s);
            if (m.getTags().containsKey("resource.group")
                && m.getTags().get("resource.group").equals(GROUP1)) {
              LOG.info("{}", m);
              completions.set(Long.parseLong(m.getValue()));
            }
          } else if (s.startsWith(COMPACTOR_MAJC_FAILED.getName())) {
            Metric m = TestStatsDSink.parseStatsDMetric(s);
            if (m.getTags().containsKey("resource.group")
                && m.getTags().get("resource.group").equals(GROUP1)) {
              LOG.info("{}", m);
              failures.set(Long.parseLong(m.getValue()));
            }
          } else if (s.startsWith(COMPACTOR_MAJC_FAILURES_TERMINATION.getName())) {
            Metric m = TestStatsDSink.parseStatsDMetric(s);
            if (m.getTags().containsKey("resource.group")
                && m.getTags().get("resource.group").equals(GROUP1)) {
              LOG.info("{}", m);
              terminations.set(Long.parseLong(m.getValue()));
            }
          } else if (s.startsWith(COMPACTOR_MAJC_FAILURES_CONSECUTIVE.getName())) {
            Metric m = TestStatsDSink.parseStatsDMetric(s);
            if (m.getTags().containsKey("resource.group")
                && m.getTags().get("resource.group").equals(GROUP1)) {
              LOG.info("{}", m);
              consecutive.set(Long.parseLong(m.getValue()));
            }
          }

        }
      }
    });
    thread.start();

    final String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Wait.waitFor(() -> ExternalCompactionUtil.countCompactors(ResourceGroupId.of(GROUP1),
          (ClientContext) client) == 1);
      Set<ServerId> compactors =
          ExternalCompactionUtil.getCompactorAddrs((ClientContext) client).get(GROUP1);
      assertEquals(1, compactors.size());
      final ServerId compactorAddr = compactors.iterator().next();
      createTable(client, table1, "cs1");
      client.tableOperations().setProperty(table1, TABLE_FILE_MAX.getKey(), "1001");
      client.tableOperations().setProperty(table1, TABLE_MAJC_RATIO.getKey(), "1001");
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(table1));

      ReadWriteIT.ingest(client, 1000, 1, 1, 0, "colf", table1, 20);

      Ample ample = ((ClientContext) client).getAmple();
      try (
          TabletsMetadata tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build()) {
        TabletMetadata tm = tms.iterator().next();
        assertEquals(50, tm.getFiles().size());
      }

      final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
      final FileSystem fs = cluster.getFileSystem();

      // Create the context directory in HDFS
      final org.apache.hadoop.fs.Path contextDir = fs.makeQualified(new org.apache.hadoop.fs.Path(
          cluster.getConfig().getAccumuloDir().toString(), "classpath"));
      assertTrue(fs.mkdirs(contextDir));

      // Copy the FooFilter.jar to the context dir
      final org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(
          System.getProperty("java.io.tmpdir") + "/classes/org/apache/accumulo/test/FooFilter.jar");
      final org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(contextDir, "Test.jar");
      fs.copyFromLocalFile(src, dst);
      assertTrue(fs.exists(dst));

      // Set the context on the table
      client.tableOperations().setProperty(table1, Property.TABLE_CLASSLOADER_CONTEXT.getKey(),
          dst.toUri().toString());

      final IteratorSetting cfg =
          new IteratorSetting(101, "FooFilter", "org.apache.accumulo.test.FooFilter");
      client.tableOperations().attachIterator(table1, cfg, EnumSet.of(IteratorScope.majc));

      // delete Test.jar, so that the classloader will fail
      assertTrue(fs.delete(dst, false));

      assertEquals(0, cancellations.get());
      assertEquals(0, completions.get());
      assertEquals(0, failures.get());
      assertEquals(0, terminations.get());
      assertEquals(0, consecutive.get());

      // Start a compaction. The missing jar should cause a failure
      client.tableOperations().compact(table1, new CompactionConfig().setWait(false));
      Wait.waitFor(
          () -> ExternalCompactionUtil.getRunningCompaction(compactorAddr, (ClientContext) client)
              == null);
      assertEquals(1, ExternalCompactionUtil.countCompactors(ResourceGroupId.of(GROUP1),
          (ClientContext) client));
      Wait.waitFor(() -> failures.get() == 1);
      Wait.waitFor(() -> consecutive.get() == 1);

      Wait.waitFor(() -> failures.get() == 0);
      client.tableOperations().compact(table1, new CompactionConfig().setWait(false));
      Wait.waitFor(
          () -> ExternalCompactionUtil.getRunningCompaction(compactorAddr, (ClientContext) client)
              == null);
      assertEquals(1, ExternalCompactionUtil.countCompactors(ResourceGroupId.of(GROUP1),
          (ClientContext) client));
      Wait.waitFor(() -> failures.get() == 1);
      Wait.waitFor(() -> consecutive.get() == 2);

      Wait.waitFor(() -> failures.get() == 0);
      client.tableOperations().compact(table1, new CompactionConfig().setWait(false));
      Wait.waitFor(
          () -> ExternalCompactionUtil.getRunningCompaction(compactorAddr, (ClientContext) client)
              == null);
      assertEquals(1, ExternalCompactionUtil.countCompactors(ResourceGroupId.of(GROUP1),
          (ClientContext) client));
      Wait.waitFor(() -> failures.get() == 1);
      Wait.waitFor(() -> consecutive.get() == 3);

      // Three failures have occurred, Compactor should shut down.
      Wait.waitFor(() -> ExternalCompactionUtil.countCompactors(ResourceGroupId.of(GROUP1),
          (ClientContext) client) == 0);
      Wait.waitFor(() -> terminations.get() == 1);
      assertEquals(0, cancellations.get());
      assertEquals(0, completions.get());

    } finally {
      shutdownTailer.set(true);
      thread.join();
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
    }

  }

}

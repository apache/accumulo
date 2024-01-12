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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.metadata.UnreferencedTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.functional.CompactionIT;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

// Use the shared Mini Cluster for all metric tests as Compaction Resources can be spun up and down per test.
public class CompactionPriorityQueueMetricsIT extends SharedMiniClusterBase {

  public static final Logger log = LoggerFactory.getLogger(CompactionPriorityQueueMetricsIT.class);

  private static TestStatsDSink sink;
  private String tableName;
  private TableId tableId;
  private AccumuloConfiguration aconf;
  private FileSystem fs;
  private String rootPath;

  public static final String QUEUE1 = "METRICSQ1";
  public static final String QUEUE1_METRIC_LABEL = "e." + MetricsUtil.formatString(QUEUE1);
  public static final String QUEUE1_SERVICE = "Q1";
  public static final int QUEUE1_SIZE = 6;

  @BeforeEach
  public void setupMetricsTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      tableName = getUniqueNames(1)[0];

      Map<String,String> props =
          Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
              "table.compaction.dispatcher.opts.service", QUEUE1_SERVICE);
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);
      c.tableOperations().create(tableName, ntc);

      tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));
      aconf = getCluster().getServerContext().getConfiguration();
      fs = getCluster().getFileSystem();
      rootPath = getCluster().getTemporaryPath().toString();
    }
  }

  private String getDir(String testName) throws Exception {
    String dir = rootPath + testName + getUniqueNames(1)[0];
    fs.delete(new Path(dir), true);
    return dir;
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @BeforeAll
  public static void setup() throws Exception {
    sink = new TestStatsDSink();
    SharedMiniClusterBase.startMiniClusterWithConfig(new CompactionPriorityQueueMetricsITConfig());
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
    if (sink != null) {
      sink.close();
    }
  }

  public static class CompactionPriorityQueueMetricsITConfig
      implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration conf) {
      cfg.setMemory(ServerType.TABLET_SERVER, 512, MemoryUnit.MEGABYTE);
      // Zero the default compactors
      cfg.getClusterServerConfiguration().setNumDefaultCompactors(0);

      // Create a new queue with zero compactors.
      cfg.setProperty("tserver.compaction.major.service." + QUEUE1_SERVICE + ".planner",
          DefaultCompactionPlanner.class.getName());
      cfg.setProperty("tserver.compaction.major.service." + QUEUE1_SERVICE + ".planner.opts.groups",
          "[{'name':'" + QUEUE1 + "'}]");

      cfg.setProperty(Property.MANAGER_COMPACTION_SERVICE_PRIORITY_QUEUE_SIZE, "6");
      cfg.getClusterServerConfiguration().addCompactorResourceGroup(QUEUE1, 0);

      // use raw local file system
      conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
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

  private String writeData(String file, AccumuloConfiguration aconf, int s, int e)
      throws Exception {
    FileSystem fs = getCluster().getFileSystem();
    String filename = file + RFile.EXTENSION;
    try (FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder()
        .forFile(UnreferencedTabletFile.of(fs, new Path(filename)), fs, fs.getConf(),
            NoCryptoServiceFactory.NONE)
        .withTableConfiguration(aconf).build()) {
      writer.startDefaultLocalityGroup();
      for (int i = s; i <= e; i++) {
        writer.append(new Key(new Text(row(i))), new Value(Integer.toString(i)));
      }
    }

    return hash(filename);
  }

  private void addSplits(AccumuloClient client, String tableName, String splitString)
      throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    for (String split : splitString.split(" ")) {
      splits.add(new Text(split));
    }
    client.tableOperations().addSplits(tableName, splits);
  }

  private void verifyData(AccumuloClient client, String table, int start, int end, boolean setTime)
      throws Exception {
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {

      Iterator<Map.Entry<Key,Value>> iter = scanner.iterator();

      for (int i = start; i <= end; i++) {
        if (!iter.hasNext()) {
          throw new Exception("row " + i + " not found");
        }

        Map.Entry<Key,Value> entry = iter.next();

        String row = String.format("%04d", i);

        if (!entry.getKey().getRow().equals(new Text(row))) {
          throw new Exception("unexpected row " + entry.getKey() + " " + i);
        }

        if (Integer.parseInt(entry.getValue().toString()) != i) {
          throw new Exception("unexpected value " + entry + " " + i);
        }

        if (setTime) {
          assertEquals(1L, entry.getKey().getTimestamp());
        }
      }

      if (iter.hasNext()) {
        throw new Exception("found more than expected " + iter.next());
      }
    }
  }

  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "WEAK_MESSAGE_DIGEST_SHA1"},
      justification = "path provided by test; sha-1 is okay for test")
  private String hash(String filename) {
    try {
      byte[] data = Files.readAllBytes(Paths.get(filename.replaceFirst("^file:", "")));
      byte[] hash = MessageDigest.getInstance("SHA1").digest(data);
      return new BigInteger(1, hash).toString(16);
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static String row(int r) {
    return String.format("%04d", r);
  }

  @Test
  public void testQueueMetrics() throws Exception {
    // Metrics collector Thread
    final LinkedBlockingQueue<TestStatsDSink.Metric> queueMetrics = new LinkedBlockingQueue<>();
    final AtomicBoolean shutdownTailer = new AtomicBoolean(false);

    Thread thread = Threads.createThread("metric-tailer", () -> {
      while (!shutdownTailer.get()) {
        List<String> statsDMetrics = sink.getLines();
        for (String s : statsDMetrics) {
          if (shutdownTailer.get()) {
            break;
          }
          if (s.startsWith(MetricsProducer.METRICS_COMPACTOR_PREFIX + "queue")) {
            queueMetrics.add(TestStatsDSink.parseStatsDMetric(s));
          }
        }
      }
    });
    thread.start();

    long highestFileCount = 0L;
    ServerContext context = getCluster().getServerContext();
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String dir = getDir("/testBulkFile-");
      FileSystem fs = getCluster().getFileSystem();
      fs.mkdirs(new Path(dir));

      // Create splits so there are two groupings of tablets with similar file counts.
      List<String> splitPoints =
          List.of("500", "1000", "1500", "2000", "3750", "5500", "7250", "9000");
      for (String splitPoint : splitPoints) {
        addSplits(c, tableName, splitPoint);
      }

      for (int i = 0; i < 100; i++) {
        writeData(dir + "/f" + i + ".", aconf, i * 100, (i + 1) * 100 - 1);
      }
      c.tableOperations().importDirectory(dir).to(tableName).load();

      IteratorSetting iterSetting = new IteratorSetting(100, CompactionIT.TestFilter.class);
      iterSetting.addOption("expectedQ", QUEUE1);
      iterSetting.addOption("modulus", 3 + "");
      CompactionConfig config =
          new CompactionConfig().setIterators(List.of(iterSetting)).setWait(false);
      c.tableOperations().compact(tableName, config);

      try (TabletsMetadata tm = context.getAmple().readTablets().forTable(tableId).build()) {
        // Get each tablet's file sizes
        for (TabletMetadata tablet : tm) {
          long fileSize = tablet.getFiles().size();
          log.info("Number of files in tablet {}: {}", tablet.getExtent().toString(), fileSize);
          highestFileCount = Math.max(highestFileCount, fileSize);
        }
      }
      verifyData(c, tableName, 0, 100 * 100 - 1, false);
    }

    boolean sawMetricsQ1 = false;
    while (!sawMetricsQ1) {
      while (!queueMetrics.isEmpty()) {
        var qm = queueMetrics.take();
        if (qm.getName().contains(MetricsProducer.METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED)
            && qm.getTags().containsValue(QUEUE1_METRIC_LABEL)) {
          if (Integer.parseInt(qm.getValue()) > 0) {
            sawMetricsQ1 = true;
          }
        }
      }
      // Current poll rate of the TestStatsDRegistryFactory is 3 seconds
      // If metrics are not found in the queue, sleep until the next poll.
      UtilWaitThread.sleep(3500);
    }

    // Set lowest priority to the lowest possible system compaction priority
    long lowestPriority = Short.MIN_VALUE;
    long rejectedCount = 0L;
    int queueSize = 0;

    boolean sawQueues = false;
    // An empty queue means that the last known value is the most recent.
    while (!queueMetrics.isEmpty()) {
      var metric = queueMetrics.take();
      if (metric.getName()
          .contains(MetricsProducer.METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED)
          && metric.getTags().containsValue(QUEUE1_METRIC_LABEL)) {
        rejectedCount = Long.parseLong(metric.getValue());
      } else if (metric.getName()
          .contains(MetricsProducer.METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_PRIORITY)
          && metric.getTags().containsValue(QUEUE1_METRIC_LABEL)) {
        lowestPriority = Math.max(lowestPriority, Long.parseLong(metric.getValue()));
      } else if (metric.getName()
          .contains(MetricsProducer.METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_LENGTH)
          && metric.getTags().containsValue(QUEUE1_METRIC_LABEL)) {
        queueSize = Integer.parseInt(metric.getValue());
      } else if (metric.getName().contains(MetricsProducer.METRICS_COMPACTOR_JOB_PRIORITY_QUEUES)) {
        sawQueues = true;
      } else {
        log.debug("{}", metric);
      }
    }

    // Confirm metrics were generated and in some cases, validate contents.
    assertTrue(rejectedCount > 0L);

    // Priority is the file counts + number of compactions for that tablet.
    // The lowestPriority job in the queue should have been
    // at least 1 count higher than the highest file count.
    assertTrue(lowestPriority > highestFileCount);

    // Multiple Queues have been created
    assertTrue(sawQueues);

    // Queue size matches the intended queue size
    assertEquals(QUEUE1_SIZE, queueSize);

    // Start Compactors
    getCluster().getConfig().getClusterServerConfiguration().addCompactorResourceGroup(QUEUE1, 1);
    getCluster().getClusterControl().start(ServerType.COMPACTOR);

    boolean emptyQueue = false;

    // Make sure that metrics added to the queue are recent
    UtilWaitThread.sleep(3500);

    while (!emptyQueue) {
      while (!queueMetrics.isEmpty()) {
        var metric = queueMetrics.take();
        if (metric.getName()
            .contains(MetricsProducer.METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED)
            && metric.getTags().containsValue(QUEUE1_METRIC_LABEL)) {
          if (Integer.parseInt(metric.getValue()) == 0) {
            emptyQueue = true;
          }
        }
      }
      UtilWaitThread.sleep(3500);
    }

    shutdownTailer.set(true);
    thread.join();
  }
}

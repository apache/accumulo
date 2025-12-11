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

import static org.apache.accumulo.core.metrics.MetricsProducer.METRICS_SCAN_EXCEPTIONS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.BadIterator;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanExecutorExceptionsIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(ScanExecutorExceptionsIT.class);
  private static TestStatsDSink sink;
  private static final AtomicLong exceptionCount = new AtomicLong(0);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @BeforeAll
  public static void setup() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void teardown() throws Exception {
    if (sink != null) {
      sink.close();
    }
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty("general.custom.metrics. opts. logging.step", "5s");
    String clazzList = LoggingMeterRegistryFactory.class.getName() + ","
        + TestStatsDRegistryFactory.class.getName();
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, clazzList);
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

  @Test
  public void testScanExecutorExceptions() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      String tableName = getUniqueNames(1)[0];
      log.info("Creating table: {}", tableName);

      // Create table with BadIterator configured for scan scope
      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting badIterSetting = new IteratorSetting(50, "bad", BadIterator.class);
      ntc.attachIterator(badIterSetting, EnumSet.of(IteratorUtil.IteratorScope.scan));
      client.tableOperations().create(tableName, ntc);

      try (BatchWriter writer = client.createBatchWriter(tableName)) {
        for (int i = 0; i < 10; i++) {
          Mutation m = new Mutation(new Text("row" + i));
          m.put("cf", "cq", "value" + i);
          writer.addMutation(m);
        }
      }

      client.tableOperations().flush(tableName, null, null, true);

      log.info("Performing scans to trigger exceptions.. .");

      for (int i = 0; i < 3; i++) {
        performScanWithException(client, tableName);
      }

      for (int i = 0; i < 3; i++) {
        performBatchScanWithException(client, tableName);
      }

      log.info("Waiting for metrics to be published...");
      Thread.sleep(15_000);
      log.info("Collecting metrics from sink...");
      List<String> statsDMetrics = sink.getLines();
      log.info("Received {} metric lines from sink", statsDMetrics.size());
      exceptionCount.set(0);

      int metricsFound = 0;
      for (String line : statsDMetrics) {
        if (line.startsWith(METRICS_SCAN_EXCEPTIONS)) {
          metricsFound++;
          TestStatsDSink.Metric metric = TestStatsDSink.parseStatsDMetric(line);
          log.info("Found scan exception metric: {}", metric);
          String executor = metric.getTags().get("executor");
          if (executor != null) {
            long val = Long.parseLong(metric.getValue());
            exceptionCount.accumulateAndGet(val, Math::max);
            log.info("Recorded exception count for executor '{}': {}", executor, val);
          }
        }
      }

      log.info("Found {} scan exception metrics total", metricsFound);
      long finalCount = exceptionCount.get();
      log.info("Final exception count: {}", finalCount);

      assertTrue(finalCount > 0, "Should have tracked exceptions, but count was: " + finalCount);
    }
  }

  private void performScanWithException(AccumuloClient client, String table) {
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      // Set a timeout to avoid hanging forever
      scanner.setTimeout(5, java.util.concurrent.TimeUnit.SECONDS);
      scanner.forEach((k, v) -> {
        // This should never be reached because the iterator throws an exception
      });
    } catch (Exception e) {
      log.debug("Expected exception from regular scan on table {}: {}", table, e.getMessage());
    }
  }

  private void performBatchScanWithException(AccumuloClient client, String table) {
    try (BatchScanner batchScanner = client.createBatchScanner(table, Authorizations.EMPTY, 2)) {
      batchScanner.setTimeout(5, java.util.concurrent.TimeUnit.SECONDS);
      batchScanner.setRanges(List.of(new Range("row0", "row5"), new Range("row6", "row9")));
      batchScanner.forEach((k, v) -> {
        // This should never be reached because the iterator throws an exception
      });
    } catch (Exception e) {
      log.debug("Expected exception from batch scan on table {}: {}", table, e.getMessage());
    }
  }
}

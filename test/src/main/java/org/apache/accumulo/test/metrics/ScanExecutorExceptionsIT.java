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
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ErrorThrowingIterator;
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
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty("general.custom.metrics.opts.logging.step", "5s");
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

      final int numEntries = 10;
      final int totalExceptions = 6;

      NewTableConfiguration ntc = new NewTableConfiguration();
      IteratorSetting errorIterSetting =
          new IteratorSetting(50, "error", ErrorThrowingIterator.class);
      errorIterSetting.addOption(ErrorThrowingIterator.TIMES, String.valueOf(totalExceptions));
      ntc.attachIterator(errorIterSetting, EnumSet.of(IteratorUtil.IteratorScope.scan));
      client.tableOperations().create(tableName, ntc);

      try (BatchWriter writer = client.createBatchWriter(tableName)) {
        for (int i = 0; i < numEntries; i++) {
          Mutation m = new Mutation(new Text("row" + i));
          m.put("cf", "cq", "value" + i);
          writer.addMutation(m);
        }
      }

      client.tableOperations().flush(tableName, null, null, true);

      log.info("Performing regular scan");
      int scanCount = performScanCountingEntries(client, tableName);
      log.info("Regular scan returned {} entries", scanCount);

      log.info("Performing batch scan");
      int batchScanCount = performBatchScanCountingEntries(client, tableName);
      log.info("Batch scan returned {} entries", batchScanCount);

      List<String> statsDMetrics;
      boolean foundMetric = false;
      long highestExceptionCount = 0;
      long startTime = System.currentTimeMillis();
      long timeout = 30_000;

      while (!foundMetric && (System.currentTimeMillis() - startTime) < timeout) {
        statsDMetrics = sink.getLines();

        if (!statsDMetrics.isEmpty()) {
          for (String line : statsDMetrics) {
            if (line.startsWith(METRICS_SCAN_EXCEPTIONS)) {
              foundMetric = true;
              TestStatsDSink.Metric metric = TestStatsDSink.parseStatsDMetric(line);
              String executor = metric.getTags().get("executor");
              if (executor != null) {
                long val = Long.parseLong(metric.getValue());
                highestExceptionCount = Math.max(highestExceptionCount, val);
                log.info("Found scan exception metric for executor '{}': {}", executor, val);
              }
            }
          }
        }

        if (!foundMetric) {
          Thread.sleep(1_000);
        }
      }

      log.info("Final exception count from metrics: {}", highestExceptionCount);

      assertTrue(foundMetric, "Should have found scan exception metric");
      assertTrue(highestExceptionCount > 0,
          "Scan exception metric should have a count > 0, but was: " + highestExceptionCount);
    }
  }

  private int performScanCountingEntries(AccumuloClient client, String table) {
    int count = 0;
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
      scanner.setTimeout(10, java.util.concurrent.TimeUnit.SECONDS);
      for (var entry : scanner) {
        count++;
        log.debug("Scan entry {}: {}", count, entry.getKey());
      }
      log.info("Scan completed successfully with {} entries", count);
    } catch (Exception e) {
      log.info("Exception during regular scan after {} entries: {}", count, e.getMessage());
    }
    return count;
  }

  private int performBatchScanCountingEntries(AccumuloClient client, String table) {
    int count = 0;
    try (BatchScanner batchScanner = client.createBatchScanner(table, Authorizations.EMPTY, 2)) {
      batchScanner.setTimeout(10, java.util.concurrent.TimeUnit.SECONDS);
      batchScanner.setRanges(List.of(new Range()));
      for (var entry : batchScanner) {
        count++;
        log.debug("Batch scan entry {}: {}", count, entry.getKey());
      }
      log.info("Batch scan completed successfully with {} entries", count);
    } catch (Exception e) {
      log.info("Exception during batch scan after {} entries: {}", count, e.getMessage());
    }
    return count;
  }
}

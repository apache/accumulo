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
package org.apache.accumulo.core.metrics;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.sink.FileSink;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.ConsoleReporter;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;

public class MetricsComparisonTest {

  private static class HadoopMetricsExample implements MetricsSource {

    private final MetricsRegistry registry;

    public HadoopMetricsExample() {
      registry = new MetricsRegistry(Interns.info(this.getClass().getSimpleName(), ""));
      registry.tag(MsInfo.ProcessName, "testProcess");
      registry.newCounter("counter", "This is a counter", 0).incr();
      registry.newGauge("gauge", "This is a gauge", 0L).incr(2);
      registry.newQuantiles("quantile", "This is a quantile", "I/O", "quantile", 5000).add(32);
      registry.newStat("stat", "This is a stat", "I/O", "stat", true).add(10, 100);
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      MetricsRecordBuilder builder = collector.addRecord("record").setContext("ctx");
      registry.snapshot(builder, all);
    }

  }

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  public static String getTestFilename(String basename) {
    return System.getProperty("test.build.classes", "target/test-classes") + "/" + basename
        + ".properties";
  }

  private void printFile(File f) throws IOException {
    BufferedReader rdr = Files.newBufferedReader(f.toPath());
    String line = rdr.readLine();
    while (line != null) {
      System.out.println(line);
      line = rdr.readLine();
    }
  }

  /*
   * Adapted from
   * https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/test/java/
   * org/apache/hadoop/metrics2/sink/TestFileSink.java
   */
  @Test
  public void testHadoopMetrics() throws Exception {

    File tmpOutput = tmpFolder.newFile("hadoop.metrics");
    System.out.println(tmpOutput.getAbsolutePath());

    String tmpConfig = getTestFilename("hadoop-metrics2-test");
    System.out.println(tmpConfig);

    new ConfigBuilder().add("*.period", 5).add("test.sink.mysink0.class", FileSink.class.getName())
        .add("test.sink.mysink0.filename", tmpOutput.getAbsolutePath())
        .add("test.sink.mysink0.context", "ctx").save(tmpConfig);

    printFile(new File(tmpConfig));

    MetricsSystemImpl hadoopMetricsSystem = new MetricsSystemImpl("test");
    hadoopMetricsSystem.start();

    HadoopMetricsExample example = new HadoopMetricsExample();
    hadoopMetricsSystem.register(example.getClass().getSimpleName(), "", example);

    Thread.sleep(10000); // 2 x the step time
    // hadoopMetricsSystem.publishMetricsNow();

    hadoopMetricsSystem.stop();
    hadoopMetricsSystem.shutdown();

    printFile(tmpOutput);
  }

  @Test
  public void testMicrometerMetrics() throws Exception {

    com.codahale.metrics.MetricRegistry codahaleRegistry =
        new com.codahale.metrics.MetricRegistry();

    DropwizardConfig config = new DropwizardConfig() {
      @Override
      public String prefix() {
        return "console";
      }

      @Override
      public String get(String key) {
        return null;
      }
    };
    DropwizardMeterRegistry registry = new DropwizardMeterRegistry(config, codahaleRegistry,
        HierarchicalNameMapper.DEFAULT, Clock.SYSTEM) {
      @Override
      protected Double nullGaugeValue() {
        return Double.NaN;
      }
    };

    ConsoleReporter reporter = ConsoleReporter.forRegistry(codahaleRegistry)
        .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.SECONDS).build();
    reporter.start(5, TimeUnit.SECONDS);

    Counter.builder("counter").register(registry).increment();
    ;
    Gauge.builder("gauge", () -> {
      return 2;
    }).register(registry);

    DistributionSummary.builder("quantile").register(registry).record(32);
    Timer.builder("stat").register(registry).record(Duration.ofMillis(10));

    Thread.sleep(10000); // 2 x the step time

    registry.close();

  }
}

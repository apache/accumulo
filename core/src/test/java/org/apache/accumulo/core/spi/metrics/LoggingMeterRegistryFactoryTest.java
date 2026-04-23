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
package org.apache.accumulo.core.spi.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;

class LoggingMeterRegistryFactoryTest {

  @Test
  public void createTest() {
    LoggingMeterRegistryFactory factory = new LoggingMeterRegistryFactory();
    var reg =
        factory.create(new LoggingMetricsParams(Map.of("prop1", "abc", "logging.step", "1s")));
    assertInstanceOf(LoggingMeterRegistry.class, reg);
  }

  private static class LoggingMetricsParams implements MeterRegistryFactory.InitParameters {

    private final Map<String,String> options;

    public LoggingMetricsParams(Map<String,String> options) {
      this.options = options;
    }

    @Override
    public Map<String,String> getOptions() {
      return options;
      // note: general.custom.metrics.opts. is expected to be stripped before passing the options.
      // return Map.of("prop1", "abc", "logging.step", "1s", "meter.id.filters", "Foo,Bar,Hat");
    }

    @Override
    public ServiceEnvironment getServiceEnv() {
      return null;
    }
  }

  /**
   * Create a factory and test that the given filters are applied to all ID's that match the given
   * meter.id.filters.
   */
  @Test
  public void createWithFilters() {
    LoggingMeterRegistryFactory factory = new LoggingMeterRegistryFactory();
    var reg = factory.create(new LoggingMetricsParams(
        Map.of("prop1", "abc", "logging.step", "1s", "meter.id.filters", "Foo,Bar,Hat")));
    reg.timer("FooTimer", "tag1", "tag2"); // Yes filters this out
    reg.timer("ProperTimer", "tag1", "tag2");

    List<Meter> meterReg = reg.getMeters(); // Does this only contain ProperTimer? Yes
    assertEquals("ProperTimer", meterReg.get(0).getId().getName());
  }

  /**
   * Verify that the filters can be split by either the default or user-specified delimiter.
   */
  @Test
  public void testNonCommaDelimiter() {
    LoggingMeterRegistryFactory factory = new LoggingMeterRegistryFactory();
    var reg = factory.create(new LoggingMetricsParams(Map.of("prop1", "abc", "logging.step", "1s",
        "meter.id.filters", "Foo:Bar:Hat", "meter.id.delimiter", ":")));
    reg.timer("FooTimer", "tag1", "tag2");
    reg.timer("ProperTimer", "tag1", "tag2");
    reg.timer("BarTimer", "tag1", "tag2");
    reg.timer("HatTimer", "tag1", "tag2");
    reg.gauge("BarGauge", 10);
    reg.gauge("ProperGauge", 10);
    reg.counter("HatCounter", "tag1", "tag2");
    reg.counter("ProperCounter", "tag1", "tag2");

    List<Meter> meterReg = reg.getMeters();
    StringBuilder allMeters = new StringBuilder();
    for (Meter m : meterReg) {
      allMeters.append(m.getId().getName());
    }

    assertFalse(allMeters.toString().contains("Foo"));
    assertFalse(allMeters.toString().contains("Bar"));
    assertFalse(allMeters.toString().contains("Hat"));
  }

  /**
   * Verify that {@link LoggingMeterRegistryFactory#getMeterFilter(String, String)} only filters out
   * meters where the filter terms are prefixes for the ID.
   */
  @Test
  public void testIfPatternIsNotFirst() {
    LoggingMeterRegistryFactory factory = new LoggingMeterRegistryFactory();
    var reg = factory.create(new LoggingMetricsParams(
        Map.of("prop1", "abc", "logging.step", "1s", "meter.id.filters", "Foo,Bar,Hat")));
    reg.timer("FooTimer", "tag1", "tag2");
    reg.timer("TimerFoo", "tag1", "tag2");

    List<Meter> meterReg = reg.getMeters();
    StringBuilder allMeters = new StringBuilder();
    for (Meter m : meterReg) {
      allMeters.append(m.getId().getName());
    }
    assertEquals("TimerFoo", allMeters.toString());
  }

  /**
   * Test that {@link LoggingMeterRegistryFactory#getMeterFilter(String, String)} can filter out
   * meters correctly even when given duplicates.
   */
  @Test
  public void testIfDuplicatePatterns() {
    LoggingMeterRegistryFactory factory = new LoggingMeterRegistryFactory();
    var reg = factory.create(new LoggingMetricsParams(
        Map.of("prop1", "abc", "logging.step", "1s", "meter.id.filters", "Foo,Foo,Foo")));
    reg.timer("FooTimer", "tag1", "tag2");

    List<Meter> meterReg = reg.getMeters();
    StringBuilder allMeters = new StringBuilder();
    for (Meter m : meterReg) {
      allMeters.append(m.getId().getName());
    }

    assertEquals("", allMeters.toString());
  }

  /**
   * Test that when {@link LoggingMeterRegistryFactory#getMeterFilter(String, String)} is given an
   * empty string of filters, all IDs are still passed through.
   */
  @Test
  public void testEmptyPatternList() {
    LoggingMeterRegistryFactory factory = new LoggingMeterRegistryFactory();
    var reg = factory.create(new LoggingMetricsParams(
        Map.of("prop1", "abc", "logging.step", "1s", "meter.id.filters", "")));
    reg.timer("FooTimer", "tag1", "tag2");
    List<Meter> meterReg = reg.getMeters();
    StringBuilder allMeters = new StringBuilder();
    for (Meter m : meterReg) {
      allMeters.append(m.getId().getName());
    }
    assertEquals("FooTimer", allMeters.toString());
  }

}

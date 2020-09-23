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
package org.apache.accumulo.master.metrics.fate;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InMemTestCollector implements MetricsCollector {

  private static final Logger log = LoggerFactory.getLogger(InMemTestCollector.class);

  final private Map<String,Number> measurements = new HashMap<>();

  final MetricsRecordBuilder builder = new FakeMetricsBuilder(measurements);

  @Override
  public MetricsRecordBuilder addRecord(String s) {
    log.debug("RecordBuilder: Adding string: {}", s);
    return builder;
  }

  @Override
  public MetricsRecordBuilder addRecord(MetricsInfo metricsInfo) {
    log.debug("FC: Add info {}", metricsInfo);
    return builder;
  }

  boolean contains(final String name) {
    return measurements.containsKey(name);
  }

  Number getValue(final String name) {
    return measurements.get(name);
  }

  @Override
  public String toString() {
    return "InMemTestCollector{" + "measurements=" + measurements + '}';
  }

  private static class FakeMetricsBuilder extends MetricsRecordBuilder {

    final Map<String,Number> measurements;

    FakeMetricsBuilder(Map<String,Number> measurements) {
      this.measurements = measurements;
    }

    @Override
    public MetricsRecordBuilder tag(MetricsInfo metricsInfo, String s) {
      log.debug("Add tag {}: {}", metricsInfo, s);
      return this;
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag metricsTag) {
      log.debug("Add tag {}", metricsTag);
      return this;
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric abstractMetric) {
      log.debug("Add abstractMetric: {}", abstractMetric);
      return this;
    }

    @Override
    public MetricsRecordBuilder setContext(String s) {
      log.debug("Set context: {}", s);
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, int i) {
      log.debug("add int counter: {}: {}", metricsInfo, i);
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, long l) {
      log.debug("add long counter: {}: {}", metricsInfo, l);
      measurements.put(metricsInfo.name(), l);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, int i) {
      log.debug("add int gauge: {}: {}", metricsInfo, i);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, long l) {
      log.debug("add long gauge: {}: {}", metricsInfo.name(), l);
      measurements.put(metricsInfo.name(), l);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, float v) {
      log.debug("add float gauge: {}: {}", metricsInfo, v);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, double v) {
      log.debug("add double gauge: {}: {}", metricsInfo, v);
      return this;
    }

    @Override
    public MetricsCollector endRecord() {
      log.debug("end record called");
      return super.endRecord();
    }

    @Override
    public MetricsCollector parent() {
      return null;
    }
  }
}

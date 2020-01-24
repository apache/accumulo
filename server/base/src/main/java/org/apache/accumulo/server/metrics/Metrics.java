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
package org.apache.accumulo.server.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.source.JvmMetricsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulo will search for a file named hadoop-metrics-accumulo.properties on the Accumulo
 * classpath to configute the hadoop metrics2 system. The hadoop metrics system publishes to jmx and
 * can be configured, via a configuration file, to publish to other metric collection systems
 * (files,...)
 * <p>
 * A note on naming: The naming for jmx vs the hadoop metrics systems are slightly different. Hadoop
 * metrics records will start with CONTEXT.RECORD, for example, accgc.AccGcCycleMetrics. The context
 * parameter value is also used by the configuration file for sink configuration.
 * <p>
 * In JMX, the hierarchy is: Hadoop..Accumulo..[jmxName]..[processName]..attributes..[name]
 * <p>
 * For jvm metrics, the hierarchy is Hadoop..Accumulo..JvmMetrics..attributes..[name]
 */
public abstract class Metrics implements MetricsSource {

  private static String processName = "Unknown";

  public static MetricsSystem initSystem(String serviceName) {
    processName = serviceName;
    String serviceInstance = System.getProperty("accumulo.metrics.service.instance", "");
    if (!serviceInstance.isBlank()) {
      processName += serviceInstance;
    }

    // create a new one if needed
    MetricsSystem ms = DefaultMetricsSystem.initialize("Accumulo");
    if (ms.getSource(JvmMetricsInfo.JvmMetrics.name()) == null) {
      JvmMetrics.create(processName, "", ms);
    }
    return ms;
  }

  private final String name;
  private final String description;
  private final String context;
  private final String record;
  private final MetricsRegistry registry;
  private final Logger log;

  protected Metrics(String name, String description, String context, String record) {
    this.log = LoggerFactory.getLogger(getClass());
    this.name = name;
    this.description = description;
    this.context = context;
    this.record = record;
    this.registry = new MetricsRegistry(Interns.info(name, description));
    this.registry.tag(MsInfo.ProcessName, processName);
  }

  public void register(MetricsSystem system) {
    system.register(name, description, this);
  }

  protected final MetricsRegistry getRegistry() {
    return registry;
  }

  /**
   * Runs prior to {@link #getMetrics(MetricsCollector, boolean)} in order to prepare metrics in the
   * {@link MetricsRegistry} to be published.
   */
  protected void prepareMetrics() {}

  /**
   * Append any additional metrics directly to the builder when
   * {@link #getMetrics(MetricsCollector, boolean)} is called, after any metrics in the
   * {@link MetricsRegistry} have already been added.
   */
  protected void getMoreMetrics(MetricsRecordBuilder builder, boolean all) {}

  @Override
  public final void getMetrics(MetricsCollector collector, boolean all) {
    log.trace("getMetrics called with collector: {} (all: {})", collector, all);
    prepareMetrics();
    MetricsRecordBuilder builder = collector.addRecord(record).setContext(context);
    registry.snapshot(builder, all);
    getMoreMetrics(builder, all);
  }

}

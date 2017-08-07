/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

/**
 *
 */
public class Metrics2ThriftMetrics implements Metrics, MetricsSource, ThriftMetricsKeys {
  public static final String CONTEXT = "thrift";

  private final MetricsSystem system;
  private final MetricsRegistry registry;
  private final String record, name, desc;

  Metrics2ThriftMetrics(MetricsSystem system, String serverName, String threadName) {
    this.system = system;
    this.record = serverName;
    this.name = THRIFT_NAME + ",sub=" + serverName;
    this.desc = "Thrift Server Metrics - " + serverName + " " + threadName;
    this.registry = new MetricsRegistry(Interns.info(name, desc));
    this.registry.tag(MsInfo.ProcessName, MetricsSystemHelper.getProcessName());
  }

  @Override
  public void add(String name, long time) {
    registry.add(name, time);
  }

  @Override
  public void register() {
    system.register(name, desc, this);
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(record).setContext(CONTEXT);

    registry.snapshot(builder, all);
  }
}

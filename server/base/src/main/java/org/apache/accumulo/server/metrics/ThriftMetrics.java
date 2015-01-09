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

import javax.management.ObjectName;

public class ThriftMetrics extends AbstractMetricsImpl implements ThriftMetricsMBean {

  static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(ThriftMetrics.class);

  private static final String METRICS_PREFIX = "thrift";

  private ObjectName OBJECT_NAME = null;

  public ThriftMetrics(String serverName, String threadName) {
    super();
    reset();
    try {
      OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=" + serverName + ",name=ThriftMetricsMBean,instance=" + threadName);
    } catch (Exception e) {
      log.error("Exception setting MBean object name", e);
    }
  }

  @Override
  protected ObjectName getObjectName() {
    return OBJECT_NAME;
  }

  @Override
  protected String getMetricsPrefix() {
    return METRICS_PREFIX;
  }

  public void reset() {
    createMetric(idle);
    createMetric(execute);
  }

  public long getExecutionAvgTime() {
    return this.getMetricAvg(execute);
  }

  public long getExecutionCount() {
    return this.getMetricCount(execute);
  }

  public long getExecutionMaxTime() {
    return this.getMetricMax(execute);
  }

  public long getExecutionMinTime() {
    return this.getMetricMin(execute);
  }

  public long getIdleAvgTime() {
    return this.getMetricAvg(idle);
  }

  public long getIdleCount() {
    return this.getMetricCount(idle);
  }

  public long getIdleMaxTime() {
    return this.getMetricMax(idle);
  }

  public long getIdleMinTime() {
    return this.getMetricMin(idle);
  }

}

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
package org.apache.accumulo.server.logger.metrics;

import javax.management.ObjectName;

import org.apache.accumulo.server.metrics.AbstractMetricsImpl;

public class LogWriterMetrics extends AbstractMetricsImpl implements LogWriterMetricsMBean {
  
  static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(LogWriterMetrics.class);
  
  private static final String METRICS_PREFIX = "logger";
  
  private static ObjectName OBJECT_NAME = null;
  
  public LogWriterMetrics() {
    super();
    reset();
    try {
      OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=LogWriter,name=LogWriterMBean,instance=" + Thread.currentThread().getName());
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
    createMetric(close);
    createMetric(copy);
    createMetric(create);
    createMetric(logAppend);
    createMetric(logFlush);
    createMetric(logException);
  }
  
  public long getCloseAvgTime() {
    return this.getMetricAvg(close);
  }
  
  public long getCloseCount() {
    return this.getMetricCount(close);
  }
  
  public long getCloseMaxTime() {
    return this.getMetricMax(close);
  }
  
  public long getCloseMinTime() {
    return this.getMetricMin(close);
  }
  
  public long getCopyAvgTime() {
    return this.getMetricAvg(copy);
  }
  
  public long getCopyCount() {
    return this.getMetricCount(copy);
  }
  
  public long getCopyMaxTime() {
    return this.getMetricMax(copy);
  }
  
  public long getCopyMinTime() {
    return this.getMetricMin(copy);
  }
  
  public long getCreateAvgTime() {
    return this.getMetricAvg(create);
  }
  
  public long getCreateCount() {
    return this.getMetricCount(create);
  }
  
  public long getCreateMaxTime() {
    return this.getMetricMax(create);
  }
  
  public long getCreateMinTime() {
    return this.getMetricMin(create);
  }
  
  public long getLogAppendAvgTime() {
    return this.getMetricAvg(logAppend);
  }
  
  public long getLogAppendCount() {
    return this.getMetricCount(logAppend);
  }
  
  public long getLogAppendMaxTime() {
    return this.getMetricMin(logAppend);
  }
  
  public long getLogAppendMinTime() {
    return this.getMetricMin(logAppend);
  }
  
  public long getLogFlushAvgTime() {
    return this.getMetricAvg(logFlush);
  }
  
  public long getLogFlushCount() {
    return this.getMetricCount(logFlush);
  }
  
  public long getLogFlushMaxTime() {
    return this.getMetricMin(logFlush);
  }
  
  public long getLogFlushMinTime() {
    return this.getMetricMin(logFlush);
  }
  
  public long getLogExceptionCount() {
    return this.getMetricCount(logException);
  }
  
}

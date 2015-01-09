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
package org.apache.accumulo.tserver.metrics;

import javax.management.ObjectName;

import org.apache.accumulo.server.metrics.AbstractMetricsImpl;

public class TabletServerScanMetrics extends AbstractMetricsImpl implements TabletServerScanMetricsMBean {

  static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TabletServerScanMetrics.class);

  public static final String METRICS_PREFIX = "tserver.scan";

  static ObjectName OBJECT_NAME = null;

  public TabletServerScanMetrics() {
    super();
    reset();
    try {
      OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerScanMetricsMBean,instance=" + Thread.currentThread().getName());
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

  public long getResultAvgSize() {
    return this.getMetricAvg(resultSize);
  }

  public long getResultCount() {
    return this.getMetricCount(resultSize);
  }

  public long getResultMaxSize() {
    return this.getMetricMax(resultSize);
  }

  public long getResultMinSize() {
    return this.getMetricMin(resultSize);
  }

  public long getScanAvgTime() {
    return this.getMetricAvg(scan);
  }

  public long getScanCount() {
    return this.getMetricCount(scan);
  }

  public long getScanMaxTime() {
    return this.getMetricMax(scan);
  }

  public long getScanMinTime() {
    return this.getMetricMin(scan);
  }

  public void reset() {
    createMetric(scan);
    createMetric(resultSize);
  }

}

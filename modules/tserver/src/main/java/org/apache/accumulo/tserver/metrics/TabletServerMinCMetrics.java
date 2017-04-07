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

public class TabletServerMinCMetrics extends AbstractMetricsImpl implements TabletServerMinCMetricsMBean {

  static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TabletServerMinCMetrics.class);

  private static final String METRICS_PREFIX = "tserver.minc";

  private ObjectName OBJECT_NAME = null;

  TabletServerMinCMetrics() {
    super();
    reset();
    try {
      OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerMinCMetricsMBean,instance=" + Thread.currentThread().getName());
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

  @Override
  public long getMinorCompactionMinTime() {
    return this.getMetricMin(MINC);
  }

  @Override
  public long getMinorCompactionAvgTime() {
    return this.getMetricAvg(MINC);
  }

  @Override
  public long getMinorCompactionCount() {
    return this.getMetricCount(MINC);
  }

  @Override
  public long getMinorCompactionMaxTime() {
    return this.getMetricMax(MINC);
  }

  @Override
  public long getMinorCompactionQueueAvgTime() {
    return this.getMetricAvg(QUEUE);
  }

  @Override
  public long getMinorCompactionQueueCount() {
    return this.getMetricCount(QUEUE);
  }

  @Override
  public long getMinorCompactionQueueMaxTime() {
    return this.getMetricMax(QUEUE);
  }

  @Override
  public long getMinorCompactionQueueMinTime() {
    return this.getMetricMin(MINC);
  }

  @Override
  public void reset() {
    createMetric("minc");
    createMetric("queue");
  }

}

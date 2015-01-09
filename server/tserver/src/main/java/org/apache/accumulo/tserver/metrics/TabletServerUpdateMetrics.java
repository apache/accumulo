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

public class TabletServerUpdateMetrics extends AbstractMetricsImpl implements TabletServerUpdateMetricsMBean {

  static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TabletServerUpdateMetrics.class);

  private static final String METRICS_PREFIX = "tserver.update";

  private static ObjectName OBJECT_NAME = null;

  public TabletServerUpdateMetrics() {
    super();
    reset();
    try {
      OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerUpdateMetricsMBean,instance="
          + Thread.currentThread().getName());
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

  public long getPermissionErrorCount() {
    return this.getMetricCount(permissionErrors);
  }

  public long getUnknownTabletErrorCount() {
    return this.getMetricCount(unknownTabletErrors);
  }

  public long getMutationArrayAvgSize() {
    return this.getMetricAvg(mutationArraySize);
  }

  public long getMutationArrayMinSize() {
    return this.getMetricMin(mutationArraySize);
  }

  public long getMutationArrayMaxSize() {
    return this.getMetricMax(mutationArraySize);
  }

  public long getCommitPrepCount() {
    return this.getMetricCount(commitPrep);
  }

  public long getCommitPrepMinTime() {
    return this.getMetricMin(commitPrep);
  }

  public long getCommitPrepMaxTime() {
    return this.getMetricMax(commitPrep);
  }

  public long getCommitPrepAvgTime() {
    return this.getMetricAvg(commitPrep);
  }

  public long getConstraintViolationCount() {
    return this.getMetricCount(constraintViolations);
  }

  public long getWALogWriteCount() {
    return this.getMetricCount(waLogWriteTime);
  }

  public long getWALogWriteMinTime() {
    return this.getMetricMin(waLogWriteTime);
  }

  public long getWALogWriteMaxTime() {
    return this.getMetricMax(waLogWriteTime);
  }

  public long getWALogWriteAvgTime() {
    return this.getMetricAvg(waLogWriteTime);
  }

  public long getCommitCount() {
    return this.getMetricCount(commitTime);
  }

  public long getCommitMinTime() {
    return this.getMetricMin(commitTime);
  }

  public long getCommitMaxTime() {
    return this.getMetricMax(commitTime);
  }

  public long getCommitAvgTime() {
    return this.getMetricAvg(commitTime);
  }

  public void reset() {
    createMetric(permissionErrors);
    createMetric(unknownTabletErrors);
    createMetric(mutationArraySize);
    createMetric(commitPrep);
    createMetric(constraintViolations);
    createMetric(waLogWriteTime);
    createMetric(commitTime);
  }

}

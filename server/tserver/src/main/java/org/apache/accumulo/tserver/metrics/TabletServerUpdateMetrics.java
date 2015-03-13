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

  static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TabletServerUpdateMetrics.class);

  private static final String METRICS_PREFIX = "tserver.update";

  private ObjectName OBJECT_NAME = null;

  TabletServerUpdateMetrics() {
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

  @Override
  public long getPermissionErrorCount() {
    return this.getMetricCount(PERMISSION_ERRORS);
  }

  @Override
  public long getUnknownTabletErrorCount() {
    return this.getMetricCount(UNKNOWN_TABLET_ERRORS);
  }

  @Override
  public long getMutationArrayAvgSize() {
    return this.getMetricAvg(MUTATION_ARRAY_SIZE);
  }

  @Override
  public long getMutationArrayMinSize() {
    return this.getMetricMin(MUTATION_ARRAY_SIZE);
  }

  @Override
  public long getMutationArrayMaxSize() {
    return this.getMetricMax(MUTATION_ARRAY_SIZE);
  }

  @Override
  public long getCommitPrepCount() {
    return this.getMetricCount(COMMIT_PREP);
  }

  @Override
  public long getCommitPrepMinTime() {
    return this.getMetricMin(COMMIT_PREP);
  }

  @Override
  public long getCommitPrepMaxTime() {
    return this.getMetricMax(COMMIT_PREP);
  }

  @Override
  public long getCommitPrepAvgTime() {
    return this.getMetricAvg(COMMIT_PREP);
  }

  @Override
  public long getConstraintViolationCount() {
    return this.getMetricCount(CONSTRAINT_VIOLATIONS);
  }

  @Override
  public long getWALogWriteCount() {
    return this.getMetricCount(WALOG_WRITE_TIME);
  }

  @Override
  public long getWALogWriteMinTime() {
    return this.getMetricMin(WALOG_WRITE_TIME);
  }

  @Override
  public long getWALogWriteMaxTime() {
    return this.getMetricMax(WALOG_WRITE_TIME);
  }

  @Override
  public long getWALogWriteAvgTime() {
    return this.getMetricAvg(WALOG_WRITE_TIME);
  }

  @Override
  public long getCommitCount() {
    return this.getMetricCount(COMMIT_TIME);
  }

  @Override
  public long getCommitMinTime() {
    return this.getMetricMin(COMMIT_TIME);
  }

  @Override
  public long getCommitMaxTime() {
    return this.getMetricMax(COMMIT_TIME);
  }

  @Override
  public long getCommitAvgTime() {
    return this.getMetricAvg(COMMIT_TIME);
  }

  @Override
  public void reset() {
    createMetric(PERMISSION_ERRORS);
    createMetric(UNKNOWN_TABLET_ERRORS);
    createMetric(MUTATION_ARRAY_SIZE);
    createMetric(COMMIT_PREP);
    createMetric(CONSTRAINT_VIOLATIONS);
    createMetric(WALOG_WRITE_TIME);
    createMetric(COMMIT_TIME);
  }

}

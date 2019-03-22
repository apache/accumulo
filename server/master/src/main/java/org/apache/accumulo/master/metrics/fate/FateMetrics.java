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
package org.apache.accumulo.master.metrics.fate;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.accumulo.server.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic implementation of fate metrics that publish to JMX when legacy metrics enabled. For logging
 * in addition to JMX, use the hadoop metrics2 implementation. The measurement type and values
 * provided are:
 * <ul>
 * <li>gauge - current count of FATE transactions in progress</li>
 * <li>gauge - last zookeeper id that modified FATE root path to provide estimate of fate
 * transaction liveliness</li>
 * <li>counter - the number of zookeeper connection errors since process started.</li>
 * </ul>
 * Implementation notes:
 * <p>
 * The fate operation estimate is based on zookeeper Stat structure and the property of pzxid. From
 * the zookeeper developer's guide: pzxid is "The zxid of the change that last modified children of
 * this znode." The pzxid should then change each time a FATE transaction is created or deleted -
 * and the zookeeper id (zxid) is expected to continuously increase because the zookeeper id is used
 * by zookeeper for ordering operations.
 */
public class FateMetrics implements Metrics, FateMetricsMBean {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(10);

  private volatile long minimumRefreshDelay;

  private final AtomicReference<FateMetricValues> metricValues;

  private volatile long lastUpdate = 0;

  private final String instanceId;

  private ObjectName objectName = null;

  private volatile boolean enabled = false;

  public FateMetrics(final String instanceId, final long minimumRefreshDelay) {

    this.instanceId = instanceId;

    this.minimumRefreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);

    metricValues = new AtomicReference<>(FateMetricValues.builder().build());

    try {
      objectName = new ObjectName(
          "accumulo.server.metrics:service=FateMetrics,name=FateMetricsMBean,instance="
              + Thread.currentThread().getName());
    } catch (Exception e) {
      log.error("Exception setting MBean object name", e);
    }
  }

  @Override
  public void register() throws Exception {
    // Register this object with the MBeanServer
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (null == objectName)
      throw new IllegalArgumentException("MBean object name must be set.");
    mbs.registerMBean(this, objectName);
    enabled = true;
  }

  @Override
  public void add(String name, long time) {
    throw new UnsupportedOperationException("add() is not implemented");
  }

  /**
   * Update the metric values from zookeeper after minimumRefreshDelay has expired.
   */
  public synchronized FateMetricValues snapshot() {

    FateMetricValues current = metricValues.get();

    long now = System.currentTimeMillis();

    if ((lastUpdate + minimumRefreshDelay) > now) {
      return current;
    }

    FateMetricValues updates = FateMetricValues.updateFromZookeeper(instanceId, current);

    metricValues.set(updates);

    lastUpdate = now;

    return updates;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public long getCurrentFateOps() {
    snapshot();
    return metricValues.get().getCurrentFateOps();
  }

  @Override
  public long getZkFateChildOpsTotal() {
    snapshot();
    return metricValues.get().getZkFateChildOpsTotal();
  }

  @Override
  public long getZKConnectionErrorsTotal() {
    snapshot();
    return metricValues.get().getZkConnectionErrors();
  }

}

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

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.server.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic implementation of fate metrics:
 * <ul>
 * <li>gauge - current count of FATE transactions in progress</li>
 * <li>gauge - last zookeeper id that modified FATE root path to provide estimate of fate
 * transaction liveliness</li>
 * <li>counter - the number of zookeeper connection errors since process started.</li>
 * </ul>
 * Implementation notes:
 * </p>
 * The fate operation estimate is based on zookeeper Stat structure and the property of pzxid. From
 * the zookeeper developer's guide: pzxid is "The zxid of the change that last modified children of
 * this znode." The pzxid should then change each time a FATE transaction is created or deleted -
 * and the zookeeper id (zxid) is expected to continuously increase because the zookeeper id is used
 * by zookeeper for ordering operations.
 */
public class FateMetrics implements Metrics, FateMetricsMBean {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(60);

  private volatile long minimumRefreshDelay = DEFAULT_MIN_REFRESH_DELAY;

  private final AtomicReference<FateMetricValues> metricValues;

  private volatile long lastUpdate = 0;

  private final Instance instance;

  private ObjectName objectName = null;

  private volatile boolean enabled = false;

  public FateMetrics(final Instance instance) {

    this.instance = instance;

    // instance = master.getInstance();

    metricValues = new AtomicReference<>(FateMetricValues.Builder.getBuilder().build());

    try {
      objectName = new ObjectName(
          "accumulo.server.metrics:service=FateMetrics,name=FateMetricsMBean,instance="
              + Thread.currentThread().getName());
    } catch (Exception e) {
      log.error("Exception setting MBean object name", e);
    }
  }

  /**
   * Get the current delay required before a the metric values will be refreshed from the system
   * (zookeeper) - if the delay has not expired, the previous values are returned.
   *
   * @return the current delay in milliseconds
   */
  public long getMinimumRefreshDelay() {
    return minimumRefreshDelay;
  }

  /**
   * Modify the refresh delay minimum in milliseconds and return the previous value. Each time these
   * metrics are fetched, the time since last update and this delay is used to determine if the
   * values should be updated from zookeeper or the current cached value is returned.
   *
   * @param value
   *          set the minimum refresh delay (in milliseconds)
   * @return the previous value.
   */
  public long updateMinimumRefreshDelay(final long value) {
    long curr = minimumRefreshDelay;
    minimumRefreshDelay = value;
    return curr;
  }

  /**
   * Clear the current measurement time so that the next populate call will read from zookeeper -
   * primarily for testing purposes.
   */
  public void resetDelayTimer() {
    lastUpdate = 0;
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
   * Update the metric values from zookeeper is the delay has expired. The delay timer can be
   * cleared using {@link #resetDelayTimer() to force an update}
   */
  public synchronized FateMetricValues snapshot() {

    FateMetricValues current = metricValues.get();

    long now = System.currentTimeMillis();

    if ((lastUpdate + minimumRefreshDelay) > now) {
      return current;
    }

    FateMetricValues updates = FateMetricValues.updateFromZookeeper(instance, current);

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

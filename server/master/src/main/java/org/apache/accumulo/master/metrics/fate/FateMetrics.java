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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

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

  // limit calls to update fate counters to guard against hammering zookeeper.
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(30);

  private long minimumRefreshDelay = DEFAULT_MIN_REFRESH_DELAY;

  private final AtomicReference<FateMetricValues> metricValues;

  private volatile long lastUpdate = 0;

  private final Instance instance;

  private final AdminUtil<String> admin;

  public FateMetrics(final Master master) {

    instance = master.getInstance();

    metricValues = new AtomicReference<>(FateMetricValues.Builder.getBuilder().build());

    admin = new AdminUtil<>(false);
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

  @Override
  public void register() throws Exception {

  }

  @Override
  public void add(String name, long time) {

  }

  /**
   * Update the metric values from zookeeper is the delay has expired.
   */
  private synchronized void snapshot() {

    long now = System.currentTimeMillis();

    if (now > (lastUpdate + minimumRefreshDelay)) {

      FateMetricValues.Builder valueBuilder = FateMetricValues.Builder.copy(metricValues.get());

      try {

        IZooReaderWriter zoo = ZooReaderWriter.getInstance();
        ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instance) + Constants.ZFATE, zoo);

        List<AdminUtil.TransactionStatus> noLocks = admin.getTransactionStatus(zs, null, null);
        valueBuilder.withCurrentFateOps(noLocks.size());

        Stat node = zoo.getZooKeeper().exists(ZooUtil.getRoot(instance) + Constants.ZFATE, false);
        valueBuilder.withLastFateZxid(node.getPzxid());

        metricValues.set(valueBuilder.build());

      } catch (KeeperException ex) {

        valueBuilder.incrZkConnectionErrors();

        metricValues.set(valueBuilder.build());

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      lastUpdate = now;

    }
  }

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public long getCurrentFateOps() {
    snapshot();
    return metricValues.get().getCurrentFateOps();
  }

  @Override
  public long getLastFateZxid() {
    snapshot();
    return metricValues.get().getLastFateZxid();
  }

  @Override
  public long getZKConnectionErrorsTotal() {
    snapshot();
    return metricValues.get().getZkConnectionErrors();
  }

  /**
   * Immutable class that holds a snapshot of fate metric values - use builder to instantiate
   * instance.
   */
  protected static class FateMetricValues {

    private final long currentFateOps;
    private final long lastFateZxid;
    private final long zkConnectionErrors;

    private FateMetricValues(final long currentFateOps, final long lastFateZxid,
        final long zkConnectionErrors) {
      this.currentFateOps = currentFateOps;
      this.lastFateZxid = lastFateZxid;
      this.zkConnectionErrors = zkConnectionErrors;
    }

    long getCurrentFateOps() {
      return currentFateOps;
    }

    long getLastFateZxid() {
      return lastFateZxid;
    }

    long getZkConnectionErrors() {
      return zkConnectionErrors;
    }

    static class Builder {

      private long currentFateOps = 0;
      private long lastFateZxid = 0;
      private long zkConnectionErrors = 0;

      Builder() {}

      static Builder getBuilder() {
        return new Builder();
      }

      static Builder copy(final FateMetricValues values) {
        Builder builder = new Builder();
        builder.currentFateOps = values.getCurrentFateOps();
        builder.lastFateZxid = values.getLastFateZxid();
        builder.zkConnectionErrors = values.getZkConnectionErrors();
        return builder;
      }

      Builder withCurrentFateOps(final long value) {
        this.currentFateOps = value;
        return this;
      }

      Builder withLastFateZxid(final long value) {
        this.lastFateZxid = value;
        return this;
      }

      Builder incrZkConnectionErrors() {
        this.zkConnectionErrors += 1L;
        return this;
      }

      Builder withZkConnectionErrors(final long value) {
        this.zkConnectionErrors = value;
        return this;
      }

      FateMetricValues build() {
        return new FateMetricValues(currentFateOps, lastFateZxid, zkConnectionErrors);
      }
    }
  }

}

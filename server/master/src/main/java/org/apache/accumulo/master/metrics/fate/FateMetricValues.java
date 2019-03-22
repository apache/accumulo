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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable class that holds a snapshot of fate metric values - use builder to instantiate
 * instance.
 */
class FateMetricValues {

  private static final Logger log = LoggerFactory.getLogger(FateMetrics.class);

  private final long updateTime;
  private final long currentFateOps;
  private final long zkFateChildOpsTotal;
  private final long zkConnectionErrors;

  private FateMetricValues(final long updateTime, final long currentFateOps,
      final long zkFateChildOpsTotal, final long zkConnectionErrors) {
    this.updateTime = updateTime;
    this.currentFateOps = currentFateOps;
    this.zkFateChildOpsTotal = zkFateChildOpsTotal;
    this.zkConnectionErrors = zkConnectionErrors;
  }

  long getCurrentFateOps() {
    return currentFateOps;
  }

  long getZkFateChildOpsTotal() {
    return zkFateChildOpsTotal;
  }

  long getZkConnectionErrors() {
    return zkConnectionErrors;
  }

  /**
   * Update FateMetricValues, populating with current values and the overwritting new values, this
   * preserves previous values in case an error or exception prevents the values from being
   * completely populated, this form may be more suitable for metric counters.
   *
   * @param instanceId
   *          Accumulo instanceId
   * @param currentValues
   *          the current fate metrics used as default
   * @return populated metrics values
   */
  static FateMetricValues updateFromZookeeper(final String instanceId,
      final FateMetricValues currentValues) {
    return updateFromZookeeper(instanceId, FateMetricValues.builder().copy(currentValues));
  }

  /**
   * Update the FATE metric values from zookeeepr - the builder is expected to have the desired
   * default values (either 0, or the previous value).
   *
   * @param instanceId
   *          Accumulo instanceId
   * @param builder
   *          value builder, populated with defaults.
   * @return an immutable instance of FateMetricsValues.
   */
  private static FateMetricValues updateFromZookeeper(final String instanceId,
      final FateMetricValues.Builder builder) {

    AdminUtil<String> admin = new AdminUtil<>(false);

    try {

      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instanceId) + Constants.ZFATE, zoo);

      List<AdminUtil.TransactionStatus> currFates = admin.getTransactionStatus(zs, null, null);
      builder.withCurrentFateOps(currFates.size());

      Stat node = zoo.getZooKeeper().exists(ZooUtil.getRoot(instanceId) + Constants.ZFATE, false);
      builder.withZkFateChildOpsTotal(node.getCversion());

      if (log.isTraceEnabled()) {
        log.trace(
            "ZkNodeStat: {czxid: {}, mzxid: {}, pzxid: {}, ctime: {}, mtime: {}, "
                + "version: {}, cversion: {}, num children: {}",
            node.getCzxid(), node.getMzxid(), node.getPzxid(), node.getCtime(), node.getMtime(),
            node.getVersion(), node.getCversion(), node.getNumChildren());
      }
    } catch (KeeperException ex) {
      log.debug("Error connecting to ZooKeeper", ex);
      builder.incrZkConnectionErrors();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    return builder.build();
  }

  @Override
  public String toString() {
    return "FateMetricValues{" + "updateTime=" + updateTime + ", currentFateOps=" + currentFateOps
        + ", zkFateChildOpsTotal=" + zkFateChildOpsTotal + ", zkConnectionErrors="
        + zkConnectionErrors + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  static class Builder {

    private long currentFateOps = 0;
    private long zkFateChildOpsTotal = 0;
    private long zkConnectionErrors = 0;

    Builder copy(final FateMetricValues prevValues) {

      if (prevValues == null) {
        return new Builder();
      }

      return new Builder().withCurrentFateOps(prevValues.getCurrentFateOps())
          .withZkFateChildOpsTotal(prevValues.getZkFateChildOpsTotal())
          .withZkConnectionErrors(prevValues.getZkConnectionErrors());
    }

    Builder withCurrentFateOps(final long value) {
      this.currentFateOps = value;
      return this;
    }

    Builder withZkFateChildOpsTotal(final long value) {
      this.zkFateChildOpsTotal = value;
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
      return new FateMetricValues(System.currentTimeMillis(), currentFateOps, zkFateChildOpsTotal,
          zkConnectionErrors);
    }
  }
}

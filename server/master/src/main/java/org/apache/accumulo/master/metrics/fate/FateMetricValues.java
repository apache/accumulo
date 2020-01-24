/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.metrics.fate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;

import org.apache.accumulo.fate.ReadOnlyTStore;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable class that holds a snapshot of fate metric values - use builder to instantiate
 * instance.
 */
class FateMetricValues {

  private static final Logger log = LoggerFactory.getLogger(FateMetricValues.class);

  private final long updateTime;
  private final long currentFateOps;
  private final long zkFateChildOpsTotal;
  private final long zkConnectionErrors;

  private final Map<String,Long> txStateCounters;
  private final Map<String,Long> opTypeCounters;

  private FateMetricValues(final long updateTime, final long currentFateOps,
      final long zkFateChildOpsTotal, final long zkConnectionErrors,
      final Map<String,Long> txStateCounters, final Map<String,Long> opTypeCounters) {
    this.updateTime = updateTime;
    this.currentFateOps = currentFateOps;
    this.zkFateChildOpsTotal = zkFateChildOpsTotal;
    this.zkConnectionErrors = zkConnectionErrors;
    this.txStateCounters = txStateCounters;
    this.opTypeCounters = opTypeCounters;
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
   * Provides counters for transaction states (NEW, IN_PROGRESS, FAILED,...).
   *
   * @return a map of transaction status counters.
   */
  Map<String,Long> getTxStateCounters() {
    return txStateCounters;
  }

  /**
   * The FATE transaction stores the transaction type as a debug string in the transaction zknode.
   * This method returns a map of counters of the current occurrences of each operation type that is
   * IN_PROGRESS.
   *
   * @return a map of operation type counters.
   */
  Map<String,Long> getOpTypeCounters() {
    return opTypeCounters;
  }

  /**
   * The FATE transaction stores the transaction type as a debug string in the transaction zknode.
   * This method returns a map of counters of the current occurrences of each operation type that is
   * IN_PROGRESS.
   *
   * @return a map of operation type counters.
   */
  private static FateMetricValues updateFromZookeeper(final ServerContext context,
      final FateMetricValues.Builder builder) {

    AdminUtil<String> admin = new AdminUtil<>(false);

    try {

      ZooReaderWriter zoo = context.getZooReaderWriter();
      ZooStore<String> zs = new ZooStore<>(context.getZooKeeperRoot() + Constants.ZFATE, zoo);

      List<AdminUtil.TransactionStatus> currFates = admin.getTransactionStatus(zs, null, null);
      builder.withCurrentFateOps(currFates.size());

      Stat node = zoo.getZooKeeper().exists(context.getZooKeeperRoot() + Constants.ZFATE, false);
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

    private Map<String,Long> txStateCounters;
    private Map<String,Long> opTypeCounters;

    Builder() {

      // states are enumerated - create new map with counts initialized to 0.
      txStateCounters = new TreeMap<>();
      for (ReadOnlyTStore.TStatus t : ReadOnlyTStore.TStatus.values()) {
        txStateCounters.put(t.name(), 0L);
      }

      opTypeCounters = Collections.emptyMap();
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

    Builder withTxStateCounters(final Map<String,Long> txStateCounters) {
      this.txStateCounters.putAll(txStateCounters);
      return this;
    }

    Builder withOpTypeCounters(final Map<String,Long> opTypeCounters) {
      this.opTypeCounters = new TreeMap<>(opTypeCounters);
      return this;
    }

    FateMetricValues build() {
      return new FateMetricValues(System.currentTimeMillis(), currentFateOps, zkFateChildOpsTotal,
          zkConnectionErrors, txStateCounters, opTypeCounters);
    }
  }
}

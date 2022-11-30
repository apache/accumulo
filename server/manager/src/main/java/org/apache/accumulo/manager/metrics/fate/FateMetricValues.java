/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.metrics.fate;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.ReadOnlyTStore;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable class that holds a snapshot of fate metric values - use builder to instantiate an
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
   * IN_PROGRESS
   *
   * @param context Accumulo context
   * @param fateRootPath the zookeeper path to fate info
   * @param zooStore a readonly zoostore
   * @return the current FATE metric values.
   */
  public static FateMetricValues getFromZooKeeper(final ServerContext context,
      final String fateRootPath, final ReadOnlyTStore<FateMetrics> zooStore) {

    FateMetricValues.Builder builder = FateMetricValues.builder();

    AdminUtil<FateMetrics> admin = new AdminUtil<>(false);

    try {

      List<AdminUtil.TransactionStatus> currFates =
          admin.getTransactionStatus(zooStore, null, null);

      builder.withCurrentFateOps(currFates.size());

      // states are enumerated - create new map with counts initialized to 0.
      Map<String,Long> states = new TreeMap<>();
      for (ReadOnlyTStore.TStatus t : ReadOnlyTStore.TStatus.values()) {
        states.put(t.name(), 0L);
      }

      // op types are dynamic, no count initialization needed - clearing prev values will
      // need to be handled by the caller - this is just the counts for current op types.
      Map<String,Long> opTypeCounters = new TreeMap<>();

      for (AdminUtil.TransactionStatus tx : currFates) {

        String stateName = tx.getStatus().name();

        // incr count for state
        states.merge(stateName, 1L, Long::sum);

        // incr count for op type for for in_progress transactions.
        if (ReadOnlyTStore.TStatus.IN_PROGRESS.equals(tx.getStatus())) {
          String opType = tx.getTxName();
          if (opType == null || opType.isEmpty()) {
            opType = "UNKNOWN";
          }
          opTypeCounters.merge(opType, 1L, Long::sum);
        }
      }

      builder.withTxStateCounters(states);
      builder.withOpTypeCounters(opTypeCounters);

      Stat node = context.getZooReaderWriter().getZooKeeper().exists(fateRootPath, false);
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
    return "FateMetricValues{updateTime=" + updateTime + ", currentFateOps=" + currentFateOps
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

    private final Map<String,Long> txStateCounters;
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

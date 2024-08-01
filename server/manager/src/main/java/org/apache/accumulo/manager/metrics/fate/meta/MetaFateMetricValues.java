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
package org.apache.accumulo.manager.metrics.fate.meta;

import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.manager.metrics.fate.FateMetricValues;
import org.apache.accumulo.manager.metrics.fate.FateMetrics;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class MetaFateMetricValues extends FateMetricValues {

  private static final Logger log = LoggerFactory.getLogger(MetaFateMetricValues.class);

  private final long zkFateChildOpsTotal;
  private final long zkConnectionErrors;

  protected MetaFateMetricValues(final long updateTime, final long currentFateOps,
      final long zkFateChildOpsTotal, final long zkConnectionErrors,
      final Map<String,Long> txStateCounters, final Map<String,Long> opTypeCounters) {
    super(updateTime, currentFateOps, txStateCounters, opTypeCounters);

    this.zkFateChildOpsTotal = zkFateChildOpsTotal;
    this.zkConnectionErrors = zkConnectionErrors;

  }

  long getZkFateChildOpsTotal() {
    return zkFateChildOpsTotal;
  }

  long getZkConnectionErrors() {
    return zkConnectionErrors;
  }

  /**
   * The FATE transaction stores the transaction type as a debug string in the transaction zknode.
   * This method returns a map of counters of the current occurrences of each operation type that is
   * IN_PROGRESS
   *
   * @param context Accumulo context
   * @param fateRootPath the zookeeper path to fate info
   * @param metaFateStore a readonly MetaFateStore
   * @return the current FATE metric values.
   */
  public static MetaFateMetricValues getMetaStoreMetrics(final ServerContext context,
      final String fateRootPath,
      final ReadOnlyFateStore<FateMetrics<MetaFateMetricValues>> metaFateStore) {
    Preconditions.checkArgument(metaFateStore.type() == FateInstanceType.META,
        "Fate store must be of type %s", FateInstanceType.META);

    Builder builder = null;

    try {
      builder = getFateMetrics(metaFateStore, new Builder());

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
      Optional.ofNullable(builder).ifPresent(Builder::incrZkConnectionErrors);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    return builder.build();
  }

  @Override
  public String toString() {
    return "MetaFateMetricValues{updateTime=" + updateTime + ", currentFateOps=" + currentFateOps
        + ", zkFateChildOpsTotal=" + zkFateChildOpsTotal + ", zkConnectionErrors="
        + zkConnectionErrors + '}';
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder extends AbstractBuilder<Builder,MetaFateMetricValues> {

    private long zkFateChildOpsTotal = 0;
    private long zkConnectionErrors = 0;

    Builder() {

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

    @Override
    protected MetaFateMetricValues build() {
      return new MetaFateMetricValues(System.currentTimeMillis(), currentFateOps,
          zkFateChildOpsTotal, zkConnectionErrors, txStateCounters, opTypeCounters);
    }
  }
}

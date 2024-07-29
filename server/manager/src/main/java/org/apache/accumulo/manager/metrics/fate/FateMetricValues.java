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
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable class that holds a snapshot of fate metric values - use builder to instantiate an
 * instance.
 */
public abstract class FateMetricValues {

  private static final Logger log = LoggerFactory.getLogger(FateMetricValues.class);

  protected final long updateTime;
  protected final long currentFateOps;

  protected final Map<String,Long> txStateCounters;
  protected final Map<String,Long> opTypeCounters;

  protected FateMetricValues(final long updateTime, final long currentFateOps,
      final Map<String,Long> txStateCounters, final Map<String,Long> opTypeCounters) {
    this.updateTime = updateTime;
    this.currentFateOps = currentFateOps;
    this.txStateCounters = txStateCounters;
    this.opTypeCounters = opTypeCounters;
  }

  public long getCurrentFateOps() {
    return currentFateOps;
  }

  /**
   * Provides counters for transaction states (NEW, IN_PROGRESS, FAILED,...).
   *
   * @return a map of transaction status counters.
   */
  public Map<String,Long> getTxStateCounters() {
    return txStateCounters;
  }

  /**
   * The FATE transaction stores the transaction type as a debug string in the transaction zknode.
   * This method returns a map of counters of the current occurrences of each operation type that is
   * IN_PROGRESS.
   *
   * @return a map of operation type counters.
   */
  public Map<String,Long> getOpTypeCounters() {
    return opTypeCounters;
  }

  protected static <T extends AbstractBuilder<T,U>,U extends FateMetricValues> T
      getFateMetrics(final ReadOnlyFateStore<FateMetrics<U>> fateStore, T builder) {

    AdminUtil<FateMetrics<U>> admin = new AdminUtil<>(false);

    List<AdminUtil.TransactionStatus> currFates =
        admin.getTransactionStatus(Map.of(fateStore.type(), fateStore), null, null, null);

    builder.withCurrentFateOps(currFates.size());

    // states are enumerated - create new map with counts initialized to 0.
    Map<String,Long> states = new TreeMap<>();
    for (ReadOnlyFateStore.TStatus t : ReadOnlyFateStore.TStatus.values()) {
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
      if (ReadOnlyFateStore.TStatus.IN_PROGRESS.equals(tx.getStatus())) {
        String opType = tx.getTxName();
        if (opType == null || opType.isEmpty()) {
          opType = "UNKNOWN";
        }
        opTypeCounters.merge(opType, 1L, Long::sum);
      }
    }

    builder.withTxStateCounters(states);
    builder.withOpTypeCounters(opTypeCounters);

    return builder;
  }

  @SuppressWarnings("unchecked")
  protected static abstract class AbstractBuilder<T extends AbstractBuilder<T,U>,
      U extends FateMetricValues> {

    protected long currentFateOps = 0;
    protected final Map<String,Long> txStateCounters;
    protected Map<String,Long> opTypeCounters;

    protected AbstractBuilder() {
      // states are enumerated - create new map with counts initialized to 0.
      txStateCounters = new TreeMap<>();
      for (ReadOnlyFateStore.TStatus t : ReadOnlyFateStore.TStatus.values()) {
        txStateCounters.put(t.name(), 0L);
      }

      opTypeCounters = Collections.emptyMap();
    }

    public T withCurrentFateOps(final long value) {
      this.currentFateOps = value;
      return (T) this;
    }

    public T withTxStateCounters(final Map<String,Long> txStateCounters) {
      this.txStateCounters.putAll(txStateCounters);
      return (T) this;
    }

    public T withOpTypeCounters(final Map<String,Long> opTypeCounters) {
      this.opTypeCounters = new TreeMap<>(opTypeCounters);
      return (T) this;
    }

    protected abstract U build();
  }
}

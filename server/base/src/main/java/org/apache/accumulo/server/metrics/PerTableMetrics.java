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
package org.apache.accumulo.server.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.*;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.*;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

/**
 * Common code for dealing with per table metrics. This code handles automatically creating and
 * deleting per table metrics as needed. To use this class extend and implement
 * {@link #newAllTablesMetrics(MeterRegistry, Consumer, List)} and
 * {@link #newPerTableMetrics(MeterRegistry, TableId, Consumer, List)} to create per table metrics
 * object and then use {@link #getTableMetrics(TableId)} to get those cached objects.
 */
public abstract class PerTableMetrics<T> implements MetricsProducer {

  public static final String TABLE_ID_TAG_NAME = "tableId";

  private final ServerContext context;

  private static class TableMetricsInfo<T2> {
    final T2 tableMetrics;
    volatile Timer inactiveTime;
    final List<Meter> meters;

    public TableMetricsInfo(T2 tableMetrics, List<Meter> meters) {
      this.tableMetrics = Objects.requireNonNull(tableMetrics);
      this.meters = meters;
    }
  }

  private final ActiveTableIdTracker activeTableIdTracker;
  private final ConcurrentHashMap<TableId,TableMetricsInfo<T>> perTableMetrics =
      new ConcurrentHashMap<>();
  private T allTableMetrics;
  private volatile MeterRegistry registry;

  /**
   * Tracks the active set of table ids in a scan server or tablet server for the purpose of per
   * table metrics.
   *
   * <p>
   * Each tablet or scan server should create a single instance of this object and pass it to all
   * {@link PerTableMetrics} objects.
   * </p>
   *
   * <p>
   * This class does not offer a tabletUnloaded method because there is no efficient way to compute
   * this. For more detail see {@link #tabletLoaded(KeyExtent)}
   * </p>
   *
   */
  public static class ActiveTableIdTracker {

    private final List<PerTableMetrics<?>> listeners = new ArrayList<>();
    private final Supplier<Set<TableId>> activeTableSupplier;
    private final AtomicReference<Set<TableId>> currentTableIds;
    private final boolean perTableActive;

    /**
     * @param activeTableSupplier this supplier should always return the latest set of table ids
     *        that exist on the server. This class will take care of caching that set.
     */
    public ActiveTableIdTracker(ServerContext context, Supplier<Set<TableId>> activeTableSupplier) {
      this.activeTableSupplier = activeTableSupplier;
      this.perTableActive =
          context.getConfiguration().getBoolean(Property.GENERAL_MICROMETER_TABLE_METRICS_ENABLED);
      if (perTableActive) {
        // The scan server and tablets servers will make a best effort attempt to call tabletLoaded,
        // but may not always because of exceptions. This periodic task will ensure changes in the
        // active set of tableIds are seen. It also handles cleanup of tablet unload for which there
        // is no explicit notification.
        var future = context.getScheduledExecutor()
            .scheduleAtFixedRate(this::checkIfTableIdsChanged, 30, 30, TimeUnit.SECONDS);
        ThreadPools.watchCriticalScheduledTask(future);
      }
      this.currentTableIds = new AtomicReference<>(Set.of());
    }

    private synchronized void addChangeListener(PerTableMetrics<?> listener) {
      if (perTableActive) {
        listeners.add(listener);
      }
    }

    private synchronized void checkIfTableIdsChanged() {
      if (!perTableActive) {
        return;
      }

      var latest = activeTableSupplier.get();
      if (!latest.equals(currentTableIds.get())) {
        currentTableIds.set(Set.copyOf(latest));
        listeners.forEach(listener -> listener.refresh(latest));
      }
    }

    /**
     * If the table id for this tablet is not being tracked will notify all per table metrics
     * implementations of the new table.
     *
     * <p>
     * There is no corresponding tableUnloaded method because implementing it would be inefficient.
     * When a scan server computes the set of table ids it iterates over all tablets. When a tablet
     * is unloaded a scan server it could still have other tablets for the same table. On each
     * tablet unload do not want to iterator over all other tablets to see if the table id is still
     * active, which is what the implementation of tableUnloaded would do.
     */
    public void tabletLoaded(KeyExtent extent) {
      if (perTableActive && !currentTableIds.get().contains(extent.tableId())) {
        checkIfTableIdsChanged();
      }
    }

    // This method avoids locking unless a table id is unknown. This is important because every scan
    // on a scan server will call it and locking would introduce thread contention.
    public void tabletsLoaded(Set<KeyExtent> extents) {
      if (perTableActive) {
        var currentSnapshot = currentTableIds.get();
        for (var extent : extents) {
          if (!currentSnapshot.contains(extent.tableId())) {
            checkIfTableIdsChanged();
            break;
          }
        }
      }
    }

    public boolean isPerTableMetricsEnabled() {
      return perTableActive;
    }

  }

  public PerTableMetrics(ServerContext context, ActiveTableIdTracker activeTableIdTracker) {
    this.context = context;
    this.activeTableIdTracker = activeTableIdTracker;
    this.activeTableIdTracker.addChangeListener(this);
  }

  /**
   * This method exist so this class can log using the logger of the subclass.
   */
  protected abstract Logger getLog();

  /**
   * Subclasses should implement this method to create a TableMetrics object that will be used in
   * the case when per table metrics are disabled. The object returned by this method will alway be
   * returned by {@link #getTableMetrics(TableId)} no matter what the table id is.
   *
   * @param registry register an meters for the table metrics in this registry
   * @param meters a consumer that accepts meters to be removed from the registry when the table
   *        metrics object is discarded. Currently this consumer does nothing with the meters, its
   *        passed for consistency with
   *        {@link #newPerTableMetrics(MeterRegistry, TableId, Consumer, List)}
   * @param tags currently an empty collection of tags, this is passed for consistency with
   *        {@link #newPerTableMetrics(MeterRegistry, TableId, Consumer, List)}
   * @return a new object that will be cached and later returned by
   *         {@link #getTableMetrics(TableId)}
   */
  protected abstract T newAllTablesMetrics(MeterRegistry registry, Consumer<Meter> meters,
      List<Tag> tags);

  /**
   *
   * Subclasses should implement this method to create per table table metrics objects. This method
   * is called in the case where per table metrics are enabled. These objects will be cached and
   * returned by {@link #getTableMetrics(TableId)}. Table metrics object in the cache that are no
   * longer needed will be automatically removed when the table is deleted or this server has not
   * hosted the table for a bit.
   *
   * @param registry register an meters for the table metrics in this registry
   * @param meters a consumer that accepts meters to be removed from the registry when the per table
   *        metrics object is discarded.
   * @param tags returns a list with a single tag in it which is the tableId. These tags should be
   *        used when registering meters
   * @return a new object that will be cached and later returned by
   *         {@link #getTableMetrics(TableId)}
   */
  protected abstract T newPerTableMetrics(MeterRegistry registry, TableId tableId,
      Consumer<Meter> meters, List<Tag> tags);

  private TableMetricsInfo<T> getOrCreateTableMetrics(TableId tableId) {
    Preconditions.checkState(activeTableIdTracker.isPerTableMetricsEnabled());
    return perTableMetrics.computeIfAbsent(tableId, tid -> {
      List<Meter> meters = new ArrayList<>();
      T tableMetrics = newPerTableMetrics(registry, tableId, meters::add,
          List.of(Tag.of(TABLE_ID_TAG_NAME, tid.canonical())));
      getLog().debug("Created {} meters for table id {} in metrics registry.", meters.size(),
          tableId);
      return new TableMetricsInfo<>(tableMetrics, meters);
    });
  }

  public void registerMetrics(MeterRegistry registry) {
    Preconditions.checkState(this.registry == null);
    this.registry = registry;
    if (!activeTableIdTracker.isPerTableMetricsEnabled()) {
      this.allTableMetrics = newAllTablesMetrics(registry, m -> {}, List.of());
    }
  }

  public T getTableMetrics(TableId tableId) {
    Preconditions.checkState(registry != null);

    if (!activeTableIdTracker.isPerTableMetricsEnabled()) {
      return allTableMetrics;
    }

    return getOrCreateTableMetrics(tableId).tableMetrics;
  }

  /**
   * This method will create per table metrics for any tables that are active on this server and
   * currently have no table metrics object in the cache. It will also remove an per table metrics
   * object from the cache that have been inactive for a while or where the table was deleted.
   */
  private void refresh(Set<TableId> currentActive) {
    if (!activeTableIdTracker.isPerTableMetricsEnabled() || registry == null) {
      return;
    }

    currentActive.forEach(tid -> {
      // This registers metrics for the table if none are currently registered and resets the
      // inactiveTime if one exists
      getOrCreateTableMetrics(tid).inactiveTime = null;
    });

    // clean up any tables that have been inactive for a bit
    var iter = perTableMetrics.entrySet().iterator();
    while (iter.hasNext()) {
      var entry = iter.next();
      var tableId = entry.getKey();
      if (!currentActive.contains(tableId)) {
        var tableMetricsInfo = entry.getValue();
        var tableState = context.getTableManager().getTableState(tableId);
        if (tableState == null || tableState == TableState.DELETING) {
          // immediately remove deleted tables
          iter.remove();
          tableMetricsInfo.meters.forEach(registry::remove);
          getLog().debug(
              "Removed {} meters for table id {} from metrics registry because table was deleted.",
              tableMetricsInfo.meters.size(), tableId);
        } else if (tableMetricsInfo.inactiveTime == null) {
          // the first time this table was seen as inactive so start a timer for removal
          tableMetricsInfo.inactiveTime = Timer.startNew();
        } else if (tableMetricsInfo.inactiveTime.hasElapsed(10, TimeUnit.MINUTES)) {
          iter.remove();
          tableMetricsInfo.meters.forEach(registry::remove);
          getLog().debug(
              "Removed {} meters for table id {} from metrics registry because table was inactive.",
              tableMetricsInfo.meters.size(), tableId);
        }
      }
    }
  }
}

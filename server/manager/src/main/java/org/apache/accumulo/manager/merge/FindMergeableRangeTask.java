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
package org.apache.accumulo.manager.merge;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.MERGEABILITY;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.Fate.FateOperation;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.filters.TabletMetadataFilter;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.manager.tableOps.merge.MergeInfo.Operation;
import org.apache.accumulo.manager.tableOps.merge.TableRangeOp;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class FindMergeableRangeTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(FindMergeableRangeTask.class);

  private static final TabletMergeabilityFilter FILTER = new TabletMergeabilityFilter();

  private final Manager manager;

  public FindMergeableRangeTask(Manager manager) {
    this.manager = Objects.requireNonNull(manager);
    log.debug("Creating FindMergeableRangeTask");
  }

  @Override
  public void run() {
    var context = manager.getContext();
    Map<TableId,String> tables = context.getTableIdToNameMap();

    log.debug("Starting FindMergeableRangeTask");

    for (Entry<TableId,String> table : tables.entrySet()) {
      TableId tableId = table.getKey();
      String tableName = table.getValue();

      long maxFileCount =
          context.getTableConfiguration(tableId).getCount(Property.TABLE_MERGE_FILE_MAX);
      long threshold =
          context.getTableConfiguration(tableId).getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
      double mergeabilityThreshold = context.getTableConfiguration(tableId)
          .getFraction(Property.TABLE_MAX_MERGEABILITY_THRESHOLD);
      long maxTotalSize = (long) (threshold * mergeabilityThreshold);

      log.debug("Checking {} for tablets that can be merged", tableName);
      log.debug("maxFileCount: {}, maxTotalSize:{}, splitThreshold:{}, mergeabilityThreshold:{}",
          maxFileCount, maxTotalSize, threshold, mergeabilityThreshold);
      try {
        NamespaceId namespaceId = context.getNamespaceId(tableId);
        var type = FateInstanceType.fromTableId(tableId);

        try (var tablets = context.getAmple().readTablets().forTable(tableId)
            .fetch(PREV_ROW, FILES, MERGEABILITY).filter(FILTER).build()) {

          final MergeableRange current =
              new MergeableRange(manager.getSteadyTime(), maxFileCount, maxTotalSize);

          for (var tm : tablets) {
            log.trace("Checking tablet {}, {}", tm.getExtent(), tm.getTabletMergeability());
            if (!current.add(tm)) {
              submit(current, type, table, namespaceId);
              current.resetAndAdd(tm);
            }
          }

          submit(current, type, table, namespaceId);
        }

      } catch (Exception e) {
        log.error("Failed to generate system merges for {}", tableName, e);
      }
    }

  }

  void submit(MergeableRange range, FateInstanceType type, Entry<TableId,String> table,
      NamespaceId namespaceId) {
    if (range.tabletCount < 2) {
      return;
    }

    log.debug("Table {} found {} tablets that can be merged for table", table.getValue(),
        range.tabletCount);

    TableId tableId = table.getKey();
    String tableName = table.getValue();

    range.startRow = Optional.ofNullable(range.startRow).map(Text::new).orElse(new Text(""));
    range.endRow = Optional.ofNullable(range.endRow).map(Text::new).orElse(new Text(""));

    String startRowStr = StringUtils.defaultIfBlank(range.startRow.toString(), "-inf");
    String endRowStr = StringUtils.defaultIfBlank(range.endRow.toString(), "+inf");
    log.debug("FindMergeableRangeTask: Creating merge op: {} from startRow: {} to endRow: {}",
        tableId, startRowStr, endRowStr);
    var fateId = manager.fate(type).startTransaction();
    String goalMessage = TableOperation.MERGE + " Merge table " + tableName + "(" + tableId
        + ") splits from " + startRowStr + " to " + endRowStr;

    manager.fate(type).seedTransaction(FateOperation.SYSTEM_MERGE, fateId,
        new TraceRepo<>(new TableRangeOp(Operation.SYSTEM_MERGE, namespaceId, tableId,
            range.startRow, range.endRow)),
        true, goalMessage);
  }

  static class MergeableRange {
    final SteadyTime currentTime;
    final long maxFileCount;
    final long maxTotalSize;

    Text startRow;
    Text endRow;
    int tabletCount;
    long totalFileCount;
    long totalFileSize;

    MergeableRange(SteadyTime currentTime, long maxFileCount, long maxTotalSize) {
      this.currentTime = currentTime;
      this.maxFileCount = maxFileCount;
      this.maxTotalSize = maxTotalSize;
    }

    boolean add(TabletMetadata tm) {
      if (validate(tm)) {
        tabletCount++;
        log.trace("Adding tablet {} to MergeableRange", tm.getExtent());
        if (tabletCount == 1) {
          startRow = tm.getPrevEndRow();
        }
        endRow = tm.getEndRow();
        totalFileCount += tm.getFiles().size();
        totalFileSize += tm.getFileSize();
        return true;
      }
      return false;
    }

    private boolean validate(TabletMetadata tm) {
      if (tabletCount > 0) {
        // If this is not the first tablet, then verify its prevEndRow matches
        // the last endRow tracked, the server filter will skip tablets marked as never
        if (!tm.getPrevEndRow().equals(endRow)) {
          return false;
        }
      }

      if (!tm.getTabletMergeability().isMergeable(currentTime)) {
        return false;
      }

      if (totalFileCount + tm.getFiles().size() > maxFileCount) {
        return false;
      }

      return totalFileSize + tm.getFileSize() <= maxTotalSize;
    }

    void resetAndAdd(TabletMetadata tm) {
      reset();
      add(tm);
    }

    void reset() {
      startRow = null;
      endRow = null;
      tabletCount = 0;
      totalFileCount = 0;
      totalFileSize = 0;
    }
  }

  // Filter out never merge tablets to cut down on what we need to check
  // We need steady time to check other tablets which is not available in the filter
  public static class TabletMergeabilityFilter extends TabletMetadataFilter {

    public static final Set<ColumnType> COLUMNS = Sets.immutableEnumSet(MERGEABILITY);

    private final static Predicate<TabletMetadata> IS_NOT_NEVER =
        tabletMetadata -> !tabletMetadata.getTabletMergeability().isNever();

    @Override
    public Set<TabletMetadata.ColumnType> getColumns() {
      return COLUMNS;
    }

    @Override
    protected Predicate<TabletMetadata> acceptTablet() {
      return IS_NOT_NEVER;
    }
  }
}

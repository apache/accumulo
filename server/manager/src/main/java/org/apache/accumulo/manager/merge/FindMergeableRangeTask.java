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
import static org.apache.accumulo.manager.merge.FindMergeableRangeTask.UnmergeableReason.MAX_FILE_COUNT;
import static org.apache.accumulo.manager.merge.FindMergeableRangeTask.UnmergeableReason.MAX_TOTAL_SIZE;
import static org.apache.accumulo.manager.merge.FindMergeableRangeTask.UnmergeableReason.NOT_CONTIGUOUS;
import static org.apache.accumulo.manager.merge.FindMergeableRangeTask.UnmergeableReason.TABLET_MERGEABILITY;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Fate.FateOperation;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.TraceRepo;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.filters.TabletMetadataFilter;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.merge.MergeInfo.Operation;
import org.apache.accumulo.manager.tableOps.merge.TableRangeOp;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * This task is used to scan tables to find ranges of tablets that can be merged together
 * automatically.
 */
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
    Map<TableId,String> tables = context.createTableIdToQualifiedNameMap();

    log.debug("Starting FindMergeableRangeTask");

    for (Entry<TableId,String> table : tables.entrySet()) {
      TableId tableId = table.getKey();
      String tableName = table.getValue();

      // Read the table configuration to compute the max total file size of a mergeable range.
      // The max size is a percentage of the configured split threshold and we do not want
      // to exceed this limit.
      long threshold =
          context.getTableConfiguration(tableId).getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
      double mergeabilityThreshold = context.getTableConfiguration(tableId)
          .getFraction(Property.TABLE_MAX_MERGEABILITY_THRESHOLD);
      if (mergeabilityThreshold <= 0) {
        log.trace("Skipping FindMergeableRangeTask for table {}, {}} is set to {}", tableName,
            Property.TABLE_MAX_MERGEABILITY_THRESHOLD.getKey(), mergeabilityThreshold);
        continue;
      }

      long maxFileCount =
          context.getTableConfiguration(tableId).getCount(Property.TABLE_MERGE_FILE_MAX);
      long maxTotalSize = (long) (threshold * mergeabilityThreshold);

      log.debug("Checking {} for tablets that can be merged", tableName);
      log.trace("maxFileCount: {}, maxTotalSize:{}, splitThreshold:{}, mergeabilityThreshold:{}",
          maxFileCount, maxTotalSize, threshold, mergeabilityThreshold);
      try {
        NamespaceId namespaceId = context.getNamespaceId(tableId);
        var type = FateInstanceType.fromTableId(tableId);

        try (var tablets = context.getAmple().readTablets().forTable(tableId)
            .fetch(PREV_ROW, FILES, MERGEABILITY).filter(FILTER).build()) {

          final MergeableRange current =
              new MergeableRange(tableId, manager.getSteadyTime(), maxFileCount, maxTotalSize);

          for (var tm : tablets) {
            log.trace("Checking tablet {}, {}", tm.getExtent(), tm.getTabletMergeability());
            // If there was an error adding the next tablet to the range then
            // the existing range is complete as we can't add more tablets so
            // submit a merge fate op and reset to find more merge ranges
            current.add(tm).ifPresent(error -> {
              submit(current, type, table, namespaceId);
              current.resetAndAdd(tm);
            });
          }

          // Try and submit any outstanding mergeable tablets
          submit(current, type, table, namespaceId);
        }

      } catch (TableNotFoundException | RuntimeException e) {
        log.error("Failed to generate system merges for {}", tableName, e);
      }
    }

  }

  void submit(MergeableRange range, FateInstanceType type, Entry<TableId,String> table,
      NamespaceId namespaceId) {
    if (range.tabletCount < 2) {
      return;
    }

    TableId tableId = table.getKey();
    String tableName = table.getValue();

    log.debug("Table {} found {} tablets that can be merged for table", tableName,
        range.tabletCount);

    final Text startRow = range.startRow != null ? range.startRow : new Text("");
    final Text endRow = range.endRow != null ? range.endRow : new Text("");

    final String startRowStr = StringUtils.defaultIfBlank(startRow.toString(), "-inf");
    final String endRowStr = StringUtils.defaultIfBlank(endRow.toString(), "+inf");
    log.debug("FindMergeableRangeTask: Creating merge op: {} from startRow: {} to endRow: {}",
        tableId, startRowStr, endRowStr);
    var fateKey = FateKey.forMerge(new KeyExtent(tableId, range.endRow, range.startRow));

    manager.fate(type).seedTransaction(FateOperation.SYSTEM_MERGE, fateKey,
        new TraceRepo<>(
            new TableRangeOp(Operation.SYSTEM_MERGE, namespaceId, tableId, startRow, endRow)),
        true);
  }

  public enum UnmergeableReason {
    NOT_CONTIGUOUS, MAX_FILE_COUNT, MAX_TOTAL_SIZE, TABLET_MERGEABILITY;

    // Cache the Optional() reason objects as we will re-use these over and over
    private static final Optional<UnmergeableReason> NOT_CONTIGUOUS_OPT =
        Optional.of(NOT_CONTIGUOUS);
    private static final Optional<UnmergeableReason> MAX_FILE_COUNT_OPT =
        Optional.of(MAX_FILE_COUNT);
    private static final Optional<UnmergeableReason> MAX_TOTAL_SIZE_OPT =
        Optional.of(MAX_TOTAL_SIZE);
    private static final Optional<UnmergeableReason> TABLET_MERGEABILITY_OPT =
        Optional.of(TABLET_MERGEABILITY);

    public Optional<UnmergeableReason> optional() {
      return switch (this) {
        case NOT_CONTIGUOUS -> NOT_CONTIGUOUS_OPT;
        case MAX_FILE_COUNT -> MAX_FILE_COUNT_OPT;
        case MAX_TOTAL_SIZE -> MAX_TOTAL_SIZE_OPT;
        case TABLET_MERGEABILITY -> TABLET_MERGEABILITY_OPT;
      };
    }
  }

  public static class MergeableRange {
    final SteadyTime currentTime;
    final TableId tableId;
    final long maxFileCount;
    final long maxTotalSize;

    Text startRow;
    Text endRow;
    int tabletCount;
    long totalFileCount;
    long totalFileSize;

    public MergeableRange(TableId tableId, SteadyTime currentTime, long maxFileCount,
        long maxTotalSize) {
      this.tableId = tableId;
      this.currentTime = currentTime;
      this.maxFileCount = maxFileCount;
      this.maxTotalSize = maxTotalSize;
    }

    public Optional<UnmergeableReason> add(TabletMetadata tm) {
      var failure = validate(tm);
      if (failure.isEmpty()) {
        tabletCount++;
        log.trace("Adding tablet {} to MergeableRange", tm.getExtent());
        if (tabletCount == 1) {
          startRow = tm.getPrevEndRow();
        }
        endRow = tm.getEndRow();
        totalFileCount += tm.getFiles().size();
        totalFileSize += tm.getFileSize();
      }
      return failure;
    }

    private Optional<UnmergeableReason> validate(TabletMetadata tm) {
      Preconditions.checkArgument(tableId.equals(tm.getTableId()), "Unexpected tableId seen %s",
          tm.getTableId());

      if (tabletCount > 0) {
        // This is at least the second tablet seen so there should not be a null prevEndRow
        Preconditions.checkState(tm.getPrevEndRow() != null,
            "Unexpected null prevEndRow found for %s", tm.getExtent());
        // If this is not the first tablet, then verify its prevEndRow matches
        // the last endRow tracked, the server filter will skip tablets marked as never
        if (!tm.getPrevEndRow().equals(endRow)) {
          return NOT_CONTIGUOUS.optional();
        }
      }

      if (!tm.getTabletMergeability().isMergeable(currentTime)) {
        return TABLET_MERGEABILITY.optional();
      }

      if (totalFileCount + tm.getFiles().size() > maxFileCount) {
        return MAX_FILE_COUNT.optional();
      }

      if (totalFileSize + tm.getFileSize() > maxTotalSize) {
        return MAX_TOTAL_SIZE.optional();
      }

      return Optional.empty();
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

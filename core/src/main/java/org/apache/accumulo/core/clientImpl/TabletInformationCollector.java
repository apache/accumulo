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
package org.apache.accumulo.core.clientImpl;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;

/**
 * Utility to read tablet information for one or more ranges.
 */
public class TabletInformationCollector {

  private static final Logger log = LoggerFactory.getLogger(TabletInformationCollector.class);

  private TabletInformationCollector() {}

  /**
   * Fetch tablet information for the provided ranges. Ranges will be merged.
   */
  public static Stream<TabletInformation> getTabletInformation(ClientContext context,
      TableId tableId, List<Range> ranges, EnumSet<TabletInformation.Field> fields) {
    var mergedRanges = Range.mergeOverlapping(ranges);

    EnumSet<ColumnType> columns = columnsForFields(fields);

    Set<TServerInstance> liveTserverSet = TabletMetadata.getLiveTServers(context);

    Supplier<Duration> currentTime = Suppliers.memoize(() -> {
      try {
        return context.instanceOperations().getManagerTime();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });

    List<Stream<TabletInformation>> tabletStreams = new ArrayList<>();
    // TODO replace the per-range builds below with a single multirange (maybe new TabletsMetadata
    // logic)
    for (Range range : mergedRanges) {

      Text startRow = range.getStartKey() == null ? null : range.getStartKey().getRow();
      Text endRow = range.getEndKey() == null ? null : range.getEndKey().getRow();
      boolean startInclusive = range.getStartKey() == null || range.isStartKeyInclusive();

      var tm = context.getAmple().readTablets().forTable(tableId)
          .overlapping(startRow, startInclusive, endRow).fetch(columns.toArray(ColumnType[]::new))
          .checkConsistency().build();

      // we need to stop the stream when we have passed the end bound
      Predicate<TabletMetadata> tabletMetadataPredicate = tme -> tme.getPrevEndRow() == null
          || !range.afterEndKey(new Key(tme.getPrevEndRow()).followingKey(PartialKey.ROW));

      Stream<TabletInformation> stream = tm.stream().onClose(tm::close).peek(tme -> {
        if (startRow != null && tme.getEndRow() != null
            && tme.getEndRow().compareTo(startRow) < 0) {
          log.debug("tablet {} is before scan start range: {}", tme.getExtent(), startRow);
          throw new RuntimeException("Bug in ample or this code.");
        }
      }).takeWhile(tabletMetadataPredicate).map(tme -> new TabletInformationImpl(tme,
          () -> TabletState.compute(tme, liveTserverSet).toString(), currentTime));
      tabletStreams.add(stream);
    }

    return tabletStreams.stream().reduce(Stream::concat).orElseGet(Stream::empty);
  }

  /**
   * @return the required {@link ColumnType}s that need to be fetched for the given set of
   *         {@link TabletInformation.Field}
   */
  private static EnumSet<ColumnType> columnsForFields(EnumSet<TabletInformation.Field> fields) {
    EnumSet<ColumnType> columns = EnumSet.noneOf(ColumnType.class);
    columns.add(ColumnType.PREV_ROW);
    if (fields.contains(TabletInformation.Field.FILES)) {
      Collections.addAll(columns, DIR, FILES, LOGS);
    }
    if (fields.contains(TabletInformation.Field.LOCATION)) {
      Collections.addAll(columns, LOCATION, LAST, SUSPEND);
    }
    if (fields.contains(TabletInformation.Field.AVAILABILITY)) {
      columns.add(ColumnType.AVAILABILITY);
    }
    if (fields.contains(TabletInformation.Field.MERGEABILITY)) {
      columns.add(ColumnType.MERGEABILITY);
    }
    return columns;
  }
}

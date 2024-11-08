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
package org.apache.accumulo.core.util.compaction;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang3.Range;

import com.google.common.base.Preconditions;

public class CompactionJobPrioritizer {

  public static final Comparator<CompactionJob> JOB_COMPARATOR =
      Comparator.comparingInt(CompactionJob::getPriority)
          .thenComparingInt(job -> job.getFiles().size()).reversed();

  private static final Map<Pair<TableId,CompactionKind>,Range<Short>> SYSTEM_TABLE_RANGES =
      new HashMap<>();
  private static final Map<Pair<NamespaceId,CompactionKind>,
      Range<Short>> ACCUMULO_NAMESPACE_RANGES = new HashMap<>();

  // Create ranges of possible priority values where each range has
  // 2000 possible values. Priority order is:
  // root table user initiated
  // root table system initiated
  // metadata table user initiated
  // metadata table system initiated
  // other tables in accumulo namespace user initiated
  // other tables in accumulo namespace system initiated
  // user tables that have more files that configured system initiated
  // user tables user initiated
  // user tables system initiated
  static final Range<Short> ROOT_TABLE_USER = Range.of((short) 30768, (short) 32767);
  static final Range<Short> ROOT_TABLE_SYSTEM = Range.of((short) 28768, (short) 30767);

  static final Range<Short> METADATA_TABLE_USER = Range.of((short) 26768, (short) 28767);
  static final Range<Short> METADATA_TABLE_SYSTEM = Range.of((short) 24768, (short) 26767);

  static final Range<Short> SYSTEM_NS_USER = Range.of((short) 22768, (short) 24767);
  static final Range<Short> SYSTEM_NS_SYSTEM = Range.of((short) 20768, (short) 22767);

  static final Range<Short> TABLE_OVER_SIZE = Range.of((short) 18768, (short) 20767);

  static final Range<Short> USER_TABLE_USER = Range.of((short) 1, (short) 18767);
  static final Range<Short> USER_TABLE_SYSTEM = Range.of((short) -32768, (short) 0);

  static {
    // root table
    SYSTEM_TABLE_RANGES.put(new Pair<>(AccumuloTable.ROOT.tableId(), CompactionKind.USER),
        ROOT_TABLE_USER);
    SYSTEM_TABLE_RANGES.put(new Pair<>(AccumuloTable.ROOT.tableId(), CompactionKind.SYSTEM),
        ROOT_TABLE_SYSTEM);

    // metadata table
    SYSTEM_TABLE_RANGES.put(new Pair<>(AccumuloTable.METADATA.tableId(), CompactionKind.USER),
        METADATA_TABLE_USER);
    SYSTEM_TABLE_RANGES.put(new Pair<>(AccumuloTable.METADATA.tableId(), CompactionKind.SYSTEM),
        METADATA_TABLE_SYSTEM);

    // metadata table
    ACCUMULO_NAMESPACE_RANGES.put(new Pair<>(Namespace.ACCUMULO.id(), CompactionKind.USER),
        SYSTEM_NS_USER);
    ACCUMULO_NAMESPACE_RANGES.put(new Pair<>(Namespace.ACCUMULO.id(), CompactionKind.SYSTEM),
        SYSTEM_NS_SYSTEM);
  }

  @SuppressWarnings("deprecation")
  public static short createPriority(final NamespaceId nsId, final TableId tableId,
      final CompactionKind kind, final int totalFiles, final int compactingFiles,
      final int maxFilesPerTablet) {

    Objects.requireNonNull(nsId, "nsId cannot be null");
    Objects.requireNonNull(tableId, "tableId cannot be null");
    Preconditions.checkArgument(totalFiles >= 0, "totalFiles is negative %s", totalFiles);
    Preconditions.checkArgument(compactingFiles >= 0, "compactingFiles is negative %s",
        compactingFiles);

    final Function<Range<Short>,Short> normalPriorityFunction = new Function<>() {
      @Override
      public Short apply(Range<Short> f) {
        return (short) Math.min(f.getMaximum(), f.getMinimum() + totalFiles + compactingFiles);
      }
    };

    final Function<Range<Short>,Short> tabletOverSizeFunction = new Function<>() {
      @Override
      public Short apply(Range<Short> f) {
        return (short) Math.min(f.getMaximum(),
            f.getMinimum() + compactingFiles + (totalFiles - maxFilesPerTablet));
      }
    };

    // Handle the case of a CHOP compaction. For the purposes of determining
    // a priority, treat them as a USER compaction.
    CompactionKind calculationKind = kind;
    if (kind == CompactionKind.SELECTOR) {
      calculationKind = CompactionKind.SYSTEM;
    }

    Range<Short> range = null;
    Function<Range<Short>,Short> func = normalPriorityFunction;
    if (Namespace.ACCUMULO.id() == nsId) {
      // Handle system tables
      range = SYSTEM_TABLE_RANGES.get(new Pair<>(tableId, calculationKind));
      if (range == null) {
        range = ACCUMULO_NAMESPACE_RANGES.get(new Pair<>(nsId, calculationKind));
      }
    } else {
      // Handle user tables
      if (totalFiles > maxFilesPerTablet && calculationKind == CompactionKind.SYSTEM) {
        range = TABLE_OVER_SIZE;
        func = tabletOverSizeFunction;
      } else if (calculationKind == CompactionKind.SYSTEM) {
        range = USER_TABLE_SYSTEM;
      } else {
        range = USER_TABLE_USER;
      }
    }

    if (range == null) {
      throw new IllegalStateException(
          "Error calculating compaction priority for table: " + tableId);
    }
    return func.apply(range);

  }

}

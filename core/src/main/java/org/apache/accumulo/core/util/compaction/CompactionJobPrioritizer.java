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

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.Pair;

import com.google.common.base.Preconditions;

public class CompactionJobPrioritizer {

  public static final Comparator<CompactionJob> JOB_COMPARATOR =
      Comparator.comparingInt(CompactionJob::getPriority)
          .thenComparingInt(job -> job.getFiles().size()).reversed();

  private static final Map<Pair<TableId,CompactionKind>,Pair<Short,Short>> SYSTEM_TABLE_RANGES =
      new HashMap<>();
  private static final Map<Pair<NamespaceId,CompactionKind>,
      Pair<Short,Short>> ACCUMULO_NAMESPACE_RANGES = new HashMap<>();

  // Create ranges of possible priority values where each range has
  // 2000 possible values. Give higher priority to tables where
  // they have more files than allowed, user compactions over system
  // compactions, root table over metadata table, metadata table over
  // other system tables and user tables.
  private static final Short TABLE_OVER_SIZE_MAX = Short.MAX_VALUE;
  private static final Short ROOT_TABLE_USER_MAX = Short.MAX_VALUE - 2001;
  private static final Short ROOT_TABLE_SYSTEM_MAX = (short) (ROOT_TABLE_USER_MAX - 2001);
  private static final Short META_TABLE_USER_MAX = (short) (ROOT_TABLE_SYSTEM_MAX - 2001);
  private static final Short META_TABLE_SYSTEM_MAX = (short) (META_TABLE_USER_MAX - 2001);
  private static final Short SYSTEM_NS_USER_MAX = (short) (META_TABLE_SYSTEM_MAX - 2001);
  private static final Short SYSTEM_NS_SYSTEM_MAX = (short) (SYSTEM_NS_USER_MAX - 2001);
  private static final Short USER_TABLE_USER_MAX = (short) (SYSTEM_NS_SYSTEM_MAX - 2001);
  private static final Short USER_TABLE_SYSTEM_MAX = 0;

  static final Pair<Short,Short> TABLE_OVER_SIZE =
      new Pair<>((short) (ROOT_TABLE_USER_MAX + 1), TABLE_OVER_SIZE_MAX);

  static final Pair<Short,Short> ROOT_TABLE_USER =
      new Pair<>((short) (ROOT_TABLE_SYSTEM_MAX + 1), ROOT_TABLE_USER_MAX);
  static final Pair<Short,Short> ROOT_TABLE_SYSTEM =
      new Pair<>((short) (META_TABLE_USER_MAX + 1), ROOT_TABLE_SYSTEM_MAX);

  static final Pair<Short,Short> METADATA_TABLE_USER =
      new Pair<>((short) (META_TABLE_SYSTEM_MAX + 1), META_TABLE_USER_MAX);
  static final Pair<Short,Short> METADATA_TABLE_SYSTEM =
      new Pair<>((short) (SYSTEM_NS_USER_MAX + 1), META_TABLE_SYSTEM_MAX);

  static final Pair<Short,Short> SYSTEM_NS_USER =
      new Pair<>((short) (SYSTEM_NS_SYSTEM_MAX + 1), SYSTEM_NS_USER_MAX);
  static final Pair<Short,Short> SYSTEM_NS_SYSTEM =
      new Pair<>((short) (USER_TABLE_USER_MAX + 1), SYSTEM_NS_SYSTEM_MAX);

  static final Pair<Short,Short> USER_TABLE_USER =
      new Pair<>((short) (USER_TABLE_SYSTEM_MAX + 1), USER_TABLE_USER_MAX);
  static final Pair<Short,Short> USER_TABLE_SYSTEM =
      new Pair<>(Short.MIN_VALUE, USER_TABLE_SYSTEM_MAX);

  static {
    // root table
    SYSTEM_TABLE_RANGES.put(new Pair<>(RootTable.ID, CompactionKind.USER), ROOT_TABLE_USER);
    SYSTEM_TABLE_RANGES.put(new Pair<>(RootTable.ID, CompactionKind.SYSTEM), ROOT_TABLE_SYSTEM);

    // metadata table
    SYSTEM_TABLE_RANGES.put(new Pair<>(MetadataTable.ID, CompactionKind.USER), METADATA_TABLE_USER);
    SYSTEM_TABLE_RANGES.put(new Pair<>(MetadataTable.ID, CompactionKind.SYSTEM),
        METADATA_TABLE_SYSTEM);

    // metadata table
    ACCUMULO_NAMESPACE_RANGES.put(new Pair<>(Namespace.ACCUMULO.id(), CompactionKind.USER),
        SYSTEM_NS_USER);
    ACCUMULO_NAMESPACE_RANGES.put(new Pair<>(Namespace.ACCUMULO.id(), CompactionKind.SYSTEM),
        SYSTEM_NS_SYSTEM);
  }

  public static short createPriority(NamespaceId nsId, TableId tableId, CompactionKind kind,
      int totalFiles, int compactingFiles, int maxFilesPerTablet) {

    Objects.requireNonNull(nsId, "nsId cannot be null");
    Objects.requireNonNull(tableId, "tableId cannot be null");
    Preconditions.checkArgument(totalFiles >= 0, "totalFiles is negative %s", totalFiles);
    Preconditions.checkArgument(compactingFiles >= 0, "compactingFiles is negative %s",
        compactingFiles);

    if (totalFiles > maxFilesPerTablet && kind == CompactionKind.SYSTEM) {
      int priority =
          TABLE_OVER_SIZE.getFirst() + compactingFiles + (totalFiles - maxFilesPerTablet);
      if (priority > Short.MAX_VALUE) {
        return Short.MAX_VALUE;
      }
      return (short) priority;
    } else {
      Pair<Short,Short> range = null;
      if (Namespace.ACCUMULO.id() == nsId) {
        range = SYSTEM_TABLE_RANGES.get(new Pair<>(tableId, kind));
        if (range == null) {
          range = ACCUMULO_NAMESPACE_RANGES.get(new Pair<>(nsId, kind));
        }
      } else {
        if (kind == CompactionKind.SYSTEM) {
          range = USER_TABLE_SYSTEM;
        } else {
          range = USER_TABLE_USER;
        }
      }
      return (short) Math.min(range.getSecond(), range.getFirst() + totalFiles + compactingFiles);
    }

  }

}

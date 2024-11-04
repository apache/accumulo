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

  public static enum Condition {
    NONE, TABLET_OVER_SIZE;
  }

  public static final Comparator<CompactionJob> JOB_COMPARATOR =
      Comparator.comparingInt(CompactionJob::getPriority)
          .thenComparingInt(job -> job.getFiles().size()).reversed();
  
  private static final Map<Pair<TableId, CompactionKind>, Pair<Short,Short>> SYSTEM_TABLE_RANGES = new HashMap<>();
  private static final Map<Pair<NamespaceId, CompactionKind>, Pair<Short,Short>> ACCUMULO_NAMESPACE_RANGES = new HashMap<>();
  
  private static final Pair<Short, Short> TABLE_OVER_SIZE = new Pair<>((short) (Short.MAX_VALUE - 2000), Short.MAX_VALUE);
  private static final Pair<Short, Short> ROOT_TABLE_USER = new Pair<>((short) (Short.MAX_VALUE - 4000), (short) (Short.MAX_VALUE - 2001));
  private static final Pair<Short, Short> ROOT_TABLE_SYSTEM = new Pair<>((short) (Short.MAX_VALUE - 6000), (short) (Short.MAX_VALUE - 4001));
  private static final Pair<Short, Short> METADATA_TABLE_USER = new Pair<>((short) (Short.MAX_VALUE - 6000), (short) (Short.MAX_VALUE - 4001));
  private static final Pair<Short, Short> METADATA_TABLE_SYSTEM = new Pair<>((short) (Short.MAX_VALUE - 8000), (short) (Short.MAX_VALUE - 6001));
  private static final Pair<Short, Short> SYSTEM_NS_USER = new Pair<>((short) (Short.MAX_VALUE - 10000), (short) (Short.MAX_VALUE - 8001));
  private static final Pair<Short, Short> SYSTEM_NS_SYSTEM = new Pair<>((short) (Short.MAX_VALUE - 12000), (short) (Short.MAX_VALUE - 10001));
  
  private static final Pair<Short, Short> USER_TABLE_USER = new Pair<>(Short.MIN_VALUE, Short.MIN_VALUE);
  private static final Pair<Short, Short> USER_TABLE_SYSTEM = new Pair<>(Short.MIN_VALUE, Short.MIN_VALUE);
  
  static {
    // root table
    SYSTEM_TABLE_RANGES.put(new Pair<>(RootTable.ID, CompactionKind.USER), ROOT_TABLE_USER);
    SYSTEM_TABLE_RANGES.put(new Pair<>(RootTable.ID, CompactionKind.SYSTEM), ROOT_TABLE_SYSTEM);

    // metadata table
    SYSTEM_TABLE_RANGES.put(new Pair<>(MetadataTable.ID, CompactionKind.USER), METADATA_TABLE_USER);
    SYSTEM_TABLE_RANGES.put(new Pair<>(MetadataTable.ID,  CompactionKind.SYSTEM), METADATA_TABLE_SYSTEM);

    // metadata table
    ACCUMULO_NAMESPACE_RANGES.put(new Pair<>(Namespace.ACCUMULO.id(), CompactionKind.USER), SYSTEM_NS_USER);
    ACCUMULO_NAMESPACE_RANGES.put(new Pair<>(Namespace.ACCUMULO.id(), CompactionKind.SYSTEM), SYSTEM_NS_SYSTEM);
  }

  public static short createPriority(NamespaceId nsId, TableId tableId, CompactionKind kind, int totalFiles, int compactingFiles,
      Condition condition, int maxFilesPerTablet) {

    Objects.requireNonNull(nsId, "nsId cannot be null");
    Objects.requireNonNull(tableId, "tableId cannot be null");
    Preconditions.checkArgument(totalFiles >= 0, "totalFiles is negative %s", totalFiles);
    Preconditions.checkArgument(compactingFiles >= 0, "compactingFiles is negative %s", compactingFiles);
    Objects.requireNonNull(condition, "condition cannot be null");

    Pair<Short,Short> range = null;
    
    switch (condition) {
      case NONE:
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
      case TABLET_OVER_SIZE:
        range = TABLE_OVER_SIZE;
        return (short) Math.min(range.getSecond(), range.getFirst() + totalFiles + compactingFiles - maxFilesPerTablet);
      default:
        throw new IllegalStateException("Unhandled condition type: " + condition);
    }
    
  }

}

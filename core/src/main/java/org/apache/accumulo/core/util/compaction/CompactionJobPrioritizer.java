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

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;

import com.google.common.base.Preconditions;

public class CompactionJobPrioritizer {

  public static final Comparator<CompactionJob> JOB_COMPARATOR =
      Comparator.comparingInt(CompactionJob::getPriority)
          .thenComparingInt(job -> job.getFiles().size()).reversed();

  private static final short ROOT_USER_MAX = Short.MAX_VALUE;
  private static final short ROOT_USER_MIN = ROOT_USER_MAX - 1000;
  private static final short ROOT_SYSTEM_MAX = ROOT_USER_MIN - 1;
  private static final short ROOT_SYSTEM_MIN = ROOT_SYSTEM_MAX - 1000;
  private static final short METADATA_USER_MAX = ROOT_SYSTEM_MIN - 1;
  private static final short METADATA_USER_MIN = METADATA_USER_MAX - 1000;
  private static final short METADATA_SYSTEM_MAX = METADATA_USER_MIN - 1;
  private static final short METADATA_SYSTEM_MIN = METADATA_SYSTEM_MAX - 1000;
  // ACCUMULO_OTHER is intended for tables in the accumulo namespace that are not accumulo.root or
  // accumulo.metadata
  private static final short ACCUMULO_OTHER_USER_MAX = METADATA_SYSTEM_MIN - 1;
  private static final short ACCUMULO_OTHER_USER_MIN = ACCUMULO_OTHER_USER_MAX - 1000;
  private static final short ACCUMULO_OTHER_SYSTEM_MAX = ACCUMULO_OTHER_USER_MIN - 1;
  private static final short ACCUMULO_OTHER_SYSTEM_MIN = ACCUMULO_OTHER_SYSTEM_MAX - 1000;
  private static final short USER_USER_MAX = ACCUMULO_OTHER_SYSTEM_MIN - 1;
  private static final short USER_USER_MIN = (USER_USER_MAX + Short.MIN_VALUE) / 2;
  private static final short USER_SYSTEM_MAX = USER_USER_MIN - 1;
  private static final short USER_SYSTEM_MIN = Short.MIN_VALUE;

  public static short createPriority(TableId tableId, CompactionKind kind, int totalFiles,
      int compactingFiles) {

    Preconditions.checkArgument(totalFiles >= 0, "totalFiles is negative %s", totalFiles);
    Preconditions.checkArgument(compactingFiles >= 0, "compactingFiles is negative %s",
        compactingFiles);

    int min;
    int max;

    // Check if the table is in the accumulo namespace
    if (AccumuloTable.allTableIds().contains(tableId)) {
      if (tableId.equals(AccumuloTable.ROOT.tableId())) {
        if (kind == CompactionKind.USER) {
          min = ROOT_USER_MIN;
          max = ROOT_USER_MAX;
        } else {
          min = ROOT_SYSTEM_MIN;
          max = ROOT_SYSTEM_MAX;
        }
      } else if (tableId.equals(AccumuloTable.METADATA.tableId())) {
        if (kind == CompactionKind.USER) {
          min = METADATA_USER_MIN;
          max = METADATA_USER_MAX;
        } else {
          min = METADATA_SYSTEM_MIN;
          max = METADATA_SYSTEM_MAX;
        }
      } else {
        // This is a table in the accumulo namespace that is not root or metadata table.
        if (kind == CompactionKind.USER) {
          min = ACCUMULO_OTHER_USER_MIN;
          max = ACCUMULO_OTHER_USER_MAX;
        } else {
          min = ACCUMULO_OTHER_SYSTEM_MIN;
          max = ACCUMULO_OTHER_SYSTEM_MAX;
        }
      }
    } else {
      if (kind == CompactionKind.USER) {
        min = USER_USER_MIN;
        max = USER_USER_MAX;
      } else {
        min = USER_SYSTEM_MIN;
        max = USER_SYSTEM_MAX;
      }
    }

    return (short) Math.min(max, min + totalFiles + compactingFiles);
  }
}

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
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;

import com.google.common.base.Preconditions;

public class CompactionJobPrioritizer {

  public static final Comparator<CompactionJob> JOB_COMPARATOR =
      Comparator.comparingInt(CompactionJob::getPriority)
          .thenComparingInt(job -> job.getFiles().size()).reversed();

  public static short createPriority(TableId tableId, CompactionKind kind, int totalFiles,
      int compactingFiles) {

    Preconditions.checkArgument(totalFiles >= 0, "totalFiles is negative %s", totalFiles);
    Preconditions.checkArgument(compactingFiles >= 0, "compactingFiles is negative %s",
        compactingFiles);

    // This holds the two bits used to encode the priority of the table.
    int tablePrefix;

    switch (Ample.DataLevel.of(tableId)) {
      case ROOT:
        tablePrefix = 0b0100_0000_0000_0000;
        break;
      case METADATA:
        tablePrefix = 0;
        break;
      case USER:
        tablePrefix = 0b1000_0000_0000_0000;
        break;
      default:
        throw new IllegalStateException("Unknown data level" + Ample.DataLevel.of(tableId));
    }

    int kindBit;

    if (kind == CompactionKind.USER) {
      kindBit = 0b0010_0000_0000_0000;
    } else {
      kindBit = 0;
    }

    int fileBits = Math.min(0b0001_1111_1111_1111, totalFiles + compactingFiles);

    // Encode the table, kind, and files into a short using two bits for the table, one bit for the
    // kind, and 13 bits for the file count.
    return (short) (tablePrefix | kindBit | fileBits);
  }

}

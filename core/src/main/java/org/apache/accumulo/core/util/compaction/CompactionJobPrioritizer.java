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

import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;

public class CompactionJobPrioritizer {

  public static final Comparator<CompactionJob> JOB_COMPARATOR =
      Comparator.comparingInt(CompactionJob::getPriority)
          .thenComparingInt(job -> job.getFiles().size()).reversed();

  public static short createPriority(CompactionKind kind, int totalFiles, int compactingFiles) {

    int prio = totalFiles + compactingFiles;

    switch (kind) {
      case USER:
      case CHOP:
        // user-initiated compactions will have a positive priority
        // based on number of files
        if (prio > Short.MAX_VALUE) {
          return Short.MAX_VALUE;
        }
        return (short) prio;
      case SELECTOR:
      case SYSTEM:
        // system-initiated compactions will have a negative priority
        // starting at -32768 and increasing based on number of files
        // maxing out at -1
        if (prio > Short.MAX_VALUE) {
          return -1;
        } else {
          return (short) (Short.MIN_VALUE + prio);
        }
      default:
        throw new AssertionError("Unknown kind " + kind);
    }
  }

}

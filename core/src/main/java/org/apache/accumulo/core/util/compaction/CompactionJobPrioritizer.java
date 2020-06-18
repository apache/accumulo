/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
      Comparator.comparingLong(CompactionJob::getPriority)
          .thenComparingInt(job -> job.getFiles().size()).reversed();

  public static long createPriority(CompactionKind kind, int totalFiles) {
    long kindPrio;

    switch (kind) {
      case USER:
        kindPrio = 4;
        break;
      case SELECTOR:
        kindPrio = 2;
        break;
      case CHOP:
        kindPrio = 3;
        break;
      case SYSTEM:
        kindPrio = 1;
        break;
      default:
        throw new AssertionError("Unknown kind " + kind);
    }

    return (kindPrio << 56) | totalFiles;
  }

}

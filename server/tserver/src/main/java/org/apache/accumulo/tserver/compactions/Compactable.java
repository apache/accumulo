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
package org.apache.accumulo.tserver.compactions;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;

/**
 * Interface between compaction service and tablet.
 */
public interface Compactable {

  public static class Files {

    public final Set<CompactableFile> allFiles;
    public final Set<CompactableFile> candidates;
    public final Collection<CompactionJob> compacting;
    public final Map<String,String> executionHints;

    public Files(Set<CompactableFile> allFiles, Set<CompactableFile> candidates,
        Collection<CompactionJob> runningJobsCopy) {
      this(allFiles, candidates, runningJobsCopy, Map.of());
    }

    public Files(Set<CompactableFile> allFiles, Set<CompactableFile> candidates,
        Collection<CompactionJob> runningJobsCopy, Map<String,String> executionHints) {
      this.allFiles = allFiles;
      this.candidates = candidates;
      this.compacting = runningJobsCopy;
      this.executionHints = executionHints;
    }

    @Override
    public String toString() {
      return "Files [allFiles=" + allFiles + ", candidates=" + candidates + ", compacting="
          + compacting + ", hints=" + executionHints + "]";
    }
  }

  TableId getTableId();

  KeyExtent getExtent();

  Optional<Files> getFiles(CompactionServiceId service, CompactionKind kind);

  void compact(CompactionServiceId service, CompactionJob job, RateLimiter readLimiter,
      RateLimiter writeLimiter, long queuedTime);

  CompactionServiceId getConfiguredService(CompactionKind kind);

  double getCompactionRatio();

  ExternalCompactionJob reserveExternalCompaction(CompactionServiceId service, CompactionJob job,
      String compactorId, ExternalCompactionId externalCompactionId);

  void commitExternalCompaction(ExternalCompactionId extCompactionId, long fileSize, long entries);

  void externalCompactionFailed(ExternalCompactionId ecid);

  boolean isActive(ExternalCompactionId ecid);

  void getExternalCompactionIds(Consumer<ExternalCompactionId> tmp);
}

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;

import com.google.common.base.Preconditions;

public class CompactionPlanImpl implements CompactionPlan {

  private final Collection<CompactionJob> jobs;

  private CompactionPlanImpl(Collection<CompactionJob> jobs) {
    this.jobs = List.copyOf(jobs);
  }

  @Override
  public Collection<CompactionJob> getJobs() {
    return jobs;
  }

  @Override
  public String toString() {
    return "jobs: " + jobs;
  }

  public static class BuilderImpl implements CompactionPlan.Builder {

    private final CompactionKind kind;
    private ArrayList<CompactionJob> jobs = new ArrayList<>();
    private final Set<CompactableFile> allFiles;
    private final Set<CompactableFile> seenFiles = new HashSet<>();
    private final Set<CompactableFile> candidates;

    public BuilderImpl(CompactionKind kind, Set<CompactableFile> allFiles,
        Set<CompactableFile> candidates) {
      this.kind = kind;
      this.allFiles = allFiles;
      this.candidates = candidates;
    }

    @Override
    public Builder addJob(short priority, CompactionExecutorId executor,
        Collection<CompactableFile> files) {
      Set<CompactableFile> filesSet =
          files instanceof Set ? (Set<CompactableFile>) files : Set.copyOf(files);

      Preconditions.checkArgument(Collections.disjoint(filesSet, seenFiles),
          "Job files overlaps with previous job %s %s", files, jobs);
      Preconditions.checkArgument(candidates.containsAll(filesSet),
          "Job files are not compaction candidates %s %s", files, candidates);

      seenFiles.addAll(filesSet);

      jobs.add(new CompactionJobImpl(priority, executor, filesSet, kind,
          Optional.of(filesSet.equals(allFiles))));
      return this;
    }

    @Override
    public CompactionPlan build() {
      return new CompactionPlanImpl(jobs);
    }
  }

}

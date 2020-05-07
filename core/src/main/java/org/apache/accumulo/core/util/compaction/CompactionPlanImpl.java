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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;

import com.google.common.base.Preconditions;

public class CompactionPlanImpl implements CompactionPlan {

  private final Collection<CompactionJob> jobs;

  CompactionPlanImpl(Collection<CompactionJob> jobs) {
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

    private CompactionKind kind;
    private ArrayList<CompactionJob> jobs = new ArrayList<>();
    private Set<CompactableFile> allFiles;
    private Set<CompactableFile> seenFiles = new HashSet<>();

    public BuilderImpl(CompactionKind kind, Set<CompactableFile> allFiles) {
      this.kind = kind;
      this.allFiles = allFiles;
    }

    @Override
    public Builder addJob(long priority, CompactionExecutorId executor,
        Collection<CompactableFile> files) {
      Set<CompactableFile> filesSet =
          files instanceof Set ? (Set<CompactableFile>) files : Set.copyOf(files);

      Preconditions.checkArgument(Collections.disjoint(filesSet, seenFiles),
          "Job files overlaps with previous job %s %s", files, jobs);
      Preconditions.checkArgument(allFiles.containsAll(filesSet),
          "Job files are not tablet files %s %s", files, allFiles);

      seenFiles.addAll(filesSet);

      jobs.add(
          new CompactionJobImpl(priority, executor, filesSet, kind, filesSet.equals(allFiles)));
      return this;
    }

    @Override
    public CompactionPlan build() {
      return new CompactionPlanImpl(jobs);
    }
  }

}

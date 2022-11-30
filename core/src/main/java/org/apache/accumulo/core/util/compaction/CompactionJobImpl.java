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

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;

/**
 * An immutable object that describes what files to compact and where to compact them.
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public class CompactionJobImpl implements CompactionJob {

  private final short priority;
  private final CompactionExecutorId executor;
  private final Set<CompactableFile> files;
  private final CompactionKind kind;
  // Tracks if a job selected all of the tablets files that existed at the time the job was created.
  private final Optional<Boolean> jobSelectedAll;

  /**
   *
   * @param jobSelectedAll This parameters only needs to be non-empty for job objects that are used
   *        to start compaction. After a job is running, its not used. So when a job object is
   *        recreated for a running external compaction this parameter can be empty.
   */
  public CompactionJobImpl(short priority, CompactionExecutorId executor,
      Collection<CompactableFile> files, CompactionKind kind, Optional<Boolean> jobSelectedAll) {
    this.priority = priority;
    this.executor = Objects.requireNonNull(executor);
    this.files = Set.copyOf(files);
    this.kind = Objects.requireNonNull(kind);
    this.jobSelectedAll = Objects.requireNonNull(jobSelectedAll);
  }

  @Override
  public short getPriority() {
    return priority;
  }

  /**
   * @return The executor to run the job.
   */
  @Override
  public CompactionExecutorId getExecutor() {
    return executor;
  }

  /**
   * @return The files to compact
   */
  @Override
  public Set<CompactableFile> getFiles() {
    return files;
  }

  /**
   * @return The kind of compaction this is.
   */
  @Override
  public CompactionKind getKind() {
    return kind;
  }

  @Override
  public int hashCode() {
    return Objects.hash(priority, executor, files, kind);
  }

  public boolean selectedAll() {
    return jobSelectedAll.get();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompactionJobImpl) {
      CompactionJobImpl ocj = (CompactionJobImpl) o;

      return priority == ocj.priority && executor.equals(ocj.executor) && files.equals(ocj.files)
          && kind == ocj.kind;
    }

    return false;
  }

  @Override
  public String toString() {
    return "CompactionJob [priority=" + priority + ", executor=" + executor + ", files=" + files
        + ", kind=" + kind + "]";
  }

}

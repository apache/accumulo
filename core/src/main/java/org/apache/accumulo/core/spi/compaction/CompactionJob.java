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
package org.apache.accumulo.core.spi.compaction;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;

/**
 * An immutable object that describes what files to compact and where to compact them.
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public class CompactionJob {

  private final long priority;
  private final CompactionExecutorId executor;
  private final Set<CompactableFile> files;
  private final CompactionKind kind;

  public CompactionJob(long priority, CompactionExecutorId executor,
      Collection<CompactableFile> files, CompactionKind kind) {
    this.priority = priority;
    this.executor = executor;
    this.files = Set.copyOf(files);
    this.kind = kind;
  }

  public long getPriority() {
    return priority;
  }

  /**
   * @return The executor to run the job.
   */
  public CompactionExecutorId getExecutor() {
    return executor;
  }

  /**
   * @return The files to compact
   */
  public Set<CompactableFile> getFiles() {
    return files;
  }

  /**
   * @return The kind of compaction this is.
   */
  public CompactionKind getKind() {
    return kind;
  }

  @Override
  public int hashCode() {
    return Objects.hash(priority, executor, files, kind);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompactionJob) {
      CompactionJob ocj = (CompactionJob) o;

      return priority == ocj.priority && executor.equals(ocj.executor) && files.equals(ocj.files)
          && kind == ocj.kind;
    }

    return false;
  }

  @Override
  public String toString() {
    return "CompactionJob [priority=" + priority + ", executor=" + executor + ", files=" + files
        + ", type=" + kind + "]";
  }

}

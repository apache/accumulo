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
package org.apache.accumulo.core.metadata.schema;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;

public class ExternalCompactionMetadata {

  private static final Gson GSON = new Gson();

  private final Set<StoredTabletFile> jobFiles;
  private final Set<StoredTabletFile> nextFiles;
  private final TabletFile compactTmpName;
  private final String compactorId;
  private final CompactionKind kind;
  private final short priority;
  private final CompactionExecutorId ceid;
  private final boolean propagateDeletes;
  private final boolean initiallySelectedAll;
  private final Long compactionId;

  public ExternalCompactionMetadata(Set<StoredTabletFile> jobFiles, Set<StoredTabletFile> nextFiles,
      TabletFile compactTmpName, String compactorId, CompactionKind kind, short priority,
      CompactionExecutorId ceid, boolean propagateDeletes, boolean initiallySelectedAll,
      Long compactionId) {
    if (!initiallySelectedAll && !propagateDeletes
        && (kind == CompactionKind.SELECTOR || kind == CompactionKind.USER)) {
      throw new IllegalArgumentException(
          "When user or selector compactions do not propagate deletes, it's expected that all "
              + "files were selected initially.");
    }
    this.jobFiles = Objects.requireNonNull(jobFiles);
    this.nextFiles = Objects.requireNonNull(nextFiles);
    this.compactTmpName = Objects.requireNonNull(compactTmpName);
    this.compactorId = Objects.requireNonNull(compactorId);
    this.kind = Objects.requireNonNull(kind);
    this.priority = priority;
    this.ceid = Objects.requireNonNull(ceid);
    this.propagateDeletes = propagateDeletes;
    this.initiallySelectedAll = initiallySelectedAll;
    this.compactionId = compactionId;
  }

  public Set<StoredTabletFile> getJobFiles() {
    return jobFiles;
  }

  public Set<StoredTabletFile> getNextFiles() {
    return nextFiles;
  }

  public TabletFile getCompactTmpName() {
    return compactTmpName;
  }

  public String getCompactorId() {
    return compactorId;
  }

  public CompactionKind getKind() {
    return kind;
  }

  public short getPriority() {
    return priority;
  }

  public CompactionExecutorId getCompactionExecutorId() {
    return ceid;
  }

  public boolean getPropagateDeletes() {
    return propagateDeletes;
  }

  public boolean getInitiallySelecteAll() {
    return initiallySelectedAll;
  }

  public Long getCompactionId() {
    return compactionId;
  }

  // This class is used to serialize and deserialize this class using GSon. Any changes to this
  // class must consider persisted data.
  private static class GSonData {
    List<String> inputs;
    List<String> nextFiles;
    String tmp;
    String compactor;
    String kind;
    String executorId;
    short priority;
    boolean propDels;
    boolean selectedAll;
    Long compactionId;
  }

  public String toJson() {
    GSonData jData = new GSonData();

    jData.inputs = jobFiles.stream().map(StoredTabletFile::getMetaUpdateDelete).collect(toList());
    jData.nextFiles =
        nextFiles.stream().map(StoredTabletFile::getMetaUpdateDelete).collect(toList());
    jData.tmp = compactTmpName.getMetaInsert();
    jData.compactor = compactorId;
    jData.kind = kind.name();
    jData.executorId = ((CompactionExecutorIdImpl) ceid).getExternalName();
    jData.priority = priority;
    jData.propDels = propagateDeletes;
    jData.selectedAll = initiallySelectedAll;
    jData.compactionId = compactionId;
    return GSON.toJson(jData);
  }

  public static ExternalCompactionMetadata fromJson(String json) {
    GSonData jData = GSON.fromJson(json, GSonData.class);

    return new ExternalCompactionMetadata(
        jData.inputs.stream().map(StoredTabletFile::new).collect(toSet()),
        jData.nextFiles.stream().map(StoredTabletFile::new).collect(toSet()),
        new TabletFile(new Path(jData.tmp)), jData.compactor, CompactionKind.valueOf(jData.kind),
        jData.priority, CompactionExecutorIdImpl.externalId(jData.executorId), jData.propDels,
        jData.selectedAll, jData.compactionId);
  }

  @Override
  public String toString() {
    return toJson();
  }
}

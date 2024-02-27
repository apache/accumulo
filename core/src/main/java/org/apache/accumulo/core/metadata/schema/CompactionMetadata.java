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
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;

public class CompactionMetadata {

  private final Set<StoredTabletFile> jobFiles;
  private final ReferencedTabletFile compactTmpName;
  private final String compactorId;
  private final CompactionKind kind;
  private final short priority;
  private final CompactorGroupId cgid;
  private final boolean propagateDeletes;
  private final FateId fateId;

  public CompactionMetadata(Set<StoredTabletFile> jobFiles, ReferencedTabletFile compactTmpName,
      String compactorId, CompactionKind kind, short priority, CompactorGroupId ceid,
      boolean propagateDeletes, FateId fateId) {
    this.jobFiles = Objects.requireNonNull(jobFiles);
    this.compactTmpName = Objects.requireNonNull(compactTmpName);
    this.compactorId = Objects.requireNonNull(compactorId);
    this.kind = Objects.requireNonNull(kind);
    this.priority = priority;
    this.cgid = Objects.requireNonNull(ceid);
    this.propagateDeletes = propagateDeletes;
    if (kind == CompactionKind.SYSTEM) {
      // its ok if this is null for system compactions because its not used.
      this.fateId = fateId;
    } else {
      this.fateId = Objects.requireNonNull(fateId);
    }
  }

  public Set<StoredTabletFile> getJobFiles() {
    return jobFiles;
  }

  public ReferencedTabletFile getCompactTmpName() {
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

  public CompactorGroupId getCompactionGroupId() {
    return cgid;
  }

  public boolean getPropagateDeletes() {
    return propagateDeletes;
  }

  public FateId getFateId() {
    return fateId;
  }

  // This class is used to serialize and deserialize this class using GSon. Any changes to this
  // class must consider persisted data.
  private static class GSonData {
    List<String> inputs;
    String tmp;
    String compactor;
    String kind;
    String groupId;
    short priority;
    boolean propDels;
    String fateId;
  }

  public String toJson() {
    GSonData jData = new GSonData();

    jData.inputs = jobFiles.stream().map(StoredTabletFile::getMetadata).collect(toList());
    jData.tmp = compactTmpName.insert().getMetadata();
    jData.compactor = compactorId;
    jData.kind = kind.name();
    jData.groupId = cgid.toString();
    jData.priority = priority;
    jData.propDels = propagateDeletes;
    jData.fateId = fateId == null ? null : fateId.canonical();
    return GSON.get().toJson(jData);
  }

  public static CompactionMetadata fromJson(String json) {
    GSonData jData = GSON.get().fromJson(json, GSonData.class);

    return new CompactionMetadata(jData.inputs.stream().map(StoredTabletFile::new).collect(toSet()),
        StoredTabletFile.of(jData.tmp).getTabletFile(), jData.compactor,
        CompactionKind.valueOf(jData.kind), jData.priority, CompactorGroupId.of(jData.groupId),
        jData.propDels, jData.fateId == null ? null : FateId.from(jData.fateId));
  }

  @Override
  public String toString() {
    return toJson();
  }
}

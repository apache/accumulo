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
package org.apache.accumulo.manager.compaction.queue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;

/**
 * This class takes a compaction job and the tablet metadata from which is was generated and
 * computes all information that is needed to actually run the compaction. A goal of this class is
 * to avoid referencing anything that is not needed. For example only a subset of the information
 * from TabletMetadata is needed, therefore a reference to the TabletMetadata object should not be
 * kept. This object will placed on a queue for compaction and that is why it should avoid using
 * more memory than needed. Before this class was created queued compaction jobs would reference a
 * tablet metadata object which included all of the tablets files. When tablets had lots of file and
 * lots compactions against them this could use lots of memory and make the manager unstable at time
 * when things were already bad.
 */
public class ResolvedCompactionJob implements CompactionJob {

  // The fate id of the table compaction that selected files
  private final FateId selectedFateId;
  private final Map<StoredTabletFile,DataFileValue> jobFiles;
  private final CompactionKind kind;
  private final boolean compactingAll;
  private final KeyExtent extent;
  private final short priority;
  private final CompactorGroupId group;
  private final String tabletDir;
  private final boolean overlapsSelectedFiles;

  private static long weigh(Range range) {
    long estDataSize = 0;
    if (range != null) {
      var staryKey = range.getStartKey();
      estDataSize += staryKey == null ? 0 : staryKey.getSize();
      var endKey = range.getEndKey();
      estDataSize += endKey == null ? 0 : endKey.getSize();
    }
    return estDataSize;
  }

  public static final SizeTrackingTreeMap.Weigher<CompactionJob> WEIGHER = job -> {
    if (job instanceof ResolvedCompactionJob) {
      var rcj = (ResolvedCompactionJob) job;
      long estDataSize = 0;
      if (rcj.selectedFateId != null) {
        estDataSize += rcj.selectedFateId.canonical().length();
      }
      for (var file : rcj.jobFiles.keySet()) {
        estDataSize += file.getMetadataPath().length();
        estDataSize += 24; // There are three longs in DataFileValue
        estDataSize += weigh(file.getRange());
      }

      estDataSize += rcj.group.canonical().length();
      estDataSize += rcj.extent.tableId().canonical().length();
      estDataSize += rcj.extent.prevEndRow() == null ? 0 : rcj.extent.prevEndRow().getLength();
      estDataSize += rcj.extent.endRow() == null ? 0 : rcj.extent.endRow().getLength();
      estDataSize += rcj.tabletDir.length();
      // Guess at how many bytes the overhead at things like pointers and primitives are taking in
      // this object.
      estDataSize += 64;
      return estDataSize;
    } else {
      // Do not know the concrete type so weigh based on the interface methods of CompaactionJob
      long estDataSize = 0;
      for (var compactableFile : job.getFiles()) {
        estDataSize += compactableFile.getUri().toString().length();
        estDataSize += 16; // There are two longs on compactableFile, this accounts for those
        estDataSize += weigh(compactableFile.getRange());
      }

      estDataSize += job.getGroup().canonical().length();

      return estDataSize;
    }
  };

  public ResolvedCompactionJob(CompactionJob job, TabletMetadata tabletMetadata) {
    this.jobFiles = new HashMap<>();
    for (CompactableFile cfile : job.getFiles()) {
      var stFile = CompactableFileImpl.toStoredTabletFile(cfile);
      var dfv = tabletMetadata.getFilesMap().get(stFile);
      jobFiles.put(stFile, dfv);
    }

    this.kind = job.getKind();

    if (job.getKind() == CompactionKind.USER) {
      this.selectedFateId = tabletMetadata.getSelectedFiles().getFateId();
      this.compactingAll = tabletMetadata.getSelectedFiles().initiallySelectedAll()
          && tabletMetadata.getSelectedFiles().getFiles().equals(jobFiles.keySet());
      this.overlapsSelectedFiles = false;
    } else {
      this.selectedFateId = null;
      this.compactingAll = tabletMetadata.getFiles().equals(jobFiles.keySet());
      var selected = tabletMetadata.getSelectedFiles();
      if (selected == null) {
        this.overlapsSelectedFiles = false;
      } else {
        this.overlapsSelectedFiles =
            !Collections.disjoint(this.jobFiles.keySet(), selected.getFiles());
      }
    }

    this.priority = job.getPriority();
    this.group = job.getGroup();

    this.extent = tabletMetadata.getExtent();
    this.tabletDir = tabletMetadata.getDirName();
  }

  public FateId getSelectedFateId() {
    return selectedFateId;
  }

  public Set<StoredTabletFile> getJobFiles() {
    return jobFiles.keySet();
  }

  public Map<StoredTabletFile,DataFileValue> getJobFilesMap() {
    return jobFiles;
  }

  public Set<CompactableFile> getCompactionFiles() {
    return jobFiles.entrySet().stream().map(e -> new CompactableFileImpl(e.getKey(), e.getValue()))
        .collect(Collectors.toSet());
  }

  @Override
  public CompactionKind getKind() {
    return kind;
  }

  public boolean isCompactingAll() {
    return compactingAll;
  }

  public KeyExtent getExtent() {
    return extent;
  }

  public String getTabletDir() {
    return tabletDir;
  }

  @Override
  public short getPriority() {
    return priority;
  }

  @Override
  public CompactorGroupId getGroup() {
    return group;
  }

  @Override
  public Set<CompactableFile> getFiles() {
    return getCompactionFiles();
  }

  public boolean isOverlapsSelectedFiles() {
    return overlapsSelectedFiles;
  }

  @Override
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }
}

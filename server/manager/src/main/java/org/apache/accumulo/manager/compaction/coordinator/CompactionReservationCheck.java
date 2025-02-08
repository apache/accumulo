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
package org.apache.accumulo.manager.compaction.coordinator;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadataCheck;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Checks if a compaction job can be reserved
 */
public class CompactionReservationCheck implements TabletMetadataCheck {

  private static final Logger log = LoggerFactory.getLogger(CompactionReservationCheck.class);

  private CompactionKind kind;
  private List<String> jobFilesStr;
  private Long steadyTimeNanos;
  private Long selectedExpirationDurationMillis;
  private String selectedFateIdStr;
  private boolean checkIfCanDeleteSelectedFiles;

  public CompactionReservationCheck() {}

  public CompactionReservationCheck(CompactionKind jobKind, Set<StoredTabletFile> jobFiles,
      FateId selectedFateId, boolean checkIfCanDeleteSelectedFiles, SteadyTime steadyTime,
      long selectedExpirationDurationMillis) {
    this.kind = jobKind;
    // since this class will be serialized as json, make the types simpler to avoid many levels of
    // nesting
    this.jobFilesStr =
        jobFiles.stream().map(StoredTabletFile::getMetadata).collect(Collectors.toList());
    this.steadyTimeNanos = steadyTime.getNanos();
    this.selectedExpirationDurationMillis = selectedExpirationDurationMillis;
    this.selectedFateIdStr = selectedFateId == null ? null : selectedFateId.canonical();
    this.checkIfCanDeleteSelectedFiles = checkIfCanDeleteSelectedFiles;
  }

  @Override
  public boolean canUpdate(TabletMetadata tablet) {
    Objects.requireNonNull(tablet);
    Preconditions.checkState(kind != null && jobFilesStr != null && steadyTimeNanos != null
        && selectedExpirationDurationMillis != null);
    // expect selectedFateIdStr to be set if this is user compaction
    Preconditions.checkState((kind == CompactionKind.USER && selectedFateIdStr != null)
        || (kind == CompactionKind.SYSTEM && selectedFateIdStr == null));
    if (checkIfCanDeleteSelectedFiles) {
      Preconditions.checkState(kind == CompactionKind.SYSTEM);
    }

    var jobFiles = jobFilesStr.stream().map(StoredTabletFile::of).collect(Collectors.toSet());
    var steadyTime = SteadyTime.from(steadyTimeNanos, TimeUnit.NANOSECONDS);

    if (tablet.getOperationId() != null) {
      return false;
    }

    if (!tablet.getFiles().containsAll(jobFiles)) {
      return false;
    }

    var currentlyCompactingFiles = tablet.getExternalCompactions().values().stream()
        .flatMap(ecm -> ecm.getJobFiles().stream()).collect(Collectors.toSet());

    if (!Collections.disjoint(jobFiles, currentlyCompactingFiles)) {
      return false;
    }

    switch (kind) {
      case SYSTEM: {
        var userRequestedCompactions = tablet.getUserCompactionsRequested().size();
        if (userRequestedCompactions > 0) {
          log.debug(
              "Unable to reserve {} for system compaction, tablet has {} pending requested user compactions",
              tablet.getExtent(), userRequestedCompactions);
          return false;
        }

        var selected = tablet.getSelectedFiles();
        if (selected != null) {
          if (checkIfCanDeleteSelectedFiles) {
            // The mutation is deleting the selected files column. This can only proceed if no jobs
            // have run against the selected files, the selected files are expired, and the files
            // being compacted overlaps with the selected files.
            if (Collections.disjoint(jobFiles, selected.getFiles())) {
              // This job does not overlap with the selected files, so something probably changed
              // since the job was generated. Do not want to risk deleted selected files that
              // changed after the job was generated.
              return false;
            }

            if (selected.getCompletedJobs() > 0) {
              return false;
            }

            if (steadyTime.minus(tablet.getSelectedFiles().getSelectedTime()).toMillis()
                < selectedExpirationDurationMillis) {
              return false;
            }
          } else {
            // To start a system compaction that overlaps with files selected for user compaction
            // the mutation must delete the selected set of files. The mutation is not deleting the
            // selected files column, so it can not overlap w/ them.
            if (!Collections.disjoint(jobFiles, selected.getFiles())) {
              return false;
            }
          }
        }
        break;
      }
      case USER: {
        var selectedFateId = FateId.from(selectedFateIdStr);
        if (tablet.getSelectedFiles() == null
            || !tablet.getSelectedFiles().getFateId().equals(selectedFateId)
            || !tablet.getSelectedFiles().getFiles().containsAll(jobFiles)) {
          return false;
        }
        break;
      }
      default:
        throw new UnsupportedOperationException("Not currently handling " + kind);
    }

    return true;
  }

  @Override
  public Set<ColumnType> columnsToRead() {
    return Set.of(PREV_ROW, OPID, SELECTED, FILES, ECOMP, USER_COMPACTION_REQUESTED);
  }
}

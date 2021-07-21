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
package org.apache.accumulo.tserver.tablet;

import static org.apache.accumulo.tserver.tablet.CompactableImpl.asFileNames;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.tserver.tablet.CompactableImpl.FileSelectionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * This class tracks status of a tablets files for compactions for {@link CompactableImpl} owning
 * the following functionality.
 *
 * <UL>
 * <LI>Tracks which files are reserved for compactions
 * <LI>Determines which files are available for compactions
 * <LI>Tracks which files are chopped and which need to be chopped
 * <LI>Tracks which files are selected for user and selector compactions
 * <LI>Coordinates the file selection process
 * </UL>
 *
 * <p>
 * The class is structured in such a way that the above functionality can be unit tested.
 *
 */
public class TabletCompactionFileManager {

  private static final Logger log = LoggerFactory.getLogger(TabletCompactionFileManager.class);

  FileSelectionStatus selectStatus = FileSelectionStatus.NOT_ACTIVE;
  private CompactionKind selectKind = null;

  // Tracks if when a set of files was selected, if at that time the set was all of the tablets
  // files. Because a set of selected files can be compacted over one or more compactions, its
  // important to track this in order to know if the last compaction is a full compaction and should
  // not propagate deletes.
  private boolean initiallySelectedAll = false;
  private Set<StoredTabletFile> selectedFiles = new HashSet<>();

  private Set<StoredTabletFile> allCompactingFiles = new HashSet<>();

  // track files produced by compactions of this tablet, those are considered chopped
  private Set<StoredTabletFile> choppedFiles = new HashSet<>();
  private FileSelectionStatus chopStatus = FileSelectionStatus.NOT_ACTIVE;
  private Set<StoredTabletFile> allFilesWhenChopStarted = new HashSet<>();

  private final Set<CompactionJob> runningJobs;

  private final KeyExtent extent;

  public TabletCompactionFileManager(Set<CompactionJob> runningJobs, KeyExtent extent,
      Collection<StoredTabletFile> extCompactingFiles, Optional<SelectedInfo> extSelInfo) {
    this.runningJobs = runningJobs;
    this.extent = extent;
    allCompactingFiles.addAll(extCompactingFiles);
    if (extSelInfo.isPresent()) {
      this.selectedFiles.addAll(extSelInfo.get().selectedFiles);
      this.selectKind = extSelInfo.get().selectKind;
      this.initiallySelectedAll = extSelInfo.get().initiallySelectedAll;
      this.selectStatus = FileSelectionStatus.SELECTED;

      log.debug("Selected compaction status initialized from external compactions {} {} {} {}",
          getExtent(), selectStatus, initiallySelectedAll, asFileNames(selectedFiles));
    }
  }

  private boolean noneRunning(CompactionKind kind) {
    return runningJobs.stream().noneMatch(job -> job.getKind() == kind);
  }

  Set<StoredTabletFile> getCompactingFiles() {
    return Collections.unmodifiableSet(allCompactingFiles);
  }

  FileSelectionStatus getSelectionStatus() {
    return selectStatus;
  }

  CompactionKind getSelectionKind() {
    return selectKind;
  }

  static class SelectedInfo {
    final boolean initiallySelectedAll;
    final Set<StoredTabletFile> selectedFiles;
    final CompactionKind selectKind;

    public SelectedInfo(boolean initiallySelectedAll, Set<StoredTabletFile> selectedFiles,
        CompactionKind selectKind) {
      this.initiallySelectedAll = initiallySelectedAll;
      this.selectedFiles = Set.copyOf(selectedFiles);
      this.selectKind = selectKind;
    }
  }

  SelectedInfo getSelectedInfo() {
    Preconditions.checkState(selectStatus == FileSelectionStatus.SELECTED);
    return new SelectedInfo(initiallySelectedAll, selectedFiles, selectKind);
  }

  boolean initiateSelection(CompactionKind kind) {

    if (selectStatus == FileSelectionStatus.NOT_ACTIVE || (kind == CompactionKind.USER
        && selectKind == CompactionKind.SELECTOR && noneRunning(CompactionKind.SELECTOR))) {
      selectStatus = FileSelectionStatus.NEW;
      selectKind = kind;
      selectedFiles.clear();
      initiallySelectedAll = false;
      return true;
    }

    return false;

  }

  boolean beginSelection() {
    if (selectStatus == FileSelectionStatus.NEW && allCompactingFiles.isEmpty()) {
      selectStatus = FileSelectionStatus.SELECTING;
      log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
      return true;
    }

    return false;
  }

  void finishSelection(Set<StoredTabletFile> selected, boolean allSelected) {
    Preconditions.checkArgument(!selected.isEmpty());
    Preconditions.checkState(selectStatus == FileSelectionStatus.SELECTING);
    selectStatus = FileSelectionStatus.SELECTED;
    selectedFiles.clear();
    selectedFiles.addAll(selected);
    initiallySelectedAll = allSelected;
    log.trace("Selected compaction status changed {} {} {} {}", getExtent(), selectStatus,
        initiallySelectedAll, asFileNames(selectedFiles));
    TabletLogger.selected(getExtent(), selectKind, selectedFiles);
  }

  void cancelSelection() {
    Preconditions.checkState(selectStatus == FileSelectionStatus.SELECTING);
    selectStatus = FileSelectionStatus.NOT_ACTIVE;
    log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
  }

  boolean isSelected(CompactionKind kind) {
    return selectStatus == FileSelectionStatus.SELECTED && kind == selectKind;
  }

  FileSelectionStatus getChopStatus() {
    return chopStatus;
  }

  ChopSelector initiateChop(Set<StoredTabletFile> allFiles) {
    Preconditions.checkState(chopStatus == FileSelectionStatus.NOT_ACTIVE);
    Set<StoredTabletFile> filesToExamine = new HashSet<>(allFiles);
    chopStatus = FileSelectionStatus.SELECTING;
    filesToExamine.removeAll(choppedFiles);
    filesToExamine.removeAll(allCompactingFiles);
    return new ChopSelector(allFiles, filesToExamine);
  }

  class ChopSelector {
    private Set<StoredTabletFile> allFiles;
    private Set<StoredTabletFile> filesToExamine;

    private ChopSelector(Set<StoredTabletFile> allFiles, Set<StoredTabletFile> filesToExamine) {
      this.allFiles = allFiles;
      this.filesToExamine = filesToExamine;
    }

    void selectChopFiles(Set<StoredTabletFile> unchoppedFiles) {
      Preconditions.checkState(chopStatus == FileSelectionStatus.SELECTING);
      choppedFiles.addAll(Sets.difference(filesToExamine, unchoppedFiles));
      chopStatus = FileSelectionStatus.SELECTED;
      allFilesWhenChopStarted.clear();
      allFilesWhenChopStarted.addAll(allFiles);

      var filesToChop = getFilesToChop(allFiles);
      if (!filesToChop.isEmpty()) {
        TabletLogger.selected(getExtent(), CompactionKind.CHOP, filesToChop);
      }
    }

    Set<StoredTabletFile> getFilesToExamine() {
      return Collections.unmodifiableSet(filesToExamine);
    }
  }

  boolean finishChop(Set<StoredTabletFile> allFiles) {

    boolean completed = false;

    if (chopStatus == FileSelectionStatus.SELECTED) {
      if (getFilesToChop(allFiles).isEmpty()) {
        chopStatus = FileSelectionStatus.NOT_ACTIVE;
        completed = true;
      }
    }

    choppedFiles.retainAll(allFiles);

    return completed;
  }

  void addChoppedFiles(Collection<StoredTabletFile> files) {
    choppedFiles.addAll(files);
  }

  void userCompactionCanceled() {
    if (isSelected(CompactionKind.USER)) {
      if (noneRunning(CompactionKind.USER)) {
        selectStatus = FileSelectionStatus.NOT_ACTIVE;
        log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
      } else {
        selectStatus = FileSelectionStatus.CANCELED;
        log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
      }
    }
  }

  private Set<StoredTabletFile> getFilesToChop(Set<StoredTabletFile> allFiles) {
    Preconditions.checkState(chopStatus == FileSelectionStatus.SELECTED);
    var copy = new HashSet<>(allFilesWhenChopStarted);
    copy.retainAll(allFiles);
    copy.removeAll(choppedFiles);
    return copy;
  }

  /**
   * @return The set of tablet files that are candidates for compaction
   */
  Set<StoredTabletFile> getCandidates(Set<StoredTabletFile> currFiles, CompactionKind kind,
      boolean isCompactionStratConfigured) {

    if (!currFiles.containsAll(allCompactingFiles)) {
      log.trace("Ignoring because compacting not a subset {}", getExtent());

      // A compaction finished, so things are out of date. This can happen because CompactableImpl
      // and Tablet have separate locks, its ok.
      return Set.of();
    }

    switch (kind) {
      case SYSTEM: {
        if (isCompactionStratConfigured)
          return Set.of();

        switch (selectStatus) {
          case NOT_ACTIVE:
          case CANCELED: {
            Set<StoredTabletFile> candidates = new HashSet<>(currFiles);
            candidates.removeAll(allCompactingFiles);
            return Collections.unmodifiableSet(candidates);
          }
          case NEW:
          case SELECTING:
            return Set.of();
          case SELECTED: {
            Set<StoredTabletFile> candidates = new HashSet<>(currFiles);
            candidates.removeAll(allCompactingFiles);
            candidates.removeAll(selectedFiles);
            return Collections.unmodifiableSet(candidates);
          }
          default:
            throw new AssertionError();
        }
      }
      case SELECTOR:
        // intentional fall through
      case USER:
        switch (selectStatus) {
          case NOT_ACTIVE:
          case NEW:
          case SELECTING:
          case CANCELED:
            return Set.of();
          case SELECTED: {
            if (selectKind == kind) {
              Set<StoredTabletFile> candidates = new HashSet<>(selectedFiles);
              candidates.removeAll(allCompactingFiles);
              candidates = Collections.unmodifiableSet(candidates);
              Preconditions.checkState(currFiles.containsAll(candidates),
                  "selected files not in all files %s %s", candidates, currFiles);
              return candidates;
            } else {
              return Set.of();
            }
          }
          default:
            throw new AssertionError();
        }
      case CHOP: {
        switch (chopStatus) {
          case NOT_ACTIVE:
          case NEW:
          case SELECTING:
            return Set.of();
          case SELECTED: {
            if (selectStatus == FileSelectionStatus.NEW
                || selectStatus == FileSelectionStatus.SELECTING)
              return Set.of();

            var filesToChop = getFilesToChop(currFiles);
            filesToChop.removeAll(allCompactingFiles);
            if (selectStatus == FileSelectionStatus.SELECTED)
              filesToChop.removeAll(selectedFiles);
            return Collections.unmodifiableSet(filesToChop);
          }
          case CANCELED: // intentional fall through, not expected status for chop
          default:
            throw new AssertionError();
        }
      }
      default:
        throw new AssertionError();
    }
  }

  /**
   * Attempts to reserve a set of files for compaction.
   *
   * @return true if the files were reserved and false otherwise
   */
  boolean reserveFiles(CompactionJob job, Set<StoredTabletFile> jobFiles) {

    Preconditions.checkArgument(!jobFiles.isEmpty());

    switch (selectStatus) {
      case NEW:
      case SELECTING:
        log.trace("Ignoring compaction because files are being selected for user compaction {} {}",
            getExtent(), job);
        return false;
      case SELECTED: {
        if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
          if (selectKind == job.getKind()) {
            if (!selectedFiles.containsAll(jobFiles)) {
              // TODO diff log level?
              log.error("Ignoring {} compaction that does not contain selected files {} {} {}",
                  job.getKind(), getExtent(), asFileNames(selectedFiles), asFileNames(jobFiles));
              return false;
            }
          } else {
            log.trace("Ingoring {} compaction because not selected kind {}", job.getKind(),
                getExtent());
            return false;
          }
        } else if (!Collections.disjoint(selectedFiles, jobFiles)) {
          log.trace("Ingoring compaction that overlaps with selected files {} {} {}", getExtent(),
              job.getKind(), asFileNames(Sets.intersection(selectedFiles, jobFiles)));
          return false;
        }
        break;
      }
      case CANCELED:
      case NOT_ACTIVE: {
        if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
          log.trace("Ignoring {} compaction because selectStatus is {} for {}", job.getKind(),
              selectStatus, getExtent());
          return false;
        }
        break;
      }
      default:
        throw new AssertionError();
    }

    if (Collections.disjoint(allCompactingFiles, jobFiles)) {
      allCompactingFiles.addAll(jobFiles);
      return true;
    } else {
      return false;
    }
  }

  private KeyExtent getExtent() {
    return extent;
  }

  boolean allSelected(Set<StoredTabletFile> jobFiles, CompactionKind kind) {
    Preconditions.checkState(selectStatus == FileSelectionStatus.SELECTED);
    Preconditions.checkArgument(kind == selectKind);

    return initiallySelectedAll && jobFiles.containsAll(selectedFiles);
  }

  /**
   * Releases a set of files that were previously reserved for compaction.
   *
   * @param newFile
   *          The file produced by a compaction. If the compaction failed, this can be null.
   */
  void completed(CompactionJob job, Set<StoredTabletFile> jobFiles, StoredTabletFile newFile) {
    Preconditions.checkArgument(!jobFiles.isEmpty());
    Preconditions.checkState(allCompactingFiles.removeAll(jobFiles));
    if (newFile != null) {
      choppedFiles.add(newFile);
    }

    if ((job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR)
        && newFile != null) {
      selectedCompactionCompleted(job, jobFiles, newFile);
    }
  }

  private void selectedCompactionCompleted(CompactionJob job, Set<StoredTabletFile> jobFiles,
      StoredTabletFile newFile) {
    Preconditions.checkArgument(
        job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR);
    Preconditions.checkState(selectedFiles.containsAll(jobFiles));
    Preconditions.checkState((selectStatus == FileSelectionStatus.SELECTED
        || selectStatus == FileSelectionStatus.CANCELED) && selectKind == job.getKind());

    selectedFiles.removeAll(jobFiles);

    if (selectedFiles.isEmpty()
        || (selectStatus == FileSelectionStatus.CANCELED && noneRunning(selectKind))) {
      selectStatus = FileSelectionStatus.NOT_ACTIVE;
      log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
    } else if (selectStatus == FileSelectionStatus.SELECTED) {
      selectedFiles.add(newFile);
      log.trace("Compacted subset of selected files {} {} -> {}", getExtent(),
          asFileNames(jobFiles), newFile.getFileName());
    } else {
      log.debug("Canceled selected compaction completed {} but others still running ", getExtent());
    }

    TabletLogger.selected(getExtent(), selectKind, selectedFiles);
  }

}

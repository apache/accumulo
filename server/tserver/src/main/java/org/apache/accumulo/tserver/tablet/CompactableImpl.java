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
package org.apache.accumulo.tserver.tablet;

import static org.apache.accumulo.tserver.TabletStatsKeeper.Operation.MAJOR;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher.DispatchParameters;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionCanceledException;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.compactions.CompactionManager;
import org.apache.accumulo.tserver.compactions.ExternalCompactionJob;
import org.apache.accumulo.tserver.managermessage.TabletStatusMessage;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

/**
 * This class exists between compaction services and tablets and tracks state related to compactions
 * for a tablet. This class was written to mainly contain code related to tracking files, state, and
 * synchronization. All other code was placed in {@link CompactableUtils} in order to make this
 * class easier to analyze.
 */
public class CompactableImpl implements Compactable {

  private static class ExternalCompactionInfo {
    ExternalCompactionMetadata meta;
    CompactionJob job;
  }

  private static final Logger log = LoggerFactory.getLogger(CompactableImpl.class);

  private final Tablet tablet;

  private final FileManager fileMgr;

  private final Set<CompactionJob> runningJobs = new HashSet<>();
  private volatile boolean compactionRunning = false;

  private final Supplier<Set<CompactionServiceId>> servicesInUse;

  private final Set<CompactionServiceId> servicesUsed = new ConcurrentSkipListSet<>();

  enum ChopSelectionStatus {
    SELECTING, SELECTED, NOT_ACTIVE, MARKING
  }

  // status of special compactions
  enum FileSelectionStatus {
    NEW, SELECTING, SELECTED, RESERVED, NOT_ACTIVE, CANCELED
  }

  private CompactionHelper chelper = null;
  private Long compactionId;
  private CompactionConfig compactionConfig;

  private final CompactionManager manager;

  AtomicLong lastSeenCompactionCancelId = new AtomicLong(Long.MIN_VALUE);

  private volatile boolean closed = false;

  private final Map<ExternalCompactionId,ExternalCompactionInfo> externalCompactions =
      new ConcurrentHashMap<>();

  private final Set<ExternalCompactionId> externalCompactionsCommitting = new HashSet<>();

  // This interface exists for two purposes. First it allows abstraction of new and old
  // implementations for user pluggable file selection code. Second it facilitates placing code
  // outside of this class.
  public interface CompactionHelper {
    Set<StoredTabletFile> selectFiles(SortedMap<StoredTabletFile,DataFileValue> allFiles);

    Set<StoredTabletFile> getFilesToDrop();

    Map<String,String> getConfigOverrides(Set<CompactableFile> files);

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

  /**
   * This class tracks status of a tablets files for compactions for {@link CompactableImpl} owning
   * the following functionality.
   *
   * <ul>
   * <li>Tracks which files are reserved for compactions
   * <li>Determines which files are available for compactions
   * <li>Tracks which files are chopped and which need to be chopped
   * <li>Tracks which files are selected for user and selector compactions
   * <li>Coordinates the file selection process
   * </ul>
   *
   * <p>
   * The class is structured in such a way that the above functionality can be unit tested.
   *
   * <p>
   * This class does no synchronization of its own and relies on CompactableImpl to do all needed
   * synchronization. CompactableImpl must make changes to files and other state like running jobs
   * in a mutually exclusive manner, so synchronization at this level is unnecessary.
   *
   */
  static abstract class FileManager {

    FileSelectionStatus selectStatus = FileSelectionStatus.NOT_ACTIVE;
    private long selectedTimeNanos;
    private Duration selectedExpirationDuration;
    private CompactionKind selectKind = null;

    // Tracks if when a set of files was selected, if at that time the set was all of the tablets
    // files. Because a set of selected files can be compacted over one or more compactions, it's
    // important to track this in order to know if the last compaction is a full compaction and
    // should not propagate deletes.
    private boolean initiallySelectedAll = false;
    private final Set<StoredTabletFile> selectedFiles = new HashSet<>();

    protected Set<StoredTabletFile> allCompactingFiles = new HashSet<>();

    // track files produced by compactions of this tablet, those are considered chopped
    private final Set<StoredTabletFile> choppedFiles = new HashSet<>();
    private ChopSelectionStatus chopStatus = ChopSelectionStatus.NOT_ACTIVE;
    private final Set<StoredTabletFile> allFilesWhenChopStarted = new HashSet<>();

    private final KeyExtent extent;
    private final Deriver<Duration> selectionExpirationDeriver;

    public FileManager(KeyExtent extent, Collection<StoredTabletFile> extCompactingFiles,
        Optional<SelectedInfo> extSelInfo, Deriver<Duration> selectionExpirationDeriver) {

      this.extent = extent;
      this.selectionExpirationDeriver = selectionExpirationDeriver;
      allCompactingFiles.addAll(extCompactingFiles);
      if (extSelInfo.isPresent()) {
        this.selectedFiles.addAll(extSelInfo.orElseThrow().selectedFiles);
        this.selectKind = extSelInfo.orElseThrow().selectKind;
        this.initiallySelectedAll = extSelInfo.orElseThrow().initiallySelectedAll;
        this.selectStatus = FileSelectionStatus.RESERVED;

        log.debug("Selected compaction status initialized from external compactions {} {} {} {}",
            getExtent(), selectStatus, initiallySelectedAll, asFileNames(selectedFiles));
      }
    }

    FileSelectionStatus getSelectionStatus() {
      return selectStatus;
    }

    CompactionKind getSelectionKind() {
      return selectKind;
    }

    @VisibleForTesting
    Set<StoredTabletFile> getSelectedFiles() {
      return Set.copyOf(selectedFiles);
    }

    SelectedInfo getReservedInfo() {
      Preconditions.checkState(selectStatus == FileSelectionStatus.RESERVED);
      return new SelectedInfo(initiallySelectedAll, selectedFiles, selectKind);
    }

    protected abstract boolean noneRunning(CompactionKind kind);

    protected abstract long getNanoTime();

    /**
     * @return the last id of the last successful user compaction
     */
    protected abstract long getLastCompactId();

    boolean initiateSelection(CompactionKind kind, Long compactionId) {

      Preconditions.checkArgument(
          kind == CompactionKind.SELECTOR && compactionId == null
              || kind == CompactionKind.USER && compactionId != null,
          "Unexpected kind and/or compaction id: %s %s", kind, compactionId);

      if (selectStatus == FileSelectionStatus.NOT_ACTIVE || (kind == CompactionKind.USER
          && selectKind == CompactionKind.SELECTOR && noneRunning(CompactionKind.SELECTOR)
          && selectStatus != FileSelectionStatus.SELECTING)) {

        // Check compaction id when a lock is held and no other user compactions have files
        // selected, at this point the results of any previous user compactions should be seen. If
        // user compaction is currently running, then will not get this far because of the checks a
        // few lines up.
        if (kind == CompactionKind.USER && getLastCompactId() >= compactionId) {
          // This user compaction has already completed, so no need to initiate selection of files
          // for user compaction.
          return false;
        }

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
      selectedTimeNanos = getNanoTime();
      // take a snapshot of this from config and use it for the entire selection for consistency
      selectedExpirationDuration = selectionExpirationDeriver.derive();
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
      return (selectStatus == FileSelectionStatus.SELECTED
          || selectStatus == FileSelectionStatus.RESERVED) && kind == selectKind;
    }

    ChopSelectionStatus getChopStatus() {
      return chopStatus;
    }

    ChopSelector initiateChop(Set<StoredTabletFile> allFiles) {
      Preconditions.checkState(chopStatus == ChopSelectionStatus.NOT_ACTIVE);
      Set<StoredTabletFile> filesToExamine = new HashSet<>(allFiles);
      chopStatus = ChopSelectionStatus.SELECTING;
      filesToExamine.removeAll(choppedFiles);
      filesToExamine.removeAll(allCompactingFiles);
      return new ChopSelector(allFiles, filesToExamine);
    }

    class ChopSelector {
      private final Set<StoredTabletFile> allFiles;
      private final Set<StoredTabletFile> filesToExamine;

      private ChopSelector(Set<StoredTabletFile> allFiles, Set<StoredTabletFile> filesToExamine) {
        this.allFiles = allFiles;
        this.filesToExamine = filesToExamine;
      }

      void selectChopFiles(Set<StoredTabletFile> unchoppedFiles) {
        Preconditions.checkState(chopStatus == ChopSelectionStatus.SELECTING);
        choppedFiles.addAll(Sets.difference(filesToExamine, unchoppedFiles));
        chopStatus = ChopSelectionStatus.SELECTED;
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

      if (chopStatus == ChopSelectionStatus.SELECTED) {
        if (getFilesToChop(allFiles).isEmpty()) {
          chopStatus = ChopSelectionStatus.MARKING;
          completed = true;
        }
      }

      choppedFiles.retainAll(allFiles);

      return completed;
    }

    void finishMarkingChop() {
      Preconditions.checkState(chopStatus == ChopSelectionStatus.MARKING);
      chopStatus = ChopSelectionStatus.NOT_ACTIVE;
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
      Preconditions.checkState(chopStatus == ChopSelectionStatus.SELECTED);
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
        // and Tablet have separate locks, it's ok.
        return Set.of();
      }

      switch (kind) {
        case SYSTEM: {
          if (isCompactionStratConfigured) {
            return Set.of();
          }

          return handleSystemCompaction(currFiles);
        }
        case SELECTOR:
          // intentional fall through
        case USER:
          return handleUserSelectorCompaction(currFiles, kind);
        case CHOP: {
          return handleChopCompaction(currFiles);
        }
        default:
          throw new AssertionError();
      }
    }

    private Set<StoredTabletFile> handleChopCompaction(Set<StoredTabletFile> currFiles) {
      switch (chopStatus) {
        case NOT_ACTIVE:
        case SELECTING:
        case MARKING:
          return Set.of();
        case SELECTED: {
          if (selectStatus == FileSelectionStatus.NEW
              || selectStatus == FileSelectionStatus.SELECTING) {
            return Set.of();
          }

          var filesToChop = getFilesToChop(currFiles);
          filesToChop.removeAll(allCompactingFiles);
          if (selectStatus == FileSelectionStatus.SELECTED
              || selectStatus == FileSelectionStatus.RESERVED) {
            filesToChop.removeAll(selectedFiles);
          }
          return Collections.unmodifiableSet(filesToChop);
        }
        default:
          throw new AssertionError();
      }
    }

    private Set<StoredTabletFile> handleUserSelectorCompaction(Set<StoredTabletFile> currFiles,
        CompactionKind kind) {
      switch (selectStatus) {
        case NOT_ACTIVE:
        case NEW:
        case SELECTING:
        case CANCELED:
          return Set.of();
        case SELECTED:
        case RESERVED: {
          if (selectKind == kind) {
            Set<StoredTabletFile> candidates = Sets.difference(selectedFiles, allCompactingFiles);
            // verify that candidates are still around and fail quietly if not
            if (!currFiles.containsAll(candidates)) {
              log.debug("Selected files not in all files {} {} {}",
                  Sets.difference(candidates, currFiles), candidates, currFiles);
              return Set.of();
            }
            // must create a copy because the sets passed to Sets.difference could change after this
            // method returns
            return Set.copyOf(candidates);
          } else {
            return Set.of();
          }
        }
        default:
          throw new AssertionError();
      }
    }

    private Set<StoredTabletFile> handleSystemCompaction(Set<StoredTabletFile> currFiles) {
      switch (selectStatus) {
        case NOT_ACTIVE:
        case CANCELED: {
          // must create a copy because the sets passed to Sets.difference could change after this
          // method returns
          return Set.copyOf(Sets.difference(currFiles, allCompactingFiles));
        }
        case NEW:
        case SELECTING:
          return Set.of();
        case SELECTED: {
          Set<StoredTabletFile> candidates = new HashSet<>(currFiles);
          candidates.removeAll(allCompactingFiles);
          if (getNanoTime() - selectedTimeNanos < selectedExpirationDuration.toNanos()) {
            candidates.removeAll(selectedFiles);
          }
          return Collections.unmodifiableSet(candidates);
        }
        case RESERVED: {
          Set<StoredTabletFile> candidates = new HashSet<>(currFiles);
          candidates.removeAll(allCompactingFiles);
          candidates.removeAll(selectedFiles);
          return Collections.unmodifiableSet(candidates);
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

      if (selectStatus == FileSelectionStatus.SELECTED
          && getNanoTime() - selectedTimeNanos > selectedExpirationDuration.toNanos()
          && job.getKind() != selectKind && !Collections.disjoint(selectedFiles, jobFiles)) {
        // If a selected compaction starts running, it should always changes the state to RESERVED.
        // So would never expect there to be any running when in the SELECTED state.
        Preconditions.checkState(noneRunning(selectKind));
        selectStatus = FileSelectionStatus.NOT_ACTIVE;
        log.trace("Selected compaction status changed {} {} because selection expired.",
            getExtent(), selectStatus);
      }

      switch (selectStatus) {
        case NEW:
        case SELECTING:
          log.trace(
              "Ignoring compaction because files are being selected for user compaction {} {}",
              getExtent(), job);
          return false;
        case SELECTED:
        case RESERVED: {
          if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
            if (selectKind == job.getKind()) {
              if (!selectedFiles.containsAll(jobFiles)) {
                log.trace("Ignoring {} compaction that does not contain selected files {} {} {}",
                    job.getKind(), getExtent(), asFileNames(selectedFiles), asFileNames(jobFiles));
                return false;
              }
            } else {
              log.trace("Ingoing {} compaction because not selected kind {}", job.getKind(),
                  getExtent());
              return false;
            }
          } else if (!Collections.disjoint(selectedFiles, jobFiles)) {
            log.trace("Ingoing compaction that overlaps with selected files {} {} {}", getExtent(),
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
        if (selectStatus == FileSelectionStatus.SELECTED && job.getKind() == selectKind) {
          selectStatus = FileSelectionStatus.RESERVED;
          log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
        }
        allCompactingFiles.addAll(jobFiles);
        return true;
      } else {
        return false;
      }
    }

    private KeyExtent getExtent() {
      return extent;
    }

    /**
     * Releases a set of files that were previously reserved for compaction.
     *
     * @param newFile The file produced by a compaction. If the compaction failed, this can be null.
     */
    void completed(CompactionJob job, Set<StoredTabletFile> jobFiles,
        Optional<StoredTabletFile> newFile, boolean successful) {
      Preconditions.checkArgument(!jobFiles.isEmpty());
      Preconditions.checkState(allCompactingFiles.removeAll(jobFiles));
      if (newFile.isPresent()) {
        choppedFiles.add(newFile.orElseThrow());
      }

      if (successful
          && (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR)) {
        selectedCompactionCompleted(job, jobFiles, newFile);
      }
    }

    private void selectedCompactionCompleted(CompactionJob job, Set<StoredTabletFile> jobFiles,
        Optional<StoredTabletFile> newFile) {
      Preconditions.checkArgument(
          job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR);
      Preconditions.checkState(selectedFiles.containsAll(jobFiles));
      Preconditions.checkState((selectStatus == FileSelectionStatus.RESERVED
          || selectStatus == FileSelectionStatus.CANCELED) && selectKind == job.getKind());

      selectedFiles.removeAll(jobFiles);

      if (selectedFiles.isEmpty()
          || (selectStatus == FileSelectionStatus.CANCELED && noneRunning(selectKind))) {
        selectStatus = FileSelectionStatus.NOT_ACTIVE;
        log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
      } else if (selectStatus == FileSelectionStatus.RESERVED) {
        if (newFile.isPresent()) {
          selectedFiles.add(newFile.orElseThrow());
        }
        log.trace("Compacted subset of selected files {} {} -> {}", getExtent(),
            asFileNames(jobFiles), newFile.orElse(null));
      } else {
        log.debug("Canceled selected compaction completed {} but others still running ",
            getExtent());
      }

      TabletLogger.selected(getExtent(), selectKind, selectedFiles);
    }

  }

  public CompactableImpl(Tablet tablet, CompactionManager manager,
      Map<ExternalCompactionId,ExternalCompactionMetadata> extCompactions) {
    this.tablet = tablet;
    this.manager = manager;

    var dataFileSizes = tablet.getDatafileManager().getDatafileSizes();

    Map<ExternalCompactionId,String> extCompactionsToRemove = new HashMap<>();

    // Memoize the supplier so it only calls tablet.getCompactionID() once, because the impl goes to
    // zookeeper. It's a supplier because it may not be needed.
    Supplier<Optional<Pair<Long,CompactionConfig>>> tabletCompactionId = Suppliers.memoize(() -> {
      try {
        return Optional.of(tablet.getCompactionID());
      } catch (NoNodeException nne) {
        return Optional.empty();
      }
    });

    var extSelInfo =
        processExternalMetadata(extCompactions, () -> tabletCompactionId.get().map(Pair::getFirst),
            dataFileSizes.keySet(), extCompactionsToRemove);

    if (extSelInfo.isPresent()) {
      if (extSelInfo.orElseThrow().selectKind == CompactionKind.USER) {
        this.chelper = CompactableUtils.getHelper(extSelInfo.orElseThrow().selectKind, tablet,
            tabletCompactionId.get().orElseThrow().getFirst(),
            tabletCompactionId.get().orElseThrow().getSecond());
        this.compactionConfig = tabletCompactionId.get().orElseThrow().getSecond();
        this.compactionId = tabletCompactionId.get().orElseThrow().getFirst();
      } else if (extSelInfo.orElseThrow().selectKind == CompactionKind.SELECTOR) {
        this.chelper =
            CompactableUtils.getHelper(extSelInfo.orElseThrow().selectKind, tablet, null, null);
      }
    }

    extCompactionsToRemove.forEach((ecid, reason) -> {
      log.warn("Removing external compaction {} for {} because {} meta: {}", ecid,
          tablet.getExtent(), reason, extCompactions.get(ecid).toJson());
    });

    if (!extCompactionsToRemove.isEmpty()) {
      var tabletMutator = tablet.getContext().getAmple().mutateTablet(tablet.getExtent());
      extCompactionsToRemove.keySet().forEach(tabletMutator::deleteExternalCompaction);
      tabletMutator.mutate();
    }

    ArrayList<StoredTabletFile> extCompactingFiles = new ArrayList<>();

    extCompactions.forEach((ecid, ecMeta) -> {
      if (!extCompactionsToRemove.containsKey(ecid)) {
        extCompactingFiles.addAll(ecMeta.getJobFiles());
        Collection<CompactableFile> files =
            ecMeta.getJobFiles().stream().map(f -> new CompactableFileImpl(f, dataFileSizes.get(f)))
                .collect(Collectors.toList());
        CompactionJob job = new CompactionJobImpl(ecMeta.getPriority(),
            ecMeta.getCompactionExecutorId(), files, ecMeta.getKind(), Optional.empty());
        addJob(job);

        ExternalCompactionInfo ecInfo = new ExternalCompactionInfo();
        ecInfo.job = job;
        ecInfo.meta = ecMeta;
        externalCompactions.put(ecid, ecInfo);

        log.debug("Loaded tablet {} has existing external compaction {} {}", getExtent(), ecid,
            ecMeta);
        manager.registerExternalCompaction(ecid, getExtent(), ecMeta.getCompactionExecutorId());
      }
    });

    if (extCompactions.values().stream().map(ecMeta -> ecMeta.getKind())
        .anyMatch(kind -> kind == CompactionKind.CHOP)) {
      initiateChop();
    }

    this.servicesInUse = Suppliers.memoizeWithExpiration(() -> {
      HashSet<CompactionServiceId> servicesIds = new HashSet<>();
      for (CompactionKind kind : CompactionKind.values()) {
        servicesIds.add(getConfiguredService(kind));
      }
      return Set.copyOf(servicesIds);
    }, 2, TimeUnit.SECONDS);

    Deriver<Duration> selectionExpirationNanosDeriver =
        tablet.getTableConfiguration().newDeriver(conf -> Duration
            .ofMillis(conf.getTimeInMillis(Property.TABLE_COMPACTION_SELECTION_EXPIRATION)));
    this.fileMgr = new FileManager(tablet.getExtent(), extCompactingFiles, extSelInfo,
        selectionExpirationNanosDeriver) {
      @Override
      protected boolean noneRunning(CompactionKind kind) {
        return CompactableImpl.this.noneRunning(kind);
      }

      @Override
      protected long getNanoTime() {
        return System.nanoTime();
      }

      @Override
      protected long getLastCompactId() {
        return tablet.getLastCompactId();
      }
    };
  }

  private synchronized boolean addJob(CompactionJob job) {
    if (runningJobs.add(job)) {
      compactionRunning = true;
      return true;
    }

    return false;
  }

  private synchronized boolean removeJob(CompactionJob job) {
    var removed = runningJobs.remove(job);
    compactionRunning = !runningJobs.isEmpty();
    return removed;
  }

  private synchronized boolean noneRunning(CompactionKind kind) {
    return runningJobs.stream().noneMatch(job -> job.getKind() == kind);
  }

  void initiateChop() {

    Set<StoredTabletFile> allFiles = tablet.getDatafiles().keySet();
    FileManager.ChopSelector chopSelector;

    synchronized (this) {
      if (fileMgr.getChopStatus() == ChopSelectionStatus.NOT_ACTIVE) {
        chopSelector = fileMgr.initiateChop(allFiles);
      } else {
        return;
      }
    }

    Set<StoredTabletFile> unchoppedFiles = selectChopFiles(chopSelector.getFilesToExamine());

    synchronized (this) {
      chopSelector.selectChopFiles(unchoppedFiles);
    }

    checkifChopComplete(tablet.getDatafiles().keySet());
  }

  private void checkifChopComplete(Set<StoredTabletFile> allFiles) {

    boolean completed;

    synchronized (this) {
      if (closed) {
        // if closed, do not attempt to transition to the MARKING state
        return;
      }
      // when this returns true it means we transitioned to the MARKING state
      completed = fileMgr.finishChop(allFiles);
    }

    if (completed) {
      try {
        markChopped();
      } finally {
        synchronized (this) {
          // transition the state from MARKING to NOT_ACTIVE
          fileMgr.finishMarkingChop();
          this.notifyAll();
        }
      }

      TabletLogger.selected(getExtent(), CompactionKind.CHOP, Set.of());
    }
  }

  private void markChopped() {
    MetadataTableUtil.chopped(tablet.getTabletServer().getContext(), getExtent(),
        tablet.getTabletServer().getLock());
    tablet.getTabletServer()
        .enqueueManagerMessage(new TabletStatusMessage(TabletLoadState.CHOPPED, getExtent()));
  }

  private Set<StoredTabletFile> selectChopFiles(Set<StoredTabletFile> chopCandidates) {
    try {
      var firstAndLastKeys = CompactableUtils.getFirstAndLastKeys(tablet, chopCandidates);
      return CompactableUtils.findChopFiles(getExtent(), firstAndLastKeys, chopCandidates);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Tablet can use this to signal files were added.
   */
  void filesAdded(boolean chopped, Collection<StoredTabletFile> files) {
    if (chopped) {
      synchronized (this) {
        fileMgr.addChoppedFiles(files);
      }
    }

    manager.compactableChanged(this);
  }

  /**
   * Tablet calls this signal a user compaction should run
   */
  void initiateUserCompaction(long compactionId, CompactionConfig compactionConfig) {
    checkIfUserCompactionCanceled();
    initiateSelection(CompactionKind.USER, compactionId, compactionConfig);
  }

  private void initiateSelection(CompactionKind kind) {
    if (kind != CompactionKind.SELECTOR) {
      return;
    }

    initiateSelection(CompactionKind.SELECTOR, null, null);
  }

  private void checkIfUserCompactionCanceled() {

    synchronized (this) {
      if (closed) {
        return;
      }

      if (!fileMgr.isSelected(CompactionKind.USER)) {
        return;
      }
    }

    var cancelId = tablet.getCompactionCancelID();

    lastSeenCompactionCancelId.getAndUpdate(prev -> Long.max(prev, cancelId));

    synchronized (this) {
      if (cancelId >= compactionId) {
        fileMgr.userCompactionCanceled();
      }
    }
  }

  /**
   * This method validates metadata about external compactions. It also extracts specific
   * information needed for user and selector compactions.
   */
  static Optional<SelectedInfo> processExternalMetadata(
      Map<ExternalCompactionId,ExternalCompactionMetadata> extCompactions,
      Supplier<Optional<Long>> tabletCompactionId, Set<StoredTabletFile> tabletFiles,
      Map<ExternalCompactionId,String> externalCompactionsToRemove) {

    // Check that external compactions have disjoint sets of files. Also check that each external
    // compaction only has files inside a tablet.
    Set<StoredTabletFile> seen = new HashSet<>();
    boolean overlap = false;

    for (var entry : extCompactions.entrySet()) {
      ExternalCompactionMetadata ecMeta = entry.getValue();
      if (!tabletFiles.containsAll(ecMeta.getJobFiles())) {
        externalCompactionsToRemove.putIfAbsent(entry.getKey(),
            "Has files outside of tablet files");
      } else if (!Collections.disjoint(seen, ecMeta.getJobFiles())) {
        overlap = true;
      }
      seen.addAll(ecMeta.getJobFiles());
    }

    if (overlap) {
      extCompactions.keySet().forEach(ecid -> {
        externalCompactionsToRemove.putIfAbsent(ecid, "Some external compaction files overlap");
      });
      return Optional.empty();
    }

    /*
     * The rest of the code validates user compaction metadata and extracts needed information.
     *
     * For user compactions a set of files is selected. Those files then get compacted by one or
     * more compactions until the set is empty. This method attempts to reconstruct the selected set
     * of files when a tablet is loaded with an external user compaction. It avoids repeating work
     * and when a user compaction completes, files are verified against the selected set. Since the
     * data is coming from persisted storage, lots of checks are done in this method rather than
     * assuming the persisted data is correct.
     */
    CompactionKind extKind = null;
    boolean unexpectedExternal = false;
    Set<StoredTabletFile> tmpSelectedFiles = null;
    Boolean initiallySelAll = null;
    Long cid = null;
    Boolean propDel = null;
    int count = 0;

    ArrayList<String> reasons = new ArrayList<>();

    for (Entry<ExternalCompactionId,ExternalCompactionMetadata> entry : extCompactions.entrySet()) {
      var ecMeta = entry.getValue();

      if (ecMeta.getKind() != CompactionKind.USER && ecMeta.getKind() != CompactionKind.SELECTOR) {
        continue;
      }

      count++;

      if (extKind == null || extKind == ecMeta.getKind()) {
        extKind = ecMeta.getKind();
      } else {
        reasons.add("Saw USER and SELECTOR");
        unexpectedExternal = true;
        break;
      }

      if (tmpSelectedFiles == null) {
        tmpSelectedFiles = Sets.union(ecMeta.getJobFiles(), ecMeta.getNextFiles());
      } else if (!Sets.union(ecMeta.getJobFiles(), ecMeta.getNextFiles())
          .equals(tmpSelectedFiles)) {
        reasons.add("Selected set of files differs");
        unexpectedExternal = true;
        break;
      }

      if (initiallySelAll == null) {
        initiallySelAll = ecMeta.getInitiallySelecteAll();
      } else if (initiallySelAll != ecMeta.getInitiallySelecteAll()) {
        unexpectedExternal = true;
        reasons.add("Disagreement on selectedAll");
        break;
      }

      if (ecMeta.getKind() == CompactionKind.USER) {
        if (ecMeta.getCompactionId() == null) {
          unexpectedExternal = true;
          reasons.add("Missing compactionId");
          break;
        } else if (cid == null) {
          cid = ecMeta.getCompactionId();
        } else if (!cid.equals(ecMeta.getCompactionId())) {
          unexpectedExternal = true;
          reasons.add("Disagreement on compactionId");
          break;
        }
      } else if (ecMeta.getCompactionId() != null) {
        unexpectedExternal = true;
        reasons.add("Unexpected compactionId");
        break;
      }

      if (propDel == null) {
        propDel = ecMeta.getPropagateDeletes();
      } else if (propDel != ecMeta.getPropagateDeletes()) {
        unexpectedExternal = true;
        reasons.add("Disagreement on propagateDeletes");
        break;
      }

    }

    if (propDel != null && !propDel && count > 1) {
      unexpectedExternal = true;
      reasons.add("Concurrent compactions not propagatingDeletes");
    }

    if (extKind == CompactionKind.USER) {
      Optional<Long> compactionId = tabletCompactionId.get();
      if (compactionId.isEmpty()) {
        unexpectedExternal = true;
        reasons.add("No compaction id in zookeeper");
      } else if (!compactionId.orElseThrow().equals(cid)) {
        unexpectedExternal = true;
        reasons.add("Compaction id mismatch with zookeeper");
      }
    }

    if (unexpectedExternal) {
      String reason = reasons.toString();
      extCompactions.entrySet().stream().filter(e -> {
        var kind = e.getValue().getKind();
        return kind == CompactionKind.SELECTOR || kind == CompactionKind.USER;
      }).map(Entry::getKey).forEach(ecid -> externalCompactionsToRemove.putIfAbsent(ecid, reason));
      return Optional.empty();
    }

    if (extKind != null) {
      return Optional.of(new SelectedInfo(initiallySelAll, tmpSelectedFiles, extKind));
    }

    return Optional.empty();
  }

  private void initiateSelection(CompactionKind kind, Long compactionId,
      CompactionConfig compactionConfig) {
    Preconditions.checkArgument(kind == CompactionKind.USER || kind == CompactionKind.SELECTOR);

    var localHelper = CompactableUtils.getHelper(kind, tablet, compactionId, compactionConfig);

    if (localHelper == null) {
      return;
    }

    synchronized (this) {
      if (closed) {
        log.trace("Selection of files was not initiated {} because closed", getExtent());
        return;
      }

      if (fileMgr.initiateSelection(kind, compactionId)) {
        this.chelper = localHelper;
        this.compactionId = compactionId;
        this.compactionConfig = compactionConfig;
        log.trace("Selected compaction status changed {} {} {} {}", getExtent(),
            fileMgr.getSelectionStatus(), compactionId, compactionConfig);
      } else {
        if (kind == CompactionKind.USER) {
          // Only log for user compaction because this code is only called when one is initiated via
          // the API call. For other compaction kinds the tserver will keep periodically attempting
          // to initiate which would result in lots of logs.
          log.trace(
              "Selection of files was not initiated {} compactionId:{} selectStatus:{} selectedFiles:{}",
              getExtent(), this.compactionId, fileMgr.selectStatus, fileMgr.selectedFiles.size());
        }
        return;
      }
    }

    selectFiles();

  }

  private void selectFiles() {

    CompactionHelper localHelper;

    synchronized (this) {
      if (!closed && fileMgr.beginSelection()) {
        localHelper = this.chelper;
      } else {
        return;
      }
    }

    try {
      var allFiles = tablet.getDatafiles();
      Set<StoredTabletFile> selectingFiles = localHelper.selectFiles(allFiles);

      if (selectingFiles.isEmpty()) {
        synchronized (this) {
          fileMgr.cancelSelection();
        }
      } else {
        var allSelected =
            allFiles.keySet().equals(Sets.union(selectingFiles, localHelper.getFilesToDrop()));
        synchronized (this) {
          fileMgr.finishSelection(selectingFiles, allSelected);
        }

        manager.compactableChanged(this);
      }
    } catch (Exception e) {
      log.error("Failed to select user compaction files {}", getExtent(), e);
    } finally {
      synchronized (this) {
        if (fileMgr.getSelectionStatus() == FileSelectionStatus.SELECTING) {
          fileMgr.cancelSelection();
        }
      }
    }
  }

  static Collection<String> asFileNames(Set<StoredTabletFile> files) {
    return Collections2.transform(files, StoredTabletFile::getFileName);
  }

  @Override
  public TableId getTableId() {
    return getExtent().tableId();
  }

  @Override
  public KeyExtent getExtent() {
    return tablet.getExtent();
  }

  @SuppressWarnings("removal")
  private boolean isCompactionStratConfigured() {
    var strategyClass = tablet.getTableConfiguration().get(Property.TABLE_COMPACTION_STRATEGY);
    return tablet.getTableConfiguration().isPropertySet(Property.TABLE_COMPACTION_STRATEGY)
        && strategyClass != null && !strategyClass.isBlank();
  }

  @Override
  public Optional<Files> getFiles(CompactionServiceId service, CompactionKind kind) {

    if (!service.equals(getConfiguredService(kind))) {
      return Optional.empty();
    }

    servicesUsed.add(service);

    var files = tablet.getDatafiles();

    // very important to call following outside of lock
    initiateSelection(kind);

    if (kind == CompactionKind.USER) {
      checkIfUserCompactionCanceled();
    }

    synchronized (this) {

      if (closed) {
        return Optional.empty();
      }

      var runningJobsCopy = Set.copyOf(runningJobs);

      Set<StoredTabletFile> candidates = fileMgr.getCandidates(
          Collections.unmodifiableSet(files.keySet()), kind, isCompactionStratConfigured());

      if (candidates.isEmpty()) {
        return Optional.empty();
      } else if (kind == CompactionKind.USER) {
        Map<String,String> hints = compactionConfig.getExecutionHints();
        return Optional.of(new Compactable.Files(files, candidates, runningJobsCopy, hints));
      } else {
        return Optional.of(new Compactable.Files(files, candidates, runningJobsCopy));
      }
    }
  }

  class CompactionCheck {
    private final Supplier<Boolean> expensiveCheck;
    private final Supplier<Boolean> inexpensiveCheck;

    public CompactionCheck(CompactionServiceId service, CompactionKind kind,
        BooleanSupplier keepRunning, Long compactionId) {
      this.expensiveCheck = Suppliers.memoizeWithExpiration(
          () -> service.equals(getConfiguredService(kind)) && keepRunning.getAsBoolean(), 3,
          TimeUnit.SECONDS);
      this.inexpensiveCheck = Suppliers.memoizeWithExpiration(() -> {
        if (closed
            || (kind == CompactionKind.USER && lastSeenCompactionCancelId.get() >= compactionId)) {
          return false;
        }
        return true;
      }, 50, TimeUnit.MILLISECONDS);
    }

    public boolean isCompactionEnabled() {
      return inexpensiveCheck.get() && expensiveCheck.get();
    }
  }

  static class CompactionInfo {
    Set<StoredTabletFile> jobFiles;
    Long checkCompactionId = null;
    boolean propagateDeletes = true;
    CompactionHelper localHelper;
    List<IteratorSetting> iters = List.of();
    CompactionConfig localCompactionCfg;
    // At the time when a set of files was selected, was the complete set of tablet files
    boolean initiallySelectedAll;
    Set<StoredTabletFile> selectedFiles;
  }

  /**
   * Attempt to reserve files for compaction. It's possible that since a compaction job was queued
   * that things have changed and there is no longer anything to do for the job. In this case
   * Optional.empty() is returned.
   */
  private Optional<CompactionInfo> reserveFilesForCompaction(CompactionServiceId service,
      CompactionJob job) {
    CompactionInfo cInfo = new CompactionInfo();

    cInfo.jobFiles = job.getFiles().stream()
        .map(cf -> ((CompactableFileImpl) cf).getStoredTabletFile()).collect(Collectors.toSet());

    if (job.getKind() == CompactionKind.USER) {
      checkIfUserCompactionCanceled();
    }

    synchronized (this) {
      if (closed) {
        return Optional.empty();
      }

      if (runningJobs.contains(job)) {
        return Optional.empty();
      }

      if (!service.equals(getConfiguredService(job.getKind()))) {
        return Optional.empty();
      }

      if (!fileMgr.reserveFiles(job, cInfo.jobFiles)) {
        return Optional.empty();
      }

      if (!addJob(job)) {
        throw new AssertionError();
      }

      switch (job.getKind()) {
        case SELECTOR:
        case USER:
          var si = fileMgr.getReservedInfo();

          if (job.getKind() == si.selectKind && si.initiallySelectedAll
              && cInfo.jobFiles.containsAll(si.selectedFiles)) {
            cInfo.propagateDeletes = false;
          }

          cInfo.selectedFiles = si.selectedFiles;
          cInfo.initiallySelectedAll = si.initiallySelectedAll;

          break;
        default:
          if (((CompactionJobImpl) job).selectedAll()) {
            // At the time when the job was created all files were selected, so deletes can be
            // dropped.
            cInfo.propagateDeletes = false;
          }

          cInfo.selectedFiles = Set.of();
      }

      if (job.getKind() == CompactionKind.USER) {
        cInfo.iters = compactionConfig.getIterators();
        cInfo.checkCompactionId = this.compactionId;
      }

      cInfo.localHelper = this.chelper;
      cInfo.localCompactionCfg = this.compactionConfig;
    }

    // Check to ensure the tablet actually has these files now that they are reserved. Compaction
    // jobs are queued for some period of time and then they try to run. Things could change while
    // they are queued. This check ensures that the files a job is reserving still exists in the
    // tablet. Without this check the compaction could run and then fail to commit on the tablet.
    // The tablet and this class have separate locks that should not be held at the same time. This
    // check is done after the file are exclusively reserved in this class to avoid race conditions.
    if (!tablet.getDatafiles().keySet().containsAll(cInfo.jobFiles)) {
      // The tablet does not know of all these files, so unreserve them.
      completeCompaction(job, cInfo.jobFiles, Optional.empty(), true);
      return Optional.empty();
    }

    return Optional.of(cInfo);
  }

  private void completeCompaction(CompactionJob job, Set<StoredTabletFile> jobFiles,
      Optional<StoredTabletFile> metaFile, boolean successful) {
    synchronized (this) {
      Preconditions.checkState(removeJob(job));
      fileMgr.completed(job, jobFiles, metaFile, successful);

      if (!compactionRunning) {
        notifyAll();
      }
    }

    checkifChopComplete(tablet.getDatafiles().keySet());
    selectFiles();
  }

  @Override
  public void compact(CompactionServiceId service, CompactionJob job, BooleanSupplier keepRunning,
      RateLimiter readLimiter, RateLimiter writeLimiter, long queuedTime) {

    Optional<CompactionInfo> ocInfo = reserveFilesForCompaction(service, job);
    if (ocInfo.isEmpty()) {
      return;
    }

    var cInfo = ocInfo.orElseThrow();
    Optional<StoredTabletFile> newFile = Optional.empty();
    long startTime = System.currentTimeMillis();
    CompactionKind kind = job.getKind();

    CompactionStats stats = new CompactionStats();

    boolean successful = false;
    try {
      TabletLogger.compacting(getExtent(), job, cInfo.localCompactionCfg);
      tablet.incrementStatusMajor();
      var check = new CompactionCheck(service, kind, keepRunning, cInfo.checkCompactionId);
      TabletFile tmpFileName = tablet.getNextMapFilenameForMajc(cInfo.propagateDeletes);
      var compactEnv = new MajCEnv(kind, check, readLimiter, writeLimiter, cInfo.propagateDeletes);

      SortedMap<StoredTabletFile,DataFileValue> allFiles = tablet.getDatafiles();
      HashMap<StoredTabletFile,DataFileValue> compactFiles = new HashMap<>();
      cInfo.jobFiles.forEach(file -> compactFiles.put(file, allFiles.get(file)));

      stats = CompactableUtils.compact(tablet, job, cInfo, compactEnv, compactFiles, tmpFileName);

      newFile = CompactableUtils.bringOnline(tablet.getDatafileManager(), cInfo, stats,
          compactFiles, allFiles, kind, tmpFileName);

      TabletLogger.compacted(getExtent(), job, newFile.orElse(null));
      successful = true;
    } catch (CompactionCanceledException cce) {
      log.debug("Compaction canceled {} ", getExtent());
    } catch (Exception e) {
      newFile = Optional.empty();
      throw new RuntimeException(e);
    } finally {
      completeCompaction(job, cInfo.jobFiles, newFile, successful);
      tablet.updateTimer(MAJOR, queuedTime, startTime, stats.getEntriesRead(), newFile == null);
    }
  }

  @Override
  public ExternalCompactionJob reserveExternalCompaction(CompactionServiceId service,
      CompactionJob job, String compactorId, ExternalCompactionId externalCompactionId) {

    Preconditions.checkState(!tablet.getExtent().isMeta());

    Optional<CompactionInfo> ocInfo = reserveFilesForCompaction(service, job);
    if (ocInfo.isEmpty()) {
      return null;
    }

    var cInfo = ocInfo.orElseThrow();

    try {
      Map<String,String> overrides =
          CompactableUtils.getOverrides(job.getKind(), tablet, cInfo.localHelper, job.getFiles());

      TabletFile compactTmpName = tablet.getNextMapFilenameForMajc(cInfo.propagateDeletes);

      ExternalCompactionInfo ecInfo = new ExternalCompactionInfo();

      ecInfo.meta = new ExternalCompactionMetadata(cInfo.jobFiles,
          Sets.difference(cInfo.selectedFiles, cInfo.jobFiles), compactTmpName, compactorId,
          job.getKind(), job.getPriority(), job.getExecutor(), cInfo.propagateDeletes,
          cInfo.initiallySelectedAll, cInfo.checkCompactionId);

      tablet.getContext().getAmple().mutateTablet(getExtent())
          .putExternalCompaction(externalCompactionId, ecInfo.meta).mutate();

      ecInfo.job = job;

      externalCompactions.put(externalCompactionId, ecInfo);

      SortedMap<StoredTabletFile,DataFileValue> allFiles = tablet.getDatafiles();
      HashMap<StoredTabletFile,DataFileValue> compactFiles = new HashMap<>();
      cInfo.jobFiles.forEach(file -> compactFiles.put(file, allFiles.get(file)));

      TabletLogger.compacting(getExtent(), job, cInfo.localCompactionCfg);

      return new ExternalCompactionJob(compactFiles, cInfo.propagateDeletes, compactTmpName,
          getExtent(), externalCompactionId, job.getKind(), cInfo.iters, cInfo.checkCompactionId,
          overrides);

    } catch (Exception e) {
      externalCompactions.remove(externalCompactionId);
      completeCompaction(job, cInfo.jobFiles, Optional.empty(), false);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void commitExternalCompaction(ExternalCompactionId extCompactionId, long fileSize,
      long entries) {

    synchronized (this) {
      if (closed) {
        return;
      }

      // defend against multiple threads trying to commit the same ECID and force tablet close to
      // wait on any pending commits
      if (!externalCompactionsCommitting.add(extCompactionId)) {
        return;
      }
    }
    try {

      ExternalCompactionInfo ecInfo = externalCompactions.get(extCompactionId);

      if (ecInfo != null) {
        log.debug("Attempting to commit external compaction {}", extCompactionId);
        Optional<StoredTabletFile> metaFile = Optional.empty();
        boolean successful = false;
        try {
          metaFile =
              tablet.getDatafileManager().bringMajorCompactionOnline(ecInfo.meta.getJobFiles(),
                  ecInfo.meta.getCompactTmpName(), ecInfo.meta.getCompactionId(),
                  Sets.union(ecInfo.meta.getJobFiles(), ecInfo.meta.getNextFiles()),
                  new DataFileValue(fileSize, entries), Optional.of(extCompactionId));
          TabletLogger.compacted(getExtent(), ecInfo.job, metaFile.orElse(null));
          successful = true;
        } catch (Exception e) {
          metaFile = Optional.empty();
          log.error("Error committing external compaction: id: {}, extent: {}", extCompactionId,
              getExtent(), e);
          throw new RuntimeException(e);
        } finally {
          completeCompaction(ecInfo.job, ecInfo.meta.getJobFiles(), metaFile, successful);
          externalCompactions.remove(extCompactionId);
          log.debug("Completed commit of external compaction {}", extCompactionId);
        }
      } else {
        log.debug("Ignoring request to commit external compaction that is unknown {}",
            extCompactionId);
      }

      tablet.getContext().getAmple().deleteExternalCompactionFinalStates(List.of(extCompactionId));
    } finally {
      synchronized (this) {
        Preconditions.checkState(externalCompactionsCommitting.remove(extCompactionId));
        notifyAll();
      }
    }
  }

  @Override
  public void externalCompactionFailed(ExternalCompactionId ecid) {

    synchronized (this) {
      if (closed) {
        return;
      }

      if (!externalCompactionsCommitting.add(ecid)) {
        return;
      }
    }
    try {

      ExternalCompactionInfo ecInfo = externalCompactions.get(ecid);

      if (ecInfo != null) {
        tablet.getContext().getAmple().mutateTablet(getExtent()).deleteExternalCompaction(ecid)
            .mutate();
        completeCompaction(ecInfo.job, ecInfo.meta.getJobFiles(), Optional.empty(), false);
        externalCompactions.remove(ecid);
        log.debug("Processed external compaction failure: id: {}, extent: {}", ecid, getExtent());
      } else {
        log.debug("Ignoring request to fail external compaction that is unknown {}", ecid);
      }

      tablet.getContext().getAmple().deleteExternalCompactionFinalStates(List.of(ecid));
    } finally {
      synchronized (this) {
        Preconditions.checkState(externalCompactionsCommitting.remove(ecid));
        notifyAll();
      }
    }
  }

  @Override
  public boolean isActive(ExternalCompactionId ecid) {
    return externalCompactions.containsKey(ecid);
  }

  @Override
  public void getExternalCompactionIds(Consumer<ExternalCompactionId> idConsumer) {
    externalCompactions.forEach((ecid, eci) -> idConsumer.accept(ecid));
  }

  @Override
  public CompactionServiceId getConfiguredService(CompactionKind kind) {

    Map<String,String> debugHints = null;

    try {
      var dispatcher = tablet.getTableConfiguration().getCompactionDispatcher();

      if (dispatcher == null) {
        log.error(
            "Failed to dispatch compaction, no dispatcher. extent:{} kind:{} hints:{}, falling back to {} service. Unable to instantiate dispatcher plugin. Check server log.",
            getExtent(), kind, debugHints, CompactionServicesConfig.DEFAULT_SERVICE);
        return CompactionServicesConfig.DEFAULT_SERVICE;
      }

      Map<String,String> tmpHints = Map.of();

      if (kind == CompactionKind.USER) {
        synchronized (this) {
          if (fileMgr.getSelectionStatus() != FileSelectionStatus.NOT_ACTIVE
              && fileMgr.getSelectionStatus() != FileSelectionStatus.CANCELED
              && fileMgr.getSelectionKind() == CompactionKind.USER) {
            tmpHints = compactionConfig.getExecutionHints();
          }
        }
      }

      var hints = tmpHints;
      debugHints = hints;

      var dispatch = dispatcher.dispatch(new DispatchParameters() {

        private final ServiceEnvironment senv = new ServiceEnvironmentImpl(tablet.getContext());

        @Override
        public ServiceEnvironment getServiceEnv() {
          return senv;
        }

        @Override
        public Map<String,String> getExecutionHints() {
          return hints;
        }

        @Override
        public CompactionKind getCompactionKind() {
          return kind;
        }

        @Override
        public CompactionServices getCompactionServices() {
          return manager.getServices();
        }
      });

      return dispatch.getService();
    } catch (RuntimeException e) {
      log.error(
          "Failed to dispatch compaction due to exception. extent:{} kind:{} hints:{}, falling back to {} service.",
          getExtent(), kind, debugHints, CompactionServicesConfig.DEFAULT_SERVICE, e);
      return CompactionServicesConfig.DEFAULT_SERVICE;
    }
  }

  @Override
  public double getCompactionRatio() {
    return tablet.getTableConfiguration().getFraction(Property.TABLE_MAJC_RATIO);
  }

  public boolean isMajorCompactionRunning() {
    // this method intentionally not synchronized because its called by stats code.
    return compactionRunning;
  }

  public boolean isMajorCompactionQueued() {
    return manager.isCompactionQueued(getExtent(), servicesInUse.get());
  }

  /**
   * Interrupts and waits for any running compactions. After this method returns, no compactions
   * should be running and none should be able to start.
   */
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }

      closed = true;

      // Wait while internal jobs are running or external compactions are committing. When
      // chopStatus is MARKING or selectStatus is SELECTING, there may be metadata table writes so
      // wait on those. Do not wait on external compactions that are running.
      Predicate<CompactionJob> jobsToWaitFor =
          job -> !((CompactionExecutorIdImpl) job.getExecutor()).isExternalId();
      while (runningJobs.stream().anyMatch(jobsToWaitFor)
          || !externalCompactionsCommitting.isEmpty()
          || fileMgr.chopStatus == ChopSelectionStatus.MARKING
          || fileMgr.selectStatus == FileSelectionStatus.SELECTING) {

        log.debug(
            "Closing {} is waiting on {} running compactions, {} committing external compactions, chop marking {}, file selection {}",
            getExtent(), runningJobs.stream().filter(jobsToWaitFor).count(),
            externalCompactionsCommitting.size(), fileMgr.chopStatus == ChopSelectionStatus.MARKING,
            fileMgr.selectStatus == FileSelectionStatus.SELECTING);

        try {
          wait(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
    manager.compactableClosed(getExtent(), servicesUsed, externalCompactions.keySet());
  }
}

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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher.DispatchParameters;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.compactions.CompactionManager;
import org.apache.accumulo.tserver.mastermessage.TabletStatusMessage;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionCanceledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

/**
 * This class exists between compaction services and tablets and tracks state related to compactions
 * for a tablet. This class was written to mainly contain code related to tracking files, state, and
 * synchronization. All other code was placed in {@link CompactableUtils} inorder to make this class
 * easier to analyze.
 */
public class CompactableImpl implements Compactable {

  private static final Logger log = LoggerFactory.getLogger(CompactableImpl.class);

  private final Tablet tablet;

  private Set<StoredTabletFile> allCompactingFiles = new HashSet<>();
  private Set<CompactionJob> runnningJobs = new HashSet<>();
  private volatile boolean compactionRunning = false;

  private Set<StoredTabletFile> selectedFiles = new HashSet<>();

  private Set<StoredTabletFile> allFilesWhenChopStarted = new HashSet<>();

  // track files produced by compactions of this tablet, those are considered chopped
  private Set<StoredTabletFile> choppedFiles = new HashSet<>();
  private SpecialStatus chopStatus = SpecialStatus.NOT_ACTIVE;

  private Supplier<Set<CompactionServiceId>> servicesInUse;

  // status of special compactions
  private enum SpecialStatus {
    NEW, SELECTING, SELECTED, NOT_ACTIVE, CANCELED
  }

  private SpecialStatus selectStatus = SpecialStatus.NOT_ACTIVE;
  private CompactionKind selectKind = null;
  private boolean selectedAll = false;
  private CompactionHelper chelper = null;
  private Long compactionId;
  private CompactionConfig compactionConfig;

  private CompactionManager manager;

  AtomicLong lastSeenCompactionCancelId = new AtomicLong(Long.MIN_VALUE);

  private volatile boolean closed = false;

  // This interface exists for two purposes. First it allows abstraction of new and old
  // implementations for user pluggable file selection code. Second it facilitates placing code
  // outside of this class.
  public static interface CompactionHelper {
    Set<StoredTabletFile> selectFiles(SortedMap<StoredTabletFile,DataFileValue> allFiles);

    Set<StoredTabletFile> getFilesToDrop();

    AccumuloConfiguration override(AccumuloConfiguration conf, Set<CompactableFile> files);

  }

  public CompactableImpl(Tablet tablet, CompactionManager manager) {
    this.tablet = tablet;
    this.manager = manager;
    this.servicesInUse = Suppliers.memoizeWithExpiration(() -> {
      HashSet<CompactionServiceId> servicesIds = new HashSet<>();
      for (CompactionKind kind : CompactionKind.values()) {
        servicesIds.add(getConfiguredService(kind));
      }
      return Set.copyOf(servicesIds);
    }, 2, TimeUnit.SECONDS);
  }

  void initiateChop() {

    Set<StoredTabletFile> allFiles = tablet.getDatafiles().keySet();
    Set<StoredTabletFile> filesToExamine = new HashSet<>(allFiles);

    synchronized (this) {
      if (chopStatus == SpecialStatus.NOT_ACTIVE) {
        chopStatus = SpecialStatus.SELECTING;
        filesToExamine.removeAll(choppedFiles);
        filesToExamine.removeAll(allCompactingFiles);
      } else {
        return;
      }
    }

    Set<StoredTabletFile> unchoppedFiles = selectChopFiles(filesToExamine);

    synchronized (this) {
      Preconditions.checkState(chopStatus == SpecialStatus.SELECTING);
      choppedFiles.addAll(Sets.difference(filesToExamine, unchoppedFiles));
      chopStatus = SpecialStatus.SELECTED;
      this.allFilesWhenChopStarted.clear();
      this.allFilesWhenChopStarted.addAll(allFiles);

      var filesToChop = getFilesToChop(allFiles);
      if (!filesToChop.isEmpty()) {
        TabletLogger.selected(getExtent(), CompactionKind.CHOP, filesToChop);
      }
    }

    checkifChopComplete(tablet.getDatafiles().keySet());
  }

  private synchronized Set<StoredTabletFile> getFilesToChop(Set<StoredTabletFile> allFiles) {
    Preconditions.checkState(chopStatus == SpecialStatus.SELECTED);
    var copy = new HashSet<>(allFilesWhenChopStarted);
    copy.retainAll(allFiles);
    copy.removeAll(choppedFiles);
    return copy;
  }

  private void checkifChopComplete(Set<StoredTabletFile> allFiles) {

    boolean completed = false;

    synchronized (this) {
      if (chopStatus == SpecialStatus.SELECTED) {
        if (getFilesToChop(allFiles).isEmpty()) {
          chopStatus = SpecialStatus.NOT_ACTIVE;
          completed = true;
        }
      }

      choppedFiles.retainAll(allFiles);
    }

    if (completed) {
      markChopped();
      TabletLogger.selected(getExtent(), CompactionKind.CHOP, Set.of());
    }
  }

  private void markChopped() {
    MetadataTableUtil.chopped(tablet.getTabletServer().getContext(), getExtent(),
        tablet.getTabletServer().getLock());
    tablet.getTabletServer()
        .enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.CHOPPED, getExtent()));
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
        choppedFiles.addAll(files);
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
    if (kind != CompactionKind.SELECTOR)
      return;

    initiateSelection(CompactionKind.SELECTOR, null, null);
  }

  private boolean noneRunning(CompactionKind kind) {
    return runnningJobs.stream().noneMatch(job -> job.getKind() == kind);
  }

  private void checkIfUserCompactionCanceled() {

    synchronized (this) {
      if (closed)
        return;

      if (selectStatus != SpecialStatus.SELECTED || selectKind != CompactionKind.USER) {
        return;
      }
    }

    var cancelId = tablet.getCompactionCancelID();

    lastSeenCompactionCancelId.getAndUpdate(prev -> Long.max(prev, cancelId));

    synchronized (this) {
      if (selectStatus == SpecialStatus.SELECTED && selectKind == CompactionKind.USER) {
        if (cancelId >= compactionId) {
          if (noneRunning(CompactionKind.USER)) {
            selectStatus = SpecialStatus.NOT_ACTIVE;
            log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
          } else {
            selectStatus = SpecialStatus.CANCELED;
            log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
          }
        }
      }
    }
  }

  private void initiateSelection(CompactionKind kind, Long compactionId,
      CompactionConfig compactionConfig) {
    Preconditions.checkArgument(kind == CompactionKind.USER || kind == CompactionKind.SELECTOR);

    var localHelper = CompactableUtils.getHelper(kind, tablet, compactionId, compactionConfig);

    if (localHelper == null)
      return;

    synchronized (this) {
      if (closed)
        return;

      if (selectStatus == SpecialStatus.NOT_ACTIVE || (kind == CompactionKind.USER
          && selectKind == CompactionKind.SELECTOR && noneRunning(CompactionKind.SELECTOR))) {
        selectStatus = SpecialStatus.NEW;
        selectKind = kind;
        selectedFiles.clear();
        selectedAll = false;
        this.chelper = localHelper;
        this.compactionId = compactionId;
        this.compactionConfig = compactionConfig;
        log.trace("Selected compaction status changed {} {} {} {}", getExtent(), selectStatus,
            compactionId, compactionConfig);
      } else {
        return;
      }
    }

    selectFiles();

  }

  private void selectFiles() {

    CompactionHelper localHelper;

    synchronized (this) {
      if (selectStatus == SpecialStatus.NEW && allCompactingFiles.isEmpty()) {
        selectedFiles.clear();
        selectStatus = SpecialStatus.SELECTING;
        localHelper = this.chelper;
        log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
      } else {
        return;
      }
    }

    try {
      var allFiles = tablet.getDatafiles();
      Set<StoredTabletFile> selectingFiles = localHelper.selectFiles(allFiles);

      if (selectingFiles.isEmpty()) {
        synchronized (this) {
          Preconditions.checkState(selectStatus == SpecialStatus.SELECTING);
          selectStatus = SpecialStatus.NOT_ACTIVE;
          log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
        }
      } else {
        var allSelected =
            allFiles.keySet().equals(Sets.union(selectingFiles, localHelper.getFilesToDrop()));
        synchronized (this) {
          Preconditions.checkState(selectStatus == SpecialStatus.SELECTING);
          selectStatus = SpecialStatus.SELECTED;
          selectedFiles.addAll(selectingFiles);
          selectedAll = allSelected;
          log.trace("Selected compaction status changed {} {} {} {}", getExtent(), selectStatus,
              selectedAll, asFileNames(selectedFiles));
          TabletLogger.selected(getExtent(), selectKind, selectedFiles);
        }

        manager.compactableChanged(this);
      }

    } catch (Exception e) {
      synchronized (this) {
        if (selectStatus == SpecialStatus.SELECTING)
          selectStatus = SpecialStatus.NOT_ACTIVE;
        log.error("Failed to select user compaction files {}", getExtent(), e);
        log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
        selectedFiles.clear();
      }
    }

  }

  private Collection<String> asFileNames(Set<StoredTabletFile> files) {
    return Collections2.transform(files, StoredTabletFile::getFileName);
  }

  private synchronized void selectedCompactionCompleted(CompactionJob job,
      Set<StoredTabletFile> jobFiles, StoredTabletFile newFile) {
    Preconditions.checkArgument(
        job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR);
    Preconditions.checkState(selectedFiles.containsAll(jobFiles));
    Preconditions.checkState(
        (selectStatus == SpecialStatus.SELECTED || selectStatus == SpecialStatus.CANCELED)
            && selectKind == job.getKind());

    selectedFiles.removeAll(jobFiles);

    if (selectedFiles.isEmpty()
        || (selectStatus == SpecialStatus.CANCELED && noneRunning(selectKind))) {
      selectStatus = SpecialStatus.NOT_ACTIVE;
      log.trace("Selected compaction status changed {} {}", getExtent(), selectStatus);
    } else if (selectStatus == SpecialStatus.SELECTED) {
      selectedFiles.add(newFile);
      log.trace("Compacted subset of selected files {} {} -> {}", getExtent(),
          asFileNames(jobFiles), newFile.getFileName());
    } else {
      log.debug("Canceled selected compaction completed {} but others still running ", getExtent());
    }

    TabletLogger.selected(getExtent(), selectKind, selectedFiles);
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
    return tablet.getTableConfiguration().isPropertySet(Property.TABLE_COMPACTION_STRATEGY, true);
  }

  @Override
  public Optional<Files> getFiles(CompactionServiceId service, CompactionKind kind) {

    if (!service.equals(getConfiguredService(kind)))
      return Optional.empty();

    var files = tablet.getDatafiles();

    // very important to call following outside of lock
    initiateSelection(kind);

    if (kind == CompactionKind.USER)
      checkIfUserCompactionCanceled();

    synchronized (this) {

      if (closed)
        return Optional.empty();

      if (!files.keySet().containsAll(allCompactingFiles)) {
        log.trace("Ignoring because compacting not a subset {}", getExtent());

        // A compaction finished, so things are out of date. This can happen because this class and
        // tablet have separate locks, its ok.
        return Optional.of(new Compactable.Files(files, Set.of(), Set.of()));
      }

      var allCompactingCopy = Set.copyOf(allCompactingFiles);
      var runningJobsCopy = Set.copyOf(runnningJobs);

      switch (kind) {
        case SYSTEM: {
          if (isCompactionStratConfigured())
            return Optional.of(new Compactable.Files(files, Set.of(), runningJobsCopy));

          switch (selectStatus) {
            case NOT_ACTIVE:
            case CANCELED:
              return Optional.of(new Compactable.Files(files,
                  Sets.difference(files.keySet(), allCompactingCopy), runningJobsCopy));
            case NEW:
            case SELECTING:
              return Optional.of(new Compactable.Files(files, Set.of(), runningJobsCopy));
            case SELECTED: {
              Set<StoredTabletFile> candidates = new HashSet<>(files.keySet());
              candidates.removeAll(allCompactingCopy);
              candidates.removeAll(selectedFiles);
              candidates = Collections.unmodifiableSet(candidates);
              return Optional.of(new Compactable.Files(files, candidates, runningJobsCopy));
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
              return Optional.of(new Compactable.Files(files, Set.of(), runningJobsCopy));
            case SELECTED: {
              if (selectKind == kind) {
                Set<StoredTabletFile> candidates = new HashSet<>(selectedFiles);
                candidates.removeAll(allCompactingFiles);
                candidates = Collections.unmodifiableSet(candidates);
                Preconditions.checkState(files.keySet().containsAll(candidates),
                    "selected files not in all files %s %s", candidates, files.keySet());
                Map<String,String> hints = Map.of();
                if (kind == CompactionKind.USER)
                  hints = compactionConfig.getExecutionHints();
                return Optional.of(new Compactable.Files(files, Set.copyOf(selectedFiles),
                    runningJobsCopy, hints));
              } else {
                return Optional.of(new Compactable.Files(files, Set.of(), runningJobsCopy));
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
              return Optional.of(new Compactable.Files(files, Set.of(), runningJobsCopy));
            case SELECTED: {
              if (selectStatus == SpecialStatus.NEW || selectStatus == SpecialStatus.SELECTING)
                return Optional.of(new Compactable.Files(files, Set.of(), runningJobsCopy));

              var filesToChop = getFilesToChop(files.keySet());
              filesToChop.removeAll(allCompactingFiles);
              filesToChop = Collections.unmodifiableSet(filesToChop);
              if (selectStatus == SpecialStatus.SELECTED)
                filesToChop.removeAll(selectedFiles);
              return Optional.of(new Compactable.Files(files, filesToChop, runningJobsCopy));
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
  }

  class CompactionCheck {
    private Supplier<Boolean> memoizedCheck;

    public CompactionCheck(CompactionServiceId service, CompactionKind kind, Long compactionId) {
      this.memoizedCheck = Suppliers.memoizeWithExpiration(() -> {
        if (closed)
          return false;
        if (!service.equals(getConfiguredService(kind)))
          return false;
        if (kind == CompactionKind.USER && lastSeenCompactionCancelId.get() >= compactionId)
          return false;

        return true;
      }, 100, TimeUnit.MILLISECONDS);
    }

    public boolean isCompactionEnabled(long entriesCompacted) {
      return memoizedCheck.get();
    }
  }

  @Override
  public void compact(CompactionServiceId service, CompactionJob job) {

    Set<StoredTabletFile> jobFiles = job.getFiles().stream()
        .map(cf -> ((CompactableFileImpl) cf).getStortedTabletFile()).collect(Collectors.toSet());

    Long compactionId = null;
    Long checkCompactionId = null;
    boolean propogateDeletes = true;
    CompactionHelper localHelper;
    List<IteratorSetting> iters = List.of();
    CompactionConfig localCompactionCfg;

    if (job.getKind() == CompactionKind.USER)
      checkIfUserCompactionCanceled();

    synchronized (this) {
      if (closed)
        return;

      if (!service.equals(getConfiguredService(job.getKind())))
        return;

      switch (selectStatus) {
        case NEW:
        case SELECTING:
          log.trace(
              "Ignoring compaction because files are being selected for user compaction {} {}",
              getExtent(), job);
          return;
        case SELECTED: {
          if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
            if (selectKind == job.getKind()) {
              if (!selectedFiles.containsAll(jobFiles)) {
                log.error("Ignoring {} compaction that does not contain selected files {} {} {}",
                    job.getKind(), getExtent(), asFileNames(selectedFiles), asFileNames(jobFiles));
                return;
              }
            } else {
              log.trace("Ingoring {} compaction because not selected kind {}", job.getKind(),
                  getExtent());
              return;
            }
          } else if (!Collections.disjoint(selectedFiles, jobFiles)) {
            log.trace("Ingoring compaction that overlaps with selected files {} {} {}", getExtent(),
                job.getKind(), asFileNames(Sets.intersection(selectedFiles, jobFiles)));
            return;
          }
          break;
        }
        case CANCELED:
        case NOT_ACTIVE: {
          if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
            log.trace("Ignoring {} compaction because selectStatus is {} for {}", job.getKind(),
                selectStatus, getExtent());
            return;
          }
          break;
        }
        default:
          throw new AssertionError();
      }

      if (Collections.disjoint(allCompactingFiles, jobFiles)) {
        allCompactingFiles.addAll(jobFiles);
        runnningJobs.add(job);
      } else {
        return;
      }

      compactionRunning = !allCompactingFiles.isEmpty();

      switch (job.getKind()) {
        case SELECTOR:
        case USER:
          Preconditions.checkState(selectStatus == SpecialStatus.SELECTED);
          if (job.getKind() == selectKind && selectedAll && jobFiles.containsAll(selectedFiles)) {
            propogateDeletes = false;
          }
          break;
        default:
          if (((CompactionJobImpl) job).selectedAll()) {
            // At the time when the job was created all files were selected, so deletes can be
            // dropped.
            propogateDeletes = false;
          }
      }

      if (job.getKind() == CompactionKind.USER && selectKind == job.getKind()
          && selectedFiles.equals(jobFiles)) {
        compactionId = this.compactionId;
      }

      if (job.getKind() == CompactionKind.USER) {
        iters = compactionConfig.getIterators();
        checkCompactionId = this.compactionId;
      }

      localHelper = this.chelper;
      localCompactionCfg = this.compactionConfig;
    }

    StoredTabletFile metaFile = null;
    try {

      TabletLogger.compacting(getExtent(), job, localCompactionCfg);

      metaFile = CompactableUtils.compact(tablet, job, jobFiles, compactionId, propogateDeletes,
          localHelper, iters, new CompactionCheck(service, job.getKind(), checkCompactionId));

      TabletLogger.compacted(getExtent(), job, metaFile);

    } catch (CompactionCanceledException cce) {
      log.debug("Compaction canceled {} ", getExtent());
      metaFile = null;
    } catch (Exception e) {
      metaFile = null;
      throw new RuntimeException(e);
    } finally {
      synchronized (this) {
        Preconditions.checkState(allCompactingFiles.removeAll(jobFiles));
        Preconditions.checkState(runnningJobs.remove(job));
        compactionRunning = !allCompactingFiles.isEmpty();

        if (allCompactingFiles.isEmpty()) {
          notifyAll();
        }

        if (metaFile != null) {
          choppedFiles.add(metaFile);
        }
      }

      checkifChopComplete(tablet.getDatafiles().keySet());

      if ((job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR)
          && metaFile != null)
        selectedCompactionCompleted(job, jobFiles, metaFile);
      else
        selectFiles();
    }
  }

  @Override
  public CompactionServiceId getConfiguredService(CompactionKind kind) {

    Map<String,String> debugHints = null;

    try {
      var dispatcher = tablet.getTableConfiguration().getCompactionDispatcher();

      Map<String,String> tmpHints = Map.of();

      if (kind == CompactionKind.USER) {
        synchronized (this) {
          if (selectStatus != SpecialStatus.NOT_ACTIVE && selectStatus != SpecialStatus.CANCELED
              && selectKind == CompactionKind.USER) {
            tmpHints = compactionConfig.getExecutionHints();
          }
        }
      }

      var hints = tmpHints;
      debugHints = hints;

      var directives = dispatcher.dispatch(new DispatchParameters() {

        @Override
        public ServiceEnvironment getServiceEnv() {
          return new ServiceEnvironmentImpl(tablet.getContext());
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

      return directives.getService();
    } catch (RuntimeException e) {
      log.error("Failed to dispatch compaction {} kind:{} hints:{}, falling back to {} service.",
          getExtent(), kind, debugHints, CompactionManager.DEFAULT_SERVICE, e);
      return CompactionManager.DEFAULT_SERVICE;
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
   * Interrupts and waits for any running compactions. After this method returns no compactions
   * should be running and none should be able to start.
   */
  public synchronized void close() {
    if (closed)
      return;

    closed = true;

    while (!allCompactingFiles.isEmpty()) {
      try {
        wait(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }
}

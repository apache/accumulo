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

import static org.apache.accumulo.tserver.TabletStatsKeeper.Operation.MAJOR;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
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
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.compaction.Compactor.CompactionCanceledException;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.compactions.CompactionManager;
import org.apache.accumulo.tserver.compactions.ExternalCompactionJob;
import org.apache.accumulo.tserver.managermessage.TabletStatusMessage;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException.NoNodeException;
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

  private static class ExternalCompactionInfo {
    ExternalCompactionMetadata meta;
    CompactionJob job;
  }

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

  private Set<CompactionServiceId> servicesUsed = new ConcurrentSkipListSet<>();

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

  private Map<ExternalCompactionId,ExternalCompactionInfo> externalCompactions =
      new ConcurrentHashMap<>();

  private Set<ExternalCompactionId> externalCompactionsCommitting = new HashSet<>();

  // This interface exists for two purposes. First it allows abstraction of new and old
  // implementations for user pluggable file selection code. Second it facilitates placing code
  // outside of this class.
  public static interface CompactionHelper {
    Set<StoredTabletFile> selectFiles(SortedMap<StoredTabletFile,DataFileValue> allFiles);

    Set<StoredTabletFile> getFilesToDrop();

    AccumuloConfiguration override(AccumuloConfiguration conf, Set<CompactableFile> files);

  }

  public CompactableImpl(Tablet tablet, CompactionManager manager,
      Map<ExternalCompactionId,ExternalCompactionMetadata> extCompactions) {
    this.tablet = tablet;
    this.manager = manager;

    var dataFileSizes = tablet.getDatafileManager().getDatafileSizes();

    Map<ExternalCompactionId,String> extCompactionsToRemove = new HashMap<>();

    initializeSelection(extCompactions, tablet, extCompactionsToRemove);

    sanityCheckExternalCompactions(extCompactions, dataFileSizes.keySet(), extCompactionsToRemove);

    extCompactionsToRemove.forEach((ecid, reason) -> {
      log.warn("Removing external compaction {} for {} because {} meta: {}", ecid,
          tablet.getExtent(), reason, extCompactions.get(ecid).toJson());
    });

    if (!extCompactionsToRemove.isEmpty()) {
      var tabletMutator = tablet.getContext().getAmple().mutateTablet(tablet.getExtent());
      extCompactionsToRemove.keySet().forEach(tabletMutator::deleteExternalCompaction);
      tabletMutator.mutate();
    }

    extCompactions.forEach((ecid, ecMeta) -> {
      if (!extCompactionsToRemove.containsKey(ecid)) {
        allCompactingFiles.addAll(ecMeta.getJobFiles());
        Collection<CompactableFile> files =
            ecMeta.getJobFiles().stream().map(f -> new CompactableFileImpl(f, dataFileSizes.get(f)))
                .collect(Collectors.toList());
        CompactionJob job = new CompactionJobImpl(ecMeta.getPriority(),
            ecMeta.getCompactionExecutorId(), files, ecMeta.getKind());
        runnningJobs.add(job);

        ExternalCompactionInfo ecInfo = new ExternalCompactionInfo();
        ecInfo.job = job;
        ecInfo.meta = ecMeta;
        externalCompactions.put(ecid, ecInfo);

        log.debug("Loaded tablet {} has existing external compaction {} {}", getExtent(), ecid,
            ecMeta);
        manager.registerExternalCompaction(ecid, getExtent(), ecMeta.getCompactionExecutorId());
      }
    });

    compactionRunning = !allCompactingFiles.isEmpty();

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
  }

  private void sanityCheckExternalCompactions(
      Map<ExternalCompactionId,ExternalCompactionMetadata> extCompactions,
      Set<StoredTabletFile> tabletFiles, Map<ExternalCompactionId,String> extCompactionsToRemove) {

    Set<StoredTabletFile> seen = new HashSet<>();
    AtomicBoolean overlap = new AtomicBoolean(false);

    extCompactions.forEach((ecid, ecMeta) -> {
      if (!tabletFiles.containsAll(ecMeta.getJobFiles())) {
        extCompactionsToRemove.putIfAbsent(ecid, "Has files outside of tablet files");
      } else if (!Collections.disjoint(seen, ecMeta.getJobFiles())) {
        overlap.set(true);
      }
    });

    if (overlap.get()) {
      extCompactions.keySet().forEach(ecid -> {
        extCompactionsToRemove.putIfAbsent(ecid, "Some external compaction files overlap");
      });
    }

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

  private void initializeSelection(
      Map<ExternalCompactionId,ExternalCompactionMetadata> extCompactions, Tablet tablet,
      Map<ExternalCompactionId,String> externalCompactionsToRemove) {
    CompactionKind extKind = null;
    boolean unexpectedExternal = false;
    Set<StoredTabletFile> tmpSelectedFiles = null;
    Boolean selAll = null;
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

      if (selAll == null) {
        selAll = ecMeta.isSelectedAll();
      } else if (selAll != ecMeta.isSelectedAll()) {
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
        propDel = ecMeta.isPropogateDeletes();
      } else if (propDel != ecMeta.isPropogateDeletes()) {
        unexpectedExternal = true;
        reasons.add("Disagreement on propogateDeletes");
        break;
      }

    }

    if (propDel != null && !propDel && count > 1) {
      unexpectedExternal = true;
      reasons.add("Concurrent compactions not propogatingDeletes");
    }

    Pair<Long,CompactionConfig> idAndCfg = null;
    if (extKind != null && extKind == CompactionKind.USER) {
      try {
        idAndCfg = tablet.getCompactionID();
        if (!idAndCfg.getFirst().equals(cid)) {
          unexpectedExternal = true;
          reasons.add("Compaction id mismatch with zookeeper");
        }
      } catch (NoNodeException e) {
        unexpectedExternal = true;
        reasons.add("No compaction id in zookeeper");
      }
    }

    if (unexpectedExternal) {
      String reason = reasons.toString();
      extCompactions.entrySet().stream().filter(e -> {
        var kind = e.getValue().getKind();
        return kind == CompactionKind.SELECTOR || kind == CompactionKind.USER;
      }).map(Entry::getKey).forEach(ecid -> externalCompactionsToRemove.putIfAbsent(ecid, reason));
      return;
    }

    if (extKind != null) {

      if (extKind == CompactionKind.USER) {
        this.chelper = CompactableUtils.getHelper(extKind, tablet, cid, idAndCfg.getSecond());
        this.compactionConfig = idAndCfg.getSecond();
        this.compactionId = cid;
      } else if (extKind == CompactionKind.SELECTOR) {
        this.chelper = CompactableUtils.getHelper(extKind, tablet, null, null);
      }

      this.selectedFiles.clear();
      this.selectedFiles.addAll(tmpSelectedFiles);
      this.selectKind = extKind;
      this.selectedAll = selAll;
      this.selectStatus = SpecialStatus.SELECTED;

      log.debug("Selected compaction status initialized from external compactions {} {} {} {}",
          getExtent(), selectStatus, selectedAll, asFileNames(selectedFiles));
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

    servicesUsed.add(service);

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
                return Optional
                    .of(new Compactable.Files(files, candidates, runningJobsCopy, hints));
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

    public boolean isCompactionEnabled() {
      return memoizedCheck.get();
    }
  }

  private static class CompactionInfo {
    Set<StoredTabletFile> jobFiles;
    Long checkCompactionId = null;
    boolean propogateDeletes = true;
    CompactionHelper localHelper;
    List<IteratorSetting> iters = List.of();
    CompactionConfig localCompactionCfg;
    boolean selectedAll;
    Set<StoredTabletFile> selectedFiles;
  }

  private CompactionInfo reserveFilesForCompaction(CompactionServiceId service, CompactionJob job) {
    CompactionInfo cInfo = new CompactionInfo();

    cInfo.jobFiles = job.getFiles().stream()
        .map(cf -> ((CompactableFileImpl) cf).getStoredTabletFile()).collect(Collectors.toSet());

    if (job.getKind() == CompactionKind.USER)
      checkIfUserCompactionCanceled();

    synchronized (this) {
      if (closed)
        return null;

      if (!service.equals(getConfiguredService(job.getKind())))
        return null;

      switch (selectStatus) {
        case NEW:
        case SELECTING:
          log.trace(
              "Ignoring compaction because files are being selected for user compaction {} {}",
              getExtent(), job);
          return null;
        case SELECTED: {
          if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
            if (selectKind == job.getKind()) {
              if (!selectedFiles.containsAll(cInfo.jobFiles)) {
                log.error("Ignoring {} compaction that does not contain selected files {} {} {}",
                    job.getKind(), getExtent(), asFileNames(selectedFiles),
                    asFileNames(cInfo.jobFiles));
                return null;
              }
            } else {
              log.trace("Ingoring {} compaction because not selected kind {}", job.getKind(),
                  getExtent());
              return null;
            }
          } else if (!Collections.disjoint(selectedFiles, cInfo.jobFiles)) {
            log.trace("Ingoring compaction that overlaps with selected files {} {} {}", getExtent(),
                job.getKind(), asFileNames(Sets.intersection(selectedFiles, cInfo.jobFiles)));
            return null;
          }
          break;
        }
        case CANCELED:
        case NOT_ACTIVE: {
          if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
            log.trace("Ignoring {} compaction because selectStatus is {} for {}", job.getKind(),
                selectStatus, getExtent());
            return null;
          }
          break;
        }
        default:
          throw new AssertionError();
      }

      if (Collections.disjoint(allCompactingFiles, cInfo.jobFiles)) {
        allCompactingFiles.addAll(cInfo.jobFiles);
        runnningJobs.add(job);
      } else {
        return null;
      }

      compactionRunning = !allCompactingFiles.isEmpty();

      switch (job.getKind()) {
        case SELECTOR:
        case USER:
          Preconditions.checkState(selectStatus == SpecialStatus.SELECTED);
          if (job.getKind() == selectKind && selectedAll
              && cInfo.jobFiles.containsAll(selectedFiles)) {
            cInfo.propogateDeletes = false;
          }

          cInfo.selectedFiles = Set.copyOf(selectedFiles);
          cInfo.selectedAll = selectedAll;

          break;
        default:
          if (((CompactionJobImpl) job).selectedAll()) {
            // At the time when the job was created all files were selected, so deletes can be
            // dropped.
            cInfo.propogateDeletes = false;
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

    return cInfo;
  }

  private void completeCompaction(CompactionJob job, Set<StoredTabletFile> jobFiles,
      StoredTabletFile metaFile) {
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

  @Override
  public void compact(CompactionServiceId service, CompactionJob job, RateLimiter readLimiter,
      RateLimiter writeLimiter, long queuedTime) {

    CompactionInfo cInfo = reserveFilesForCompaction(service, job);
    if (cInfo == null)
      return;

    StoredTabletFile metaFile = null;
    long startTime = System.currentTimeMillis();
    // create an empty stats object to be populated by CompactableUtils.compact()
    CompactionStats stats = new CompactionStats();
    try {

      TabletLogger.compacting(getExtent(), job, cInfo.localCompactionCfg);
      tablet.incrementStatusMajor();

      metaFile = CompactableUtils.compact(tablet, job, cInfo.jobFiles, cInfo.checkCompactionId,
          cInfo.selectedFiles, cInfo.propogateDeletes, cInfo.localHelper, cInfo.iters,
          new CompactionCheck(service, job.getKind(), cInfo.checkCompactionId), readLimiter,
          writeLimiter, stats);

      TabletLogger.compacted(getExtent(), job, metaFile);

    } catch (CompactionCanceledException cce) {
      log.debug("Compaction canceled {} ", getExtent());
      metaFile = null;
    } catch (Exception e) {
      metaFile = null;
      throw new RuntimeException(e);
    } finally {
      completeCompaction(job, cInfo.jobFiles, metaFile);
      tablet.updateTimer(MAJOR, queuedTime, startTime, stats.getEntriesRead(), metaFile == null);
    }
  }

  @Override
  public ExternalCompactionJob reserveExternalCompaction(CompactionServiceId service,
      CompactionJob job, String compactorId, ExternalCompactionId externalCompactionId) {

    Preconditions.checkState(!tablet.getExtent().isMeta());

    CompactionInfo cInfo = reserveFilesForCompaction(service, job);
    if (cInfo == null)
      return null;
    try {
      AccumuloConfiguration compactionConfig = CompactableUtils.getCompactionConfig(job.getKind(),
          tablet, cInfo.localHelper, job.getFiles());
      Map<String,String> tableCompactionProperties = new HashMap<>();
      Map<String,String> defaultsProps = new HashMap<>();
      DefaultConfiguration.getInstance().forEach(e -> defaultsProps.put(e.getKey(), e.getValue()));
      compactionConfig.forEach(entry -> {
        var k = entry.getKey();
        var v = entry.getValue();
        if (k.startsWith(Property.TABLE_PREFIX.getKey())
            && !defaultsProps.getOrDefault(k, "").equals(v)) {
          tableCompactionProperties.put(k, v);
        }
      });

      var newFile = tablet.getNextMapFilename(!cInfo.propogateDeletes ? "A" : "C");
      var compactTmpName = new TabletFile(new Path(newFile.getMetaInsert() + "_tmp"));

      ExternalCompactionInfo ecInfo = new ExternalCompactionInfo();

      ecInfo.meta = new ExternalCompactionMetadata(cInfo.jobFiles,
          Sets.difference(cInfo.selectedFiles, cInfo.jobFiles), compactTmpName, newFile,
          compactorId, job.getKind(), job.getPriority(), job.getExecutor(), cInfo.propogateDeletes,
          cInfo.selectedAll, cInfo.checkCompactionId);

      tablet.getContext().getAmple().mutateTablet(getExtent())
          .putExternalCompaction(externalCompactionId, ecInfo.meta).mutate();

      ecInfo.job = job;

      externalCompactions.put(externalCompactionId, ecInfo);

      SortedMap<StoredTabletFile,DataFileValue> allFiles = tablet.getDatafiles();
      HashMap<StoredTabletFile,DataFileValue> compactFiles = new HashMap<>();
      cInfo.jobFiles.forEach(file -> compactFiles.put(file, allFiles.get(file)));

      return new ExternalCompactionJob(compactFiles, cInfo.propogateDeletes, compactTmpName,
          getExtent(), externalCompactionId, job.getKind(), cInfo.iters, cInfo.checkCompactionId,
          tableCompactionProperties);

    } catch (Exception e) {
      externalCompactions.remove(externalCompactionId);
      completeCompaction(job, cInfo.jobFiles, null);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void commitExternalCompaction(ExternalCompactionId extCompactionId, long fileSize,
      long entries) {

    synchronized (this) {
      if (closed)
        return;

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
        StoredTabletFile metaFile = null;
        try {
          // possibly do some sanity checks here
          metaFile = tablet.getDatafileManager().bringMajorCompactionOnline(
              ecInfo.meta.getJobFiles(), ecInfo.meta.getCompactTmpName(), ecInfo.meta.getNewFile(),
              ecInfo.meta.getCompactionId(),
              Sets.union(ecInfo.meta.getJobFiles(), ecInfo.meta.getNextFiles()),
              new DataFileValue(fileSize, entries), Optional.of(extCompactionId));
          TabletLogger.compacted(getExtent(), ecInfo.job, metaFile);
        } catch (Exception e) {
          metaFile = null;
          log.error("Error committing external compaction {}", extCompactionId, e);
          throw new RuntimeException(e);
        } finally {
          completeCompaction(ecInfo.job, ecInfo.meta.getJobFiles(), metaFile);
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
        Preconditions.checkState(!closed);
        notifyAll();
      }
    }
  }

  @Override
  public void externalCompactionFailed(ExternalCompactionId ecid) {

    synchronized (this) {
      if (closed)
        return;

      if (!externalCompactionsCommitting.add(ecid)) {
        return;
      }
    }
    try {

      ExternalCompactionInfo ecInfo = externalCompactions.get(ecid);

      if (ecInfo != null) {
        tablet.getContext().getAmple().mutateTablet(getExtent()).deleteExternalCompaction(ecid)
            .mutate();
        completeCompaction(ecInfo.job, ecInfo.meta.getJobFiles(), null);
        externalCompactions.remove(ecid);
        log.debug("Processed external compaction failure {}", ecid);
      } else {
        log.debug("Ignoring request to fail external compaction that is unknown {}", ecid);
      }

      tablet.getContext().getAmple().deleteExternalCompactionFinalStates(List.of(ecid));
    } finally {
      synchronized (this) {
        Preconditions.checkState(externalCompactionsCommitting.remove(ecid));
        Preconditions.checkState(!closed);
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

      var dispatch = dispatcher.dispatch(new DispatchParameters() {

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

      return dispatch.getService();
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
    synchronized (this) {
      if (closed)
        return;

      closed = true;

      // wait while internal jobs are running or external compactions are committing, but do not
      // wait on external compactions that are running
      while (runnningJobs.stream()
          .anyMatch(job -> !((CompactionExecutorIdImpl) job.getExecutor()).isExternalId())
          || !externalCompactionsCommitting.isEmpty()) {
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

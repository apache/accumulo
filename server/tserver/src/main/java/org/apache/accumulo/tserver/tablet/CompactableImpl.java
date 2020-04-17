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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher.DispatchParameters;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionService;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.mastermessage.TabletStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
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

  // TODO would be better if this set were persistent
  private Set<StoredTabletFile> selectedFiles = new HashSet<>();

  private Set<StoredTabletFile> choppingFiles = new HashSet<>();

  // track files produced by compactions of this tablet, those are considered chopped
  private Set<StoredTabletFile> choppedFiles = new HashSet<>();
  private SpecialStatus chopStatus = SpecialStatus.NOT_ACTIVE;

  // status of special compactions
  private enum SpecialStatus {
    NEW, SELECTING, SELECTED, NOT_ACTIVE
  }

  private Deriver<CompactionDispatcher> dispactDeriver;

  private SpecialStatus selectStatus = SpecialStatus.NOT_ACTIVE;
  private CompactionKind selectKind = null;
  private boolean selectedAll = false;
  private CompactionHelper chelper = null;
  private Long compactionId;
  private CompactionConfig compactionConfig;
  private volatile Consumer<Compactable> newFileCallback;

  // This interface exists for two purposes. First it allows abstraction of new and old
  // implementations for user pluggable file selection code. Second it facilitates placing code
  // outside of this class.
  public static interface CompactionHelper {
    Set<StoredTabletFile> selectFiles(SortedMap<StoredTabletFile,DataFileValue> allFiles);

    Set<StoredTabletFile> getFilesToDrop();

    AccumuloConfiguration override(AccumuloConfiguration conf, Set<CompactableFile> files);

  }

  public CompactableImpl(Tablet tablet) {
    this.tablet = tablet;
    this.dispactDeriver = CompactableUtils.createDispatcher(tablet);
  }

  void initiateChop() {
    Set<StoredTabletFile> chopCandidates = new HashSet<>();
    synchronized (this) {
      if (chopStatus == SpecialStatus.NOT_ACTIVE) {
        // TODO may want to do nothing instead of throw exception
        Preconditions.checkState(selectStatus == SpecialStatus.NOT_ACTIVE);
        chopStatus = SpecialStatus.SELECTING;
        choppingFiles.clear();

        chopCandidates.addAll(tablet.getDatafiles().keySet());
        // any files currently compacting will be chopped
        chopCandidates.removeAll(allCompactingFiles);
        chopCandidates.removeAll(choppedFiles);

      } else {
        // TODO
        return;
      }
    }

    Set<StoredTabletFile> chopSelections = selectChopFiles(chopCandidates);
    if (chopSelections.isEmpty()) {
      markChopped();
    }

    synchronized (this) {
      Preconditions.checkState(chopStatus == SpecialStatus.SELECTING);
      if (chopSelections.isEmpty()) {
        chopStatus = SpecialStatus.NOT_ACTIVE;
      } else {
        chopStatus = SpecialStatus.SELECTED;
        choppingFiles.addAll(chopSelections);
      }

      // any candidates that were analyzed and found not needing a chop can be considered chopped
      choppedFiles.addAll(Sets.difference(chopCandidates, chopSelections));
    }

  }

  /**
   * Tablet can use this to signal files were added.
   */
  void filesAdded() {
    if (newFileCallback != null)
      newFileCallback.accept(this);
  }

  @Override
  public void registerNewFilesCallback(Consumer<Compactable> callback) {
    this.newFileCallback = callback;

  }

  private void markChopped() {
    // TODO work into compaction mutation
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

  private void chopCompactionCompleted(Set<StoredTabletFile> jobFiles) {
    boolean markChopped = false;
    synchronized (this) {
      Preconditions.checkState(chopStatus == SpecialStatus.SELECTED);
      Preconditions.checkState(choppingFiles.containsAll(jobFiles));

      // TODO need to consider compacting files

      choppingFiles.removeAll(jobFiles);

      if (choppingFiles.isEmpty()) {
        chopStatus = SpecialStatus.NOT_ACTIVE;
        markChopped = true;
      }
    }

    if (markChopped)
      markChopped();
  }

  /**
   * Tablet calls this signal a user compaction should run
   */
  void initiateUserCompaction(long compactionId, CompactionConfig compactionConfig) {
    initiateSelection(CompactionKind.USER, compactionId, compactionConfig);
  }

  private void initiateSelection(CompactionKind kind) {
    if (kind != CompactionKind.SELECTOR)
      return;

    initiateSelection(CompactionKind.SELECTOR, null, null);
  }

  private void initiateSelection(CompactionKind kind, Long compactionId,
      CompactionConfig compactionConfig) {
    Preconditions.checkArgument(kind == CompactionKind.USER || kind == CompactionKind.SELECTOR);

    // TODO optimize this
    var localHelper = CompactableUtils.getHelper(kind, tablet, compactionId, compactionConfig);

    if (localHelper == null)
      return;

    synchronized (this) {
      if (selectStatus == SpecialStatus.NOT_ACTIVE || (kind == CompactionKind.USER
          && selectKind == CompactionKind.SELECTOR
          && runnningJobs.stream().noneMatch(job -> job.getKind() == CompactionKind.SELECTOR))) {
        // TODO check chop status
        selectStatus = SpecialStatus.NEW;
        selectKind = kind;
        selectedFiles.clear();
        selectedAll = false;
        this.chelper = localHelper;
        this.compactionId = compactionId;
        this.compactionConfig = compactionConfig;
        // TODO config
        log.debug("Selected compaction status changed {} {}", getExtent(), selectStatus);
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
        log.debug("Selected compaction status changed {} {}", getExtent(), selectStatus);
      } else {
        return;
      }
    }

    try {
      var allFiles = tablet.getDatafiles();
      Set<StoredTabletFile> selectingFiles = localHelper.selectFiles(allFiles);

      if (selectingFiles.isEmpty()) {
        synchronized (this) {
          selectStatus = SpecialStatus.NOT_ACTIVE;
          log.debug("Selected compaction status changed {} {}", getExtent(), selectStatus);
        }
      } else {
        var allSelected =
            allFiles.keySet().equals(Sets.union(selectingFiles, localHelper.getFilesToDrop()));
        synchronized (this) {
          selectStatus = SpecialStatus.SELECTED;
          selectedFiles.addAll(selectingFiles);
          selectedAll = allSelected;
          log.debug("Selected compaction status changed {} {} {} {}", getExtent(), selectStatus,
              selectedAll, asFileNames(selectedFiles));
        }

        // TODO notify compaction manager to process this tablet!
      }

    } catch (Exception e) {
      synchronized (this) {
        selectStatus = SpecialStatus.NEW;
        log.error("Failed to select user compaction files {}", getExtent(), e);
        log.debug("Selected compaction status changed {} {}", getExtent(), selectStatus);
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
    Preconditions.checkState(selectStatus == SpecialStatus.SELECTED && selectKind == job.getKind());

    selectedFiles.removeAll(jobFiles);

    if (selectedFiles.isEmpty()) {
      selectStatus = SpecialStatus.NOT_ACTIVE;
      log.debug("Selected compaction status changed {} {}", getExtent(), selectStatus);
    } else {
      selectedFiles.add(newFile);
      log.debug("Compacted subset of selected files {} {} -> {}", getExtent(),
          asFileNames(jobFiles), newFile.getFileName());
    }
  }

  @Override
  public TableId getTableId() {
    return getExtent().getTableId();
  }

  @Override
  public KeyExtent getExtent() {
    return tablet.getExtent();
  }

  @Override
  public Optional<Files> getFiles(CompactionServiceId service, CompactionKind kind) {
    // TODO not consistently obtaing tablet state

    if (tablet.isClosing() || tablet.isClosed() || !service.equals(getConfiguredService(kind)))
      return Optional.empty();

    var files = tablet.getDatafiles();

    // very important to call following outside of lock
    initiateSelection(kind);

    synchronized (this) {

      if (!files.keySet().containsAll(allCompactingFiles)) {
        log.debug("Ignoring because compacting not a subset {}", getExtent());

        // A compaction finished, so things are out of date. This can happen because this class and
        // tablet have separate locks, its ok.
        return Optional.of(new Compactable.Files(files, kind, Set.of(), Set.of()));
      }

      var allCompactingCopy = Set.copyOf(allCompactingFiles);
      var runningJobsCopy = Set.copyOf(runnningJobs);

      switch (kind) {
        case SYSTEM: {
          if (tablet.getTableConfiguration().isPropertySet(Property.TABLE_COMPACTION_STRATEGY,
              true))
            return Optional.of(new Compactable.Files(files, kind, Set.of(), runningJobsCopy));

          switch (selectStatus) {
            case NOT_ACTIVE:
              return Optional.of(new Compactable.Files(files, kind,
                  Sets.difference(files.keySet(), allCompactingCopy), runningJobsCopy));
            case NEW:
            case SELECTING:
              return Optional.of(new Compactable.Files(files, kind, Set.of(), runningJobsCopy));
            case SELECTED: {
              Set<StoredTabletFile> candidates = new HashSet<>(files.keySet());
              candidates.removeAll(allCompactingCopy);
              candidates.removeAll(selectedFiles);
              candidates = Collections.unmodifiableSet(candidates);
              return Optional.of(new Compactable.Files(files, kind, candidates, runningJobsCopy));
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
              return Optional.of(new Compactable.Files(files, kind, Set.of(), runningJobsCopy));
            case SELECTED: {
              if (selectKind == kind) {
                Set<StoredTabletFile> candidates = new HashSet<>(selectedFiles);
                candidates.removeAll(allCompactingFiles);
                candidates = Collections.unmodifiableSet(candidates);
                Preconditions.checkState(files.keySet().containsAll(candidates),
                    "selected files not in all files %s %s", candidates, files.keySet());
                return Optional.of(
                    new Compactable.Files(files, kind, Set.copyOf(selectedFiles), runningJobsCopy));
              } else {
                return Optional.of(new Compactable.Files(files, kind, Set.of(), runningJobsCopy));
              }
            }
            default:
              throw new AssertionError();
          }
        case CHOP: // TODO analyze how this interacts with selected compactions
          switch (chopStatus) {
            case NOT_ACTIVE:
            case NEW:
            case SELECTING:
              return Optional.of(new Compactable.Files(files, kind, Set.of(), runningJobsCopy));
            case SELECTED: // TODO remove compacting files?
              return Optional.of(
                  new Compactable.Files(files, kind, Set.copyOf(choppingFiles), runningJobsCopy));
          }
        default:
          throw new AssertionError();
      }
    }
  }

  @Override
  public void compact(CompactionServiceId service, CompactionJob job) {

    Set<StoredTabletFile> jobFiles = job.getFiles().stream()
        .map(cf -> ((CompactableFileImpl) cf).getStortedTabletFile()).collect(Collectors.toSet());

    Long compactionId = null;
    boolean propogateDeletesForSelected = true;
    CompactionHelper localHelper;
    List<IteratorSetting> iters = List.of();

    synchronized (this) {
      if (!service.equals(getConfiguredService(job.getKind())))
        return;

      // TODO check chop status??

      switch (selectStatus) {
        case NEW:
        case SELECTING:
          log.debug(
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
              log.debug("Ingoring {} compaction because not selected kind {}", job.getKind(),
                  getExtent());
              return;
            }
          } else if (!Collections.disjoint(selectedFiles, jobFiles)) {
            log.debug("Ingoring compaction that overlaps with selected files {} {} {}", getExtent(),
                job.getKind(), asFileNames(Sets.intersection(selectedFiles, jobFiles)));
            return;
          }
          break;
        }
        case NOT_ACTIVE: {
          if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
            log.debug("Ingoring {} compaction because nothing selected {}", job.getKind(),
                getExtent());
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
        return; // TODO log an error?
      }

      compactionRunning = !allCompactingFiles.isEmpty();

      if (job.getKind() == selectKind && selectedAll && jobFiles.containsAll(selectedFiles)) {
        propogateDeletesForSelected = false;
      }

      if (job.getKind() == CompactionKind.USER && selectKind == job.getKind()
          && selectedFiles.equals(jobFiles)) {
        compactionId = this.compactionId;
      }

      if (job.getKind() == CompactionKind.USER) {
        iters = compactionConfig.getIterators();
      }

      localHelper = this.chelper;
    }
    // TODO only add if not in set!
    StoredTabletFile metaFile = null;
    try {
      metaFile = CompactableUtils.compact(tablet, job, jobFiles, compactionId,
          propogateDeletesForSelected, localHelper, iters);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      synchronized (this) {
        allCompactingFiles.removeAll(jobFiles);
        runnningJobs.remove(job); // TODO check return true?
        compactionRunning = !allCompactingFiles.isEmpty();

        // TODO this tracking feels a bit iffy
        if (metaFile != null) {
          choppedFiles.add(metaFile);
          choppedFiles.removeAll(jobFiles);
        }
      }

      if ((job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR)
          && metaFile != null)
        selectedCompactionCompleted(job, jobFiles, metaFile);// TODO what if it failed?
      else if (job.getKind() == CompactionKind.CHOP)
        chopCompactionCompleted(jobFiles);
      else
        selectFiles();
    }
  }

  @Override
  public CompactionServiceId getConfiguredService(CompactionKind kind) {

    var dispatcher = dispactDeriver.derive();

    Map<String,String> tmpHints = Map.of();

    if (kind == CompactionKind.USER) {
      synchronized (this) {
        if (selectStatus != SpecialStatus.NOT_ACTIVE && selectKind == CompactionKind.USER) {
          tmpHints = compactionConfig.getExecutionHints();
        } else {
          // TODO is this expected??
        }
      }
    }

    var hints = tmpHints;

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
      public Map<CompactionServiceId,CompactionService> getCompactionServices() {
        // TODO
        return Map.of();
      }
    });

    // TODO
    return directives.getService();
  }

  @Override
  public double getCompactionRatio() {
    return tablet.getTableConfiguration().getFraction(Property.TABLE_MAJC_RATIO);
  }

  public boolean isMajorCompactionRunning() {
    // this method intentionally not synchronized because its called by stats code.
    return compactionRunning;
  }
}

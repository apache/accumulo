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

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.ManagerMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

class DatafileManager {
  private final Logger log = LoggerFactory.getLogger(DatafileManager.class);
  // access to datafilesizes needs to be synchronized: see CompactionRunner#getNumFiles
  private final Map<StoredTabletFile,DataFileValue> datafileSizes =
      Collections.synchronizedMap(new TreeMap<>());
  private final Tablet tablet;

  // ensure we only have one reader/writer of our bulk file notes at a time
  private final Object bulkFileImportLock = new Object();

  // This must be incremented before and after datafileSizes and metadata table updates. These
  // counts allow detection of overlapping operations w/o placing a lock around metadata table
  // updates and datafileSizes updates. There is a periodic metadata consistency check that runs in
  // the tablet server against all tablets. This check compares what a tablet object has in memory
  // to what is in the metadata table to ensure they are in agreement. Inorder to avoid false
  // positives, when this consistency check runs its needs to know if it overlaps in time with any
  // metadata updates made by the tablet. The consistency check uses these counts to know that.
  private final AtomicReference<MetadataUpdateCount> metadataUpdateCount;

  DatafileManager(Tablet tablet, SortedMap<StoredTabletFile,DataFileValue> datafileSizes) {
    this.datafileSizes.putAll(datafileSizes);
    this.tablet = tablet;
    this.metadataUpdateCount =
        new AtomicReference<>(new MetadataUpdateCount(tablet.getExtent(), 0L, 0L));
  }

  private final Set<TabletFile> filesToDeleteAfterScan = new HashSet<>();
  private final Map<Long,Set<StoredTabletFile>> scanFileReservations = new HashMap<>();
  private final MapCounter<StoredTabletFile> fileScanReferenceCounts = new MapCounter<>();
  private long nextScanReservationId = 0;

  static void rename(VolumeManager fs, Path src, Path dst) throws IOException {
    if (!fs.rename(src, dst)) {
      throw new IOException("Rename " + src + " to " + dst + " returned false ");
    }
  }

  Pair<Long,Map<TabletFile,DataFileValue>> reserveFilesForScan() {
    synchronized (tablet) {

      Set<StoredTabletFile> absFilePaths = new HashSet<>(datafileSizes.keySet());

      long rid = nextScanReservationId++;

      scanFileReservations.put(rid, absFilePaths);

      Map<TabletFile,DataFileValue> ret = new HashMap<>();

      for (StoredTabletFile path : absFilePaths) {
        fileScanReferenceCounts.increment(path, 1);
        ret.put(path, datafileSizes.get(path));
      }

      return new Pair<>(rid, ret);
    }
  }

  void returnFilesForScan(Long reservationId) {

    final Set<StoredTabletFile> filesToDelete = new HashSet<>();

    synchronized (tablet) {
      Set<StoredTabletFile> absFilePaths = scanFileReservations.remove(reservationId);

      if (absFilePaths == null) {
        throw new IllegalArgumentException("Unknown scan reservation id " + reservationId);
      }

      boolean notify = false;
      for (StoredTabletFile path : absFilePaths) {
        long refCount = fileScanReferenceCounts.decrement(path, 1);
        if (refCount == 0) {
          if (filesToDeleteAfterScan.remove(path)) {
            filesToDelete.add(path);
          }
          notify = true;
        } else if (refCount < 0) {
          throw new IllegalStateException("Scan ref count for " + path + " is " + refCount);
        }
      }

      if (notify) {
        tablet.notifyAll();
      }
    }

    if (!filesToDelete.isEmpty()) {
      log.debug("Removing scan refs from metadata {} {}", tablet.getExtent(), filesToDelete);
      MetadataTableUtil.removeScanFiles(tablet.getExtent(), filesToDelete, tablet.getContext(),
          tablet.getTabletServer().getLock());
    }
  }

  void removeFilesAfterScan(Set<StoredTabletFile> scanFiles) {
    if (scanFiles.isEmpty()) {
      return;
    }

    Set<StoredTabletFile> filesToDelete = new HashSet<>();

    synchronized (tablet) {
      for (StoredTabletFile path : scanFiles) {
        if (fileScanReferenceCounts.get(path) == 0) {
          filesToDelete.add(path);
        } else {
          filesToDeleteAfterScan.add(path);
        }
      }
    }

    if (!filesToDelete.isEmpty()) {
      log.debug("Removing scan refs from metadata {} {}", tablet.getExtent(), filesToDelete);
      MetadataTableUtil.removeScanFiles(tablet.getExtent(), filesToDelete, tablet.getContext(),
          tablet.getTabletServer().getLock());
    }
  }

  private TreeSet<StoredTabletFile> waitForScansToFinish(Set<StoredTabletFile> pathsToWaitFor) {
    long maxWait = 10000L;
    long startTime = System.currentTimeMillis();
    TreeSet<StoredTabletFile> inUse = new TreeSet<>();

    Span span = TraceUtil.startSpan(this.getClass(), "waitForScans");
    try (Scope scope = span.makeCurrent()) {
      synchronized (tablet) {
        for (StoredTabletFile path : pathsToWaitFor) {
          while (fileScanReferenceCounts.get(path) > 0
              && System.currentTimeMillis() - startTime < maxWait) {
            try {
              tablet.wait(100);
            } catch (InterruptedException e) {
              log.warn("{}", e.getMessage(), e);
            }
          }
        }

        for (StoredTabletFile path : pathsToWaitFor) {
          if (fileScanReferenceCounts.get(path) > 0) {
            inUse.add(path);
          }
        }
      }
    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
    return inUse;
  }

  public Collection<StoredTabletFile> importMapFiles(long tid, Map<TabletFile,DataFileValue> paths,
      boolean setTime) throws IOException {

    String bulkDir = null;
    // once tablet files are inserted into the metadata they will become StoredTabletFiles
    Map<StoredTabletFile,DataFileValue> newFiles = new HashMap<>(paths.size());

    for (TabletFile tpath : paths.keySet()) {
      boolean inTheRightDirectory = false;
      Path parent = tpath.getPath().getParent().getParent();
      for (String tablesDir : tablet.getContext().getTablesDirs()) {
        if (parent.equals(new Path(tablesDir, tablet.getExtent().tableId().canonical()))) {
          inTheRightDirectory = true;
          break;
        }
      }
      if (!inTheRightDirectory) {
        throw new IOException("Data file " + tpath + " not in table dirs");
      }

      if (bulkDir == null) {
        bulkDir = tpath.getTabletDir();
      } else if (!bulkDir.equals(tpath.getTabletDir())) {
        throw new IllegalArgumentException("bulk files in different dirs " + bulkDir + " " + tpath);
      }

    }

    if (tablet.getExtent().isMeta()) {
      throw new IllegalArgumentException("Can not import files to a metadata tablet");
    }
    // increment start count before metadata update AND updating in memory map of files
    metadataUpdateCount.updateAndGet(MetadataUpdateCount::incrementStart);
    // do not place any code here between above stmt and try{}finally
    try {
      synchronized (bulkFileImportLock) {

        if (!paths.isEmpty()) {
          long bulkTime = Long.MIN_VALUE;
          if (setTime) {
            for (DataFileValue dfv : paths.values()) {
              long nextTime = tablet.getAndUpdateTime();
              if (nextTime < bulkTime) {
                throw new IllegalStateException(
                    "Time went backwards unexpectedly " + nextTime + " " + bulkTime);
              }
              bulkTime = nextTime;
              dfv.setTime(bulkTime);
            }
          }

          newFiles = tablet.updatePersistedTime(bulkTime, paths, tid);
        }
      }

      synchronized (tablet) {
        for (Entry<StoredTabletFile,DataFileValue> tpath : newFiles.entrySet()) {
          if (datafileSizes.containsKey(tpath.getKey())) {
            log.error("Adding file that is already in set {}", tpath.getKey());
          }
          datafileSizes.put(tpath.getKey(), tpath.getValue());
        }

        tablet.getTabletResources().importedMapFiles();

        tablet.computeNumEntries();
      }

      for (Entry<StoredTabletFile,DataFileValue> entry : newFiles.entrySet()) {
        TabletLogger.bulkImported(tablet.getExtent(), entry.getKey());
      }
    } catch (Exception e) {
      // Any exception in this code is prone to leaving the persisted tablet metadata and the
      // tablets in memory data structs out of sync. Log the extent and exact files involved as this
      // may be useful for debugging.
      log.error("Failure adding bulk import files {} {}", tablet.getExtent(), paths.keySet(), e);
      throw e;
    } finally {
      // increment finish count after metadata update AND updating in memory map of files
      metadataUpdateCount.updateAndGet(MetadataUpdateCount::incrementFinish);
    }

    return newFiles.keySet();
  }

  /**
   * Returns Optional of the new file created. It is possible that the file was just flushed with no
   * entries so was not inserted into the metadata. In this case empty is returned. If the file was
   * stored in the metadata table, then StoredTableFile will be returned.
   */
  Optional<StoredTabletFile> bringMinorCompactionOnline(TabletFile tmpDatafile,
      TabletFile newDatafile, DataFileValue dfv, CommitSession commitSession, long flushId) {
    Optional<StoredTabletFile> newFile;
    // rename before putting in metadata table, so files in metadata table should
    // always exist
    boolean attemptedRename = false;
    VolumeManager vm = tablet.getTabletServer().getContext().getVolumeManager();
    do {
      try {
        if (dfv.getNumEntries() == 0) {
          log.debug("No data entries so delete temporary file {}", tmpDatafile);
          vm.deleteRecursively(tmpDatafile.getPath());
        } else {
          if (!attemptedRename && vm.exists(newDatafile.getPath())) {
            log.warn("Target map file already exist {}", newDatafile);
            throw new RuntimeException("File unexpectedly exists " + newDatafile.getPath());
          }
          // the following checks for spurious rename failures that succeeded but gave an IoE
          if (attemptedRename && vm.exists(newDatafile.getPath())
              && !vm.exists(tmpDatafile.getPath())) {
            // seems like previous rename succeeded, so break
            break;
          }
          attemptedRename = true;
          rename(vm, tmpDatafile.getPath(), newDatafile.getPath());
        }
        break;
      } catch (IOException ioe) {
        log.warn("Tablet " + tablet.getExtent() + " failed to rename " + newDatafile
            + " after MinC, will retry in 60 secs...", ioe);
        sleepUninterruptibly(1, TimeUnit.MINUTES);
      }
    } while (true);

    long t1, t2;

    // increment start count before metadata update AND updating in memory map of files
    metadataUpdateCount.updateAndGet(MetadataUpdateCount::incrementStart);
    // do not place any code here between above stmt and following try{}finally
    try {
      // Should not hold the tablet lock while trying to acquire the log lock because this could
      // lead to deadlock. However there is a path in the code that does this. See #3759
      tablet.getLogLock().lock();
      // do not place any code here between lock and try
      try {
        // The following call pairs with tablet.finishClearingUnusedLogs() later in this block. If
        // moving where the following method is called, examine it and finishClearingUnusedLogs()
        // before moving.
        Set<String> unusedWalLogs = tablet.beginClearingUnusedLogs();

        // the order of writing to metadata and walog is important in the face of machine/process
        // failures need to write to metadata before writing to walog, when things are done in the
        // reverse order data could be lost... the minor compaction start even should be written
        // before the following metadata write is made
        newFile = tablet.updateTabletDataFile(commitSession.getMaxCommittedTime(), newDatafile, dfv,
            unusedWalLogs, flushId);

        // Mark that we have data we want to replicate
        // This WAL could still be in use by other Tablets *from the same table*, so we can only
        // mark
        // that there is data to replicate,
        // but it is *not* closed. We know it is not closed by the fact that this MinC triggered. A
        // MinC cannot happen unless the
        // tablet is online and thus these WALs are referenced by that tablet. Therefore, the WAL
        // replication status cannot be 'closed'.
        @SuppressWarnings("deprecation")
        boolean replicate = org.apache.accumulo.core.replication.ReplicationConfigurationUtil
            .isEnabled(tablet.getExtent(), tablet.getTableConfiguration());
        if (replicate) {
          // unusedWalLogs is of the form host/fileURI, need to strip off the host portion
          Set<String> logFileOnly = new HashSet<>();
          for (String unusedWalLog : unusedWalLogs) {
            int index = unusedWalLog.indexOf('/');
            if (index == -1) {
              log.warn(
                  "Could not find host component to strip from DFSLogger representation of WAL");
            } else {
              unusedWalLog = unusedWalLog.substring(index + 1);
            }
            logFileOnly.add(unusedWalLog);
          }

          if (log.isDebugEnabled()) {
            log.debug("Recording that data has been ingested into {} using {}", tablet.getExtent(),
                logFileOnly);
          }
          for (String logFile : logFileOnly) {
            @SuppressWarnings("deprecation")
            Status status =
                org.apache.accumulo.server.replication.StatusUtil.openWithUnknownLength();
            ReplicationTableUtil.updateFiles(tablet.getContext(), tablet.getExtent(), logFile,
                status);
          }
        }

        tablet.finishClearingUnusedLogs();
      } finally {
        tablet.getLogLock().unlock();
      }

      do {
        try {
          // the purpose of making this update use the new commit session, instead of the old one
          // passed in, is because the new one will reference the logs used by current memory...

          tablet.getTabletServer().minorCompactionFinished(
              tablet.getTabletMemory().getCommitSession(), commitSession.getWALogSeq() + 2);
          break;
        } catch (IOException e) {
          log.error("Failed to write to write-ahead log " + e.getMessage() + " will retry", e);
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      } while (true);

      synchronized (tablet) {
        t1 = System.currentTimeMillis();

        if (newFile.isPresent()) {
          StoredTabletFile newFileStored = newFile.orElseThrow();
          if (datafileSizes.containsKey(newFileStored)) {
            log.error("Adding file that is already in set {}", newFileStored);
          }
          datafileSizes.put(newFileStored, dfv);
        }

        tablet.flushComplete(flushId);

        t2 = System.currentTimeMillis();
      }
    } catch (Exception e) {
      // Any exception in this code is prone to leaving the persisted tablet metadata and the
      // tablets in memory data structs out of sync. Log the extent and exact file involved as this
      // may be useful for debugging.
      log.error("Failure adding minor compacted file {} {}", tablet.getExtent(), newDatafile, e);
      throw e;
    } finally {
      // increment finish count after metadata update AND updating in memory map of files
      metadataUpdateCount.updateAndGet(MetadataUpdateCount::incrementFinish);
    }

    TabletLogger.flushed(tablet.getExtent(), newFile);

    if (log.isTraceEnabled()) {
      log.trace(String.format("MinC finish lock %.2f secs %s", (t2 - t1) / 1000.0,
          tablet.getExtent().toString()));
    }
    long splitSize = tablet.getTableConfiguration().getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    if (dfv.getSize() > splitSize) {
      log.debug(String.format("Minor Compaction wrote out file larger than split threshold."
          + " split threshold = %,d  file size = %,d", splitSize, dfv.getSize()));
    }

    return newFile;
  }

  Optional<StoredTabletFile> bringMajorCompactionOnline(Set<StoredTabletFile> oldDatafiles,
      TabletFile tmpDatafile, Long compactionId, Set<StoredTabletFile> selectedFiles,
      DataFileValue dfv, Optional<ExternalCompactionId> ecid) throws IOException {
    final KeyExtent extent = tablet.getExtent();
    VolumeManager vm = tablet.getTabletServer().getContext().getVolumeManager();
    long t1, t2;

    TabletFile newDatafile = CompactableUtils.computeCompactionFileDest(tmpDatafile);

    if (vm.exists(newDatafile.getPath())) {
      log.error("Target map file already exist " + newDatafile, new Exception());
      throw new IllegalStateException("Target map file already exist " + newDatafile);
    }

    if (dfv.getNumEntries() == 0) {
      vm.deleteRecursively(tmpDatafile.getPath());
    } else {
      // rename before putting in metadata table, so files in metadata table should
      // always exist
      rename(vm, tmpDatafile.getPath(), newDatafile.getPath());
    }

    Location lastLocation = null;
    Optional<StoredTabletFile> newFile;

    if (dfv.getNumEntries() > 0) {
      // calling insert to get the new file before inserting into the metadata
      newFile = Optional.of(newDatafile.insert());
    } else {
      newFile = Optional.empty();
    }
    Long compactionIdToWrite = null;

    // increment start count before metadata update AND updating in memory map of files
    metadataUpdateCount.updateAndGet(MetadataUpdateCount::incrementStart);
    // do not place any code here between above stmt and try{}finally
    try {

      synchronized (tablet) {
        t1 = System.currentTimeMillis();

        Preconditions.checkState(datafileSizes.keySet().containsAll(oldDatafiles),
            "Compacted files %s are not a subset of tablet files %s", oldDatafiles,
            datafileSizes.keySet());
        if (newFile.isPresent()) {
          Preconditions.checkState(!datafileSizes.containsKey(newFile.orElseThrow()),
              "New compaction file %s already exist in tablet files %s", newFile,
              datafileSizes.keySet());
        }

        tablet.incrementDataSourceDeletions();

        datafileSizes.keySet().removeAll(oldDatafiles);

        if (newFile.isPresent()) {
          datafileSizes.put(newFile.orElseThrow(), dfv);
          // could be used by a follow on compaction in a multipass compaction
        }

        tablet.computeNumEntries();

        lastLocation = tablet.resetLastLocation();

        if (compactionId != null && Collections.disjoint(selectedFiles, datafileSizes.keySet())) {
          compactionIdToWrite = compactionId;
        }

        t2 = System.currentTimeMillis();
      }

      // known consistency issue between minor and major compactions - see ACCUMULO-18
      Set<StoredTabletFile> filesInUseByScans = waitForScansToFinish(oldDatafiles);
      if (!filesInUseByScans.isEmpty()) {
        log.debug("Adding scan refs to metadata {} {}", extent, filesInUseByScans);
      }
      ManagerMetadataUtil.replaceDatafiles(tablet.getContext(), extent, oldDatafiles,
          filesInUseByScans, newFile, compactionIdToWrite, dfv,
          tablet.getTabletServer().getTabletSession(), lastLocation,
          tablet.getTabletServer().getLock(), ecid);
      tablet.setLastCompactionID(compactionIdToWrite);
      removeFilesAfterScan(filesInUseByScans);

    } catch (Exception e) {
      // Any exception in this code is prone to leaving the persisted tablet metadata and the
      // tablets in memory data structs out of sync. Log the extent and exact files involved as this
      // may be useful for debugging.
      log.error("Failure updating files after major compaction {} {} {}", tablet.getExtent(),
          newFile, oldDatafiles, e);
      throw e;
    } finally {
      // increment finish count after metadata update AND updating in memory map of files
      metadataUpdateCount.updateAndGet(MetadataUpdateCount::incrementFinish);
    }

    if (log.isTraceEnabled()) {
      log.trace(String.format("MajC finish lock %.2f secs", (t2 - t1) / 1000.0));
    }

    return newFile;
  }

  public SortedMap<StoredTabletFile,DataFileValue> getDatafileSizes() {
    synchronized (tablet) {
      TreeMap<StoredTabletFile,DataFileValue> copy = new TreeMap<>(datafileSizes);
      return Collections.unmodifiableSortedMap(copy);
    }
  }

  public Set<TabletFile> getFiles() {
    synchronized (tablet) {
      HashSet<TabletFile> files = new HashSet<>(datafileSizes.keySet());
      return Collections.unmodifiableSet(files);
    }
  }

  public int getNumFiles() {
    return datafileSizes.size();
  }

  public MetadataUpdateCount getUpdateCount() {
    return metadataUpdateCount.get();
  }

}

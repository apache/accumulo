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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.replication.ReplicationConfigurationUtil;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.util.MasterMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.hadoop.fs.Path;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

class DatafileManager {
  private final Logger log = LoggerFactory.getLogger(DatafileManager.class);
  // access to datafilesizes needs to be synchronized: see CompactionRunner#getNumFiles
  private final Map<StoredTabletFile,DataFileValue> datafileSizes =
      Collections.synchronizedMap(new TreeMap<>());
  private final Tablet tablet;

  // ensure we only have one reader/writer of our bulk file notes at at time
  private final Object bulkFileImportLock = new Object();

  DatafileManager(Tablet tablet, SortedMap<StoredTabletFile,DataFileValue> datafileSizes) {
    for (Entry<StoredTabletFile,DataFileValue> datafiles : datafileSizes.entrySet()) {
      this.datafileSizes.put(datafiles.getKey(), datafiles.getValue());
    }
    this.tablet = tablet;
  }

  private final Set<TabletFile> filesToDeleteAfterScan = new HashSet<>();
  private final Map<Long,Set<StoredTabletFile>> scanFileReservations = new HashMap<>();
  private final MapCounter<StoredTabletFile> fileScanReferenceCounts = new MapCounter<>();
  private long nextScanReservationId = 0;
  private boolean reservationsBlocked = false;

  static void rename(VolumeManager fs, Path src, Path dst) throws IOException {
    if (!fs.rename(src, dst)) {
      throw new IOException("Rename " + src + " to " + dst + " returned false ");
    }
  }

  Pair<Long,Map<TabletFile,DataFileValue>> reserveFilesForScan() {
    synchronized (tablet) {

      while (reservationsBlocked) {
        try {
          tablet.wait(50);
        } catch (InterruptedException e) {
          log.warn("{}", e.getMessage(), e);
        }
      }

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

      if (absFilePaths == null)
        throw new IllegalArgumentException("Unknown scan reservation id " + reservationId);

      boolean notify = false;
      for (StoredTabletFile path : absFilePaths) {
        long refCount = fileScanReferenceCounts.decrement(path, 1);
        if (refCount == 0) {
          if (filesToDeleteAfterScan.remove(path))
            filesToDelete.add(path);
          notify = true;
        } else if (refCount < 0)
          throw new IllegalStateException("Scan ref count for " + path + " is " + refCount);
      }

      if (notify)
        tablet.notifyAll();
    }

    if (!filesToDelete.isEmpty()) {
      log.debug("Removing scan refs from metadata {} {}", tablet.getExtent(), filesToDelete);
      MetadataTableUtil.removeScanFiles(tablet.getExtent(), filesToDelete, tablet.getContext(),
          tablet.getTabletServer().getLock());
    }
  }

  void removeFilesAfterScan(Set<StoredTabletFile> scanFiles) {
    if (scanFiles.isEmpty())
      return;

    Set<StoredTabletFile> filesToDelete = new HashSet<>();

    synchronized (tablet) {
      for (StoredTabletFile path : scanFiles) {
        if (fileScanReferenceCounts.get(path) == 0)
          filesToDelete.add(path);
        else
          filesToDeleteAfterScan.add(path);
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

    try (TraceScope waitForScans = Trace.startSpan("waitForScans")) {
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
          if (fileScanReferenceCounts.get(path) > 0)
            inUse.add(path);
        }
      }
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
      for (String tablesDir : ServerConstants.getTablesDirs(tablet.getContext())) {
        if (parent.equals(new Path(tablesDir, tablet.getExtent().tableId().canonical()))) {
          inTheRightDirectory = true;
          break;
        }
      }
      if (!inTheRightDirectory) {
        throw new IOException("Data file " + tpath + " not in table dirs");
      }

      if (bulkDir == null)
        bulkDir = tpath.getTabletDir();
      else if (!bulkDir.equals(tpath.getTabletDir()))
        throw new IllegalArgumentException("bulk files in different dirs " + bulkDir + " " + tpath);

    }

    if (tablet.getExtent().isMeta()) {
      throw new IllegalArgumentException("Can not import files to a metadata tablet");
    }

    synchronized (bulkFileImportLock) {

      if (!paths.isEmpty()) {
        long bulkTime = Long.MIN_VALUE;
        if (setTime) {
          for (DataFileValue dfv : paths.values()) {
            long nextTime = tablet.getAndUpdateTime();
            if (nextTime < bulkTime)
              throw new IllegalStateException(
                  "Time went backwards unexpectedly " + nextTime + " " + bulkTime);
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

    return newFiles.keySet();
  }

  StoredTabletFile bringMinorCompactionOnline(TabletFile tmpDatafile, TabletFile newDatafile,
      DataFileValue dfv, CommitSession commitSession, long flushId) {
    StoredTabletFile newFile;
    // rename before putting in metadata table, so files in metadata table should
    // always exist
    boolean attemptedRename = false;
    do {
      try {
        if (dfv.getNumEntries() == 0) {

          tablet.getTabletServer().getFileSystem().deleteRecursively(tmpDatafile.getPath());
        } else {
          if (!attemptedRename
              && tablet.getTabletServer().getFileSystem().exists(newDatafile.getPath())) {
            log.warn("Target map file already exist {}", newDatafile);
            throw new RuntimeException("File unexpectedly exists " + newDatafile.getPath());
          }
          // the following checks for spurious rename failures that succeeded but gave an IoE
          if (attemptedRename
              && tablet.getTabletServer().getFileSystem().exists(newDatafile.getPath())
              && !tablet.getTabletServer().getFileSystem().exists(tmpDatafile.getPath())) {
            // seems like previous rename succeeded, so break
            break;
          }
          attemptedRename = true;
          rename(tablet.getTabletServer().getFileSystem(), tmpDatafile.getPath(),
              newDatafile.getPath());
        }
        break;
      } catch (IOException ioe) {
        log.warn("Tablet " + tablet.getExtent() + " failed to rename " + newDatafile
            + " after MinC, will retry in 60 secs...", ioe);
        sleepUninterruptibly(1, TimeUnit.MINUTES);
      }
    } while (true);

    long t1, t2;

    Set<String> unusedWalLogs = tablet.beginClearingUnusedLogs();
    boolean replicate =
        ReplicationConfigurationUtil.isEnabled(tablet.getExtent(), tablet.getTableConfiguration());
    Set<String> logFileOnly = null;
    if (replicate) {
      // unusedWalLogs is of the form host/fileURI, need to strip off the host portion
      logFileOnly = new HashSet<>();
      for (String unusedWalLog : unusedWalLogs) {
        int index = unusedWalLog.indexOf('/');
        if (index == -1) {
          log.warn("Could not find host component to strip from DFSLogger representation of WAL");
        } else {
          unusedWalLog = unusedWalLog.substring(index + 1);
        }
        logFileOnly.add(unusedWalLog);
      }
    }
    try {
      // the order of writing to metadata and walog is important in the face of machine/process
      // failures
      // need to write to metadata before writing to walog, when things are done in the reverse
      // order
      // data could be lost... the minor compaction start even should be written before the
      // following metadata
      // write is made

      newFile = tablet.updateTabletDataFile(commitSession.getMaxCommittedTime(), newDatafile, dfv,
          unusedWalLogs, flushId);

      // Mark that we have data we want to replicate
      // This WAL could still be in use by other Tablets *from the same table*, so we can only mark
      // that there is data to replicate,
      // but it is *not* closed. We know it is not closed by the fact that this MinC triggered. A
      // MinC cannot happen unless the
      // tablet is online and thus these WALs are referenced by that tablet. Therefore, the WAL
      // replication status cannot be 'closed'.
      if (replicate) {
        if (log.isDebugEnabled()) {
          log.debug("Recording that data has been ingested into {} using {}", tablet.getExtent(),
              logFileOnly);
        }
        for (String logFile : logFileOnly) {
          ReplicationTableUtil.updateFiles(tablet.getContext(), tablet.getExtent(), logFile,
              StatusUtil.openWithUnknownLength());
        }
      }
    } finally {
      tablet.finishClearingUnusedLogs();
    }

    do {
      try {
        // the purpose of making this update use the new commit session, instead of the old one
        // passed in,
        // is because the new one will reference the logs used by current memory...

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

      if (dfv.getNumEntries() > 0) {
        if (datafileSizes.containsKey(newFile)) {
          log.error("Adding file that is already in set {}", newFile);
        }
        datafileSizes.put(newFile, dfv);
      }

      tablet.flushComplete(flushId);

      t2 = System.currentTimeMillis();
    }

    TabletLogger.flushed(tablet.getExtent(), newDatafile);

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

  StoredTabletFile bringMajorCompactionOnline(Set<StoredTabletFile> oldDatafiles,
      TabletFile tmpDatafile, TabletFile newDatafile, Long compactionId, DataFileValue dfv)
      throws IOException {
    final KeyExtent extent = tablet.getExtent();
    long t1, t2;

    if (tablet.getTabletServer().getFileSystem().exists(newDatafile.getPath())) {
      log.error("Target map file already exist " + newDatafile, new Exception());
      throw new IllegalStateException("Target map file already exist " + newDatafile);
    }

    if (dfv.getNumEntries() == 0) {
      tablet.getTabletServer().getFileSystem().deleteRecursively(tmpDatafile.getPath());
    } else {
      // rename before putting in metadata table, so files in metadata table should
      // always exist
      rename(tablet.getTabletServer().getFileSystem(), tmpDatafile.getPath(),
          newDatafile.getPath());
    }

    TServerInstance lastLocation = null;
    // calling insert to get the new file before inserting into the metadata
    StoredTabletFile newFile = newDatafile.insert();
    synchronized (tablet) {
      t1 = System.currentTimeMillis();

      Preconditions.checkState(datafileSizes.keySet().containsAll(oldDatafiles),
          "Compacted files %s are not a subset of tablet files %s", oldDatafiles,
          datafileSizes.keySet());
      if (dfv.getNumEntries() > 0) {
        Preconditions.checkState(!datafileSizes.containsKey(newFile),
            "New compaction file %s already exist in tablet files %s", newFile,
            datafileSizes.keySet());
      }

      tablet.incrementDataSourceDeletions();

      datafileSizes.keySet().removeAll(oldDatafiles);

      if (dfv.getNumEntries() > 0) {
        datafileSizes.put(newFile, dfv);
        // could be used by a follow on compaction in a multipass compaction
      }

      tablet.computeNumEntries();

      lastLocation = tablet.resetLastLocation();

      tablet.setLastCompactionID(compactionId);
      t2 = System.currentTimeMillis();
    }

    // known consistency issue between minor and major compactions - see ACCUMULO-18
    Set<StoredTabletFile> filesInUseByScans = waitForScansToFinish(oldDatafiles);
    if (!filesInUseByScans.isEmpty())
      log.debug("Adding scan refs to metadata {} {}", extent, filesInUseByScans);
    MasterMetadataUtil.replaceDatafiles(tablet.getContext(), extent, oldDatafiles,
        filesInUseByScans, newFile, compactionId, dfv,
        tablet.getTabletServer().getClientAddressString(), lastLocation,
        tablet.getTabletServer().getLock());
    removeFilesAfterScan(filesInUseByScans);

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

}

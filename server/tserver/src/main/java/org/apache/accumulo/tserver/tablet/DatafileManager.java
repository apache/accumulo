/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.tserver.tablet;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

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

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.replication.ReplicationConfigurationUtil;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.util.MasterMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.tserver.TLevel;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

class DatafileManager {
  private final Logger log = Logger.getLogger(DatafileManager.class);
  // access to datafilesizes needs to be synchronized: see CompactionRunner#getNumFiles
  private final Map<FileRef,DataFileValue> datafileSizes = Collections.synchronizedMap(new TreeMap<FileRef,DataFileValue>());
  private final Tablet tablet;
  private Long maxMergingMinorCompactionFileSize;

  // ensure we only have one reader/writer of our bulk file notes at at time
  private final Object bulkFileImportLock = new Object();

  DatafileManager(Tablet tablet, SortedMap<FileRef,DataFileValue> datafileSizes) {
    for (Entry<FileRef,DataFileValue> datafiles : datafileSizes.entrySet()) {
      this.datafileSizes.put(datafiles.getKey(), datafiles.getValue());
    }
    this.tablet = tablet;
  }

  private FileRef mergingMinorCompactionFile = null;
  private final Set<FileRef> filesToDeleteAfterScan = new HashSet<>();
  private final Map<Long,Set<FileRef>> scanFileReservations = new HashMap<>();
  private final MapCounter<FileRef> fileScanReferenceCounts = new MapCounter<>();
  private long nextScanReservationId = 0;
  private boolean reservationsBlocked = false;

  private final Set<FileRef> majorCompactingFiles = new HashSet<>();

  static void rename(VolumeManager fs, Path src, Path dst) throws IOException {
    if (!fs.rename(src, dst)) {
      throw new IOException("Rename " + src + " to " + dst + " returned false ");
    }
  }

  Pair<Long,Map<FileRef,DataFileValue>> reserveFilesForScan() {
    synchronized (tablet) {

      while (reservationsBlocked) {
        try {
          tablet.wait(50);
        } catch (InterruptedException e) {
          log.warn(e, e);
        }
      }

      Set<FileRef> absFilePaths = new HashSet<>(datafileSizes.keySet());

      long rid = nextScanReservationId++;

      scanFileReservations.put(rid, absFilePaths);

      Map<FileRef,DataFileValue> ret = new HashMap<>();

      for (FileRef path : absFilePaths) {
        fileScanReferenceCounts.increment(path, 1);
        ret.put(path, datafileSizes.get(path));
      }

      return new Pair<>(rid, ret);
    }
  }

  void returnFilesForScan(Long reservationId) {

    final Set<FileRef> filesToDelete = new HashSet<>();

    synchronized (tablet) {
      Set<FileRef> absFilePaths = scanFileReservations.remove(reservationId);

      if (absFilePaths == null)
        throw new IllegalArgumentException("Unknown scan reservation id " + reservationId);

      boolean notify = false;
      for (FileRef path : absFilePaths) {
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

    if (filesToDelete.size() > 0) {
      log.debug("Removing scan refs from metadata " + tablet.getExtent() + " " + filesToDelete);
      MetadataTableUtil.removeScanFiles(tablet.getExtent(), filesToDelete, tablet.getTabletServer(), tablet.getTabletServer().getLock());
    }
  }

  void removeFilesAfterScan(Set<FileRef> scanFiles) {
    if (scanFiles.size() == 0)
      return;

    Set<FileRef> filesToDelete = new HashSet<>();

    synchronized (tablet) {
      for (FileRef path : scanFiles) {
        if (fileScanReferenceCounts.get(path) == 0)
          filesToDelete.add(path);
        else
          filesToDeleteAfterScan.add(path);
      }
    }

    if (filesToDelete.size() > 0) {
      log.debug("Removing scan refs from metadata " + tablet.getExtent() + " " + filesToDelete);
      MetadataTableUtil.removeScanFiles(tablet.getExtent(), filesToDelete, tablet.getTabletServer(), tablet.getTabletServer().getLock());
    }
  }

  private TreeSet<FileRef> waitForScansToFinish(Set<FileRef> pathsToWaitFor, boolean blockNewScans, long maxWaitTime) {
    long startTime = System.currentTimeMillis();
    TreeSet<FileRef> inUse = new TreeSet<>();

    Span waitForScans = Trace.start("waitForScans");
    try {
      synchronized (tablet) {
        if (blockNewScans) {
          if (reservationsBlocked)
            throw new IllegalStateException();

          reservationsBlocked = true;
        }

        for (FileRef path : pathsToWaitFor) {
          while (fileScanReferenceCounts.get(path) > 0 && System.currentTimeMillis() - startTime < maxWaitTime) {
            try {
              tablet.wait(100);
            } catch (InterruptedException e) {
              log.warn(e, e);
            }
          }
        }

        for (FileRef path : pathsToWaitFor) {
          if (fileScanReferenceCounts.get(path) > 0)
            inUse.add(path);
        }

        if (blockNewScans) {
          reservationsBlocked = false;
          tablet.notifyAll();
        }

      }
    } finally {
      waitForScans.stop();
    }
    return inUse;
  }

  public void importMapFiles(long tid, Map<FileRef,DataFileValue> pathsString, boolean setTime) throws IOException {

    String bulkDir = null;

    Map<FileRef,DataFileValue> paths = new HashMap<>();
    for (Entry<FileRef,DataFileValue> entry : pathsString.entrySet())
      paths.put(entry.getKey(), entry.getValue());

    for (FileRef tpath : paths.keySet()) {

      boolean inTheRightDirectory = false;
      Path parent = tpath.path().getParent().getParent();
      for (String tablesDir : ServerConstants.getTablesDirs()) {
        if (parent.equals(new Path(tablesDir, tablet.getExtent().getTableId()))) {
          inTheRightDirectory = true;
          break;
        }
      }
      if (!inTheRightDirectory) {
        throw new IOException("Data file " + tpath + " not in table dirs");
      }

      if (bulkDir == null)
        bulkDir = tpath.path().getParent().toString();
      else if (!bulkDir.equals(tpath.path().getParent().toString()))
        throw new IllegalArgumentException("bulk files in different dirs " + bulkDir + " " + tpath);

    }

    if (tablet.getExtent().isMeta()) {
      throw new IllegalArgumentException("Can not import files to a metadata tablet");
    }

    synchronized (bulkFileImportLock) {

      if (paths.size() > 0) {
        long bulkTime = Long.MIN_VALUE;
        if (setTime) {
          for (DataFileValue dfv : paths.values()) {
            long nextTime = tablet.getAndUpdateTime();
            if (nextTime < bulkTime)
              throw new IllegalStateException("Time went backwards unexpectedly " + nextTime + " " + bulkTime);
            bulkTime = nextTime;
            dfv.setTime(bulkTime);
          }
        }

        tablet.updatePersistedTime(bulkTime, paths, tid);
      }
    }

    synchronized (tablet) {
      for (Entry<FileRef,DataFileValue> tpath : paths.entrySet()) {
        if (datafileSizes.containsKey(tpath.getKey())) {
          log.error("Adding file that is already in set " + tpath.getKey());
        }
        datafileSizes.put(tpath.getKey(), tpath.getValue());

      }

      tablet.getTabletResources().importedMapFiles();

      tablet.computeNumEntries();
    }

    for (Entry<FileRef,DataFileValue> entry : paths.entrySet()) {
      log.log(TLevel.TABLET_HIST, tablet.getExtent() + " import " + entry.getKey() + " " + entry.getValue());
    }
  }

  FileRef reserveMergingMinorCompactionFile() {
    if (mergingMinorCompactionFile != null)
      throw new IllegalStateException("Tried to reserve merging minor compaction file when already reserved  : " + mergingMinorCompactionFile);

    if (tablet.getExtent().isRootTablet())
      return null;

    int maxFiles = tablet.getTableConfiguration().getMaxFilesPerTablet();

    // when a major compaction is running and we are at max files, write out
    // one extra file... want to avoid the case where major compaction is
    // compacting everything except for the largest file, and therefore the
    // largest file is returned for merging.. the following check mostly
    // avoids this case, except for the case where major compactions fail or
    // are canceled
    if (majorCompactingFiles.size() > 0 && datafileSizes.size() == maxFiles)
      return null;

    if (datafileSizes.size() >= maxFiles) {
      // find the smallest file

      long maxFileSize = Long.MAX_VALUE;
      maxMergingMinorCompactionFileSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(tablet.getTableConfiguration().get(
          Property.TABLE_MINC_MAX_MERGE_FILE_SIZE));
      if (maxMergingMinorCompactionFileSize > 0) {
        maxFileSize = maxMergingMinorCompactionFileSize;
      }
      long min = maxFileSize;
      FileRef minName = null;

      for (Entry<FileRef,DataFileValue> entry : datafileSizes.entrySet()) {
        if (entry.getValue().getSize() <= min && !majorCompactingFiles.contains(entry.getKey())) {
          min = entry.getValue().getSize();
          minName = entry.getKey();
        }
      }

      if (minName == null)
        return null;

      mergingMinorCompactionFile = minName;
      return minName;
    }

    return null;
  }

  void unreserveMergingMinorCompactionFile(FileRef file) {
    if ((file == null && mergingMinorCompactionFile != null) || (file != null && mergingMinorCompactionFile == null)
        || (file != null && mergingMinorCompactionFile != null && !file.equals(mergingMinorCompactionFile)))
      throw new IllegalStateException("Disagreement " + file + " " + mergingMinorCompactionFile);

    mergingMinorCompactionFile = null;
  }

  void bringMinorCompactionOnline(FileRef tmpDatafile, FileRef newDatafile, FileRef absMergeFile, DataFileValue dfv, CommitSession commitSession, long flushId)
      throws IOException {

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    if (tablet.getExtent().isRootTablet()) {
      try {
        if (!zoo.isLockHeld(tablet.getTabletServer().getLock().getLockID())) {
          throw new IllegalStateException();
        }
      } catch (Exception e) {
        throw new IllegalStateException("Can not bring major compaction online, lock not held", e);
      }
    }

    // rename before putting in metadata table, so files in metadata table should
    // always exist
    do {
      try {
        if (dfv.getNumEntries() == 0) {
          tablet.getTabletServer().getFileSystem().deleteRecursively(tmpDatafile.path());
        } else {
          if (tablet.getTabletServer().getFileSystem().exists(newDatafile.path())) {
            log.warn("Target map file already exist " + newDatafile);
            tablet.getTabletServer().getFileSystem().deleteRecursively(newDatafile.path());
          }

          rename(tablet.getTabletServer().getFileSystem(), tmpDatafile.path(), newDatafile.path());
        }
        break;
      } catch (IOException ioe) {
        log.warn("Tablet " + tablet.getExtent() + " failed to rename " + newDatafile + " after MinC, will retry in 60 secs...", ioe);
        sleepUninterruptibly(1, TimeUnit.MINUTES);
      }
    } while (true);

    long t1, t2;

    // the code below always assumes merged files are in use by scans... this must be done
    // because the in memory list of files is not updated until after the metadata table
    // therefore the file is available to scans until memory is updated, but want to ensure
    // the file is not available for garbage collection... if memory were updated
    // before this point (like major compactions do), then the following code could wait
    // for scans to finish like major compactions do.... used to wait for scans to finish
    // here, but that was incorrect because a scan could start after waiting but before
    // memory was updated... assuming the file is always in use by scans leads to
    // one uneeded metadata update when it was not actually in use
    Set<FileRef> filesInUseByScans = Collections.emptySet();
    if (absMergeFile != null)
      filesInUseByScans = Collections.singleton(absMergeFile);

    // very important to write delete entries outside of log lock, because
    // this metadata write does not go up... it goes sideways or to itself
    if (absMergeFile != null)
      MetadataTableUtil.addDeleteEntries(tablet.getExtent(), Collections.singleton(absMergeFile), tablet.getTabletServer());

    Set<String> unusedWalLogs = tablet.beginClearingUnusedLogs();
    boolean replicate = ReplicationConfigurationUtil.isEnabled(tablet.getExtent(), tablet.getTableConfiguration());
    Set<String> logFileOnly = null;
    if (replicate) {
      // unusedWalLogs is of the form host/fileURI, need to strip off the host portion
      logFileOnly = new HashSet<>();
      for (String unusedWalLog : unusedWalLogs) {
        int index = unusedWalLog.indexOf('/');
        if (-1 == index) {
          log.warn("Could not find host component to strip from DFSLogger representation of WAL");
        } else {
          unusedWalLog = unusedWalLog.substring(index + 1);
        }
        logFileOnly.add(unusedWalLog);
      }
    }
    try {
      // the order of writing to metadata and walog is important in the face of machine/process failures
      // need to write to metadata before writing to walog, when things are done in the reverse order
      // data could be lost... the minor compaction start even should be written before the following metadata
      // write is made

      tablet.updateTabletDataFile(commitSession.getMaxCommittedTime(), newDatafile, absMergeFile, dfv, unusedWalLogs, filesInUseByScans, flushId);

      // Mark that we have data we want to replicate
      // This WAL could still be in use by other Tablets *from the same table*, so we can only mark that there is data to replicate,
      // but it is *not* closed. We know it is not closed by the fact that this MinC triggered. A MinC cannot happen unless the
      // tablet is online and thus these WALs are referenced by that tablet. Therefore, the WAL replication status cannot be 'closed'.
      if (replicate) {
        if (log.isDebugEnabled()) {
          log.debug("Recording that data has been ingested into " + tablet.getExtent() + " using " + logFileOnly);
        }
        for (String logFile : logFileOnly) {
          ReplicationTableUtil.updateFiles(tablet.getTabletServer(), tablet.getExtent(), logFile, StatusUtil.openWithUnknownLength());
        }
      }
    } finally {
      tablet.finishClearingUnusedLogs();
    }

    do {
      try {
        // the purpose of making this update use the new commit session, instead of the old one passed in,
        // is because the new one will reference the logs used by current memory...

        tablet.getTabletServer().minorCompactionFinished(tablet.getTabletMemory().getCommitSession(), newDatafile.toString(), commitSession.getWALogSeq() + 2);
        break;
      } catch (IOException e) {
        log.error("Failed to write to write-ahead log " + e.getMessage() + " will retry", e);
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    } while (true);

    synchronized (tablet) {
      t1 = System.currentTimeMillis();

      if (datafileSizes.containsKey(newDatafile)) {
        log.error("Adding file that is already in set " + newDatafile);
      }

      if (dfv.getNumEntries() > 0) {
        datafileSizes.put(newDatafile, dfv);
      }

      if (absMergeFile != null) {
        datafileSizes.remove(absMergeFile);
      }

      unreserveMergingMinorCompactionFile(absMergeFile);

      tablet.flushComplete(flushId);

      t2 = System.currentTimeMillis();
    }

    // must do this after list of files in memory is updated above
    removeFilesAfterScan(filesInUseByScans);

    if (absMergeFile != null)
      log.log(TLevel.TABLET_HIST, tablet.getExtent() + " MinC [" + absMergeFile + ",memory] -> " + newDatafile);
    else
      log.log(TLevel.TABLET_HIST, tablet.getExtent() + " MinC [memory] -> " + newDatafile);
    log.debug(String.format("MinC finish lock %.2f secs %s", (t2 - t1) / 1000.0, tablet.getExtent().toString()));
    long splitSize = tablet.getTableConfiguration().getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    if (dfv.getSize() > splitSize) {
      log.debug(String.format("Minor Compaction wrote out file larger than split threshold.  split threshold = %,d  file size = %,d", splitSize, dfv.getSize()));
    }
  }

  public void reserveMajorCompactingFiles(Collection<FileRef> files) {
    if (majorCompactingFiles.size() != 0)
      throw new IllegalStateException("Major compacting files not empty " + majorCompactingFiles);

    if (mergingMinorCompactionFile != null && files.contains(mergingMinorCompactionFile))
      throw new IllegalStateException("Major compaction tried to resrve file in use by minor compaction " + mergingMinorCompactionFile);

    majorCompactingFiles.addAll(files);
  }

  public void clearMajorCompactingFile() {
    majorCompactingFiles.clear();
  }

  void bringMajorCompactionOnline(Set<FileRef> oldDatafiles, FileRef tmpDatafile, FileRef newDatafile, Long compactionId, DataFileValue dfv) throws IOException {
    final KeyExtent extent = tablet.getExtent();
    long t1, t2;

    if (!extent.isRootTablet()) {

      if (tablet.getTabletServer().getFileSystem().exists(newDatafile.path())) {
        log.error("Target map file already exist " + newDatafile, new Exception());
        throw new IllegalStateException("Target map file already exist " + newDatafile);
      }

      // rename before putting in metadata table, so files in metadata table should
      // always exist
      rename(tablet.getTabletServer().getFileSystem(), tmpDatafile.path(), newDatafile.path());

      if (dfv.getNumEntries() == 0) {
        tablet.getTabletServer().getFileSystem().deleteRecursively(newDatafile.path());
      }
    }

    TServerInstance lastLocation = null;
    synchronized (tablet) {

      t1 = System.currentTimeMillis();

      IZooReaderWriter zoo = ZooReaderWriter.getInstance();

      tablet.incrementDataSourceDeletions();

      if (extent.isRootTablet()) {

        waitForScansToFinish(oldDatafiles, true, Long.MAX_VALUE);

        try {
          if (!zoo.isLockHeld(tablet.getTabletServer().getLock().getLockID())) {
            throw new IllegalStateException();
          }
        } catch (Exception e) {
          throw new IllegalStateException("Can not bring major compaction online, lock not held", e);
        }

        // mark files as ready for deletion, but
        // do not delete them until we successfully
        // rename the compacted map file, in case
        // the system goes down

        RootFiles.replaceFiles(tablet.getTableConfiguration(), tablet.getTabletServer().getFileSystem(), tablet.getLocation(), oldDatafiles, tmpDatafile,
            newDatafile);
      }

      // atomically remove old files and add new file
      for (FileRef oldDatafile : oldDatafiles) {
        if (!datafileSizes.containsKey(oldDatafile)) {
          log.error("file does not exist in set " + oldDatafile);
        }
        datafileSizes.remove(oldDatafile);
        majorCompactingFiles.remove(oldDatafile);
      }

      if (datafileSizes.containsKey(newDatafile)) {
        log.error("Adding file that is already in set " + newDatafile);
      }

      if (dfv.getNumEntries() > 0) {
        datafileSizes.put(newDatafile, dfv);
      }

      // could be used by a follow on compaction in a multipass compaction
      majorCompactingFiles.add(newDatafile);

      tablet.computeNumEntries();

      lastLocation = tablet.resetLastLocation();

      tablet.setLastCompactionID(compactionId);
      t2 = System.currentTimeMillis();
    }

    if (!extent.isRootTablet()) {
      Set<FileRef> filesInUseByScans = waitForScansToFinish(oldDatafiles, false, 10000);
      if (filesInUseByScans.size() > 0)
        log.debug("Adding scan refs to metadata " + extent + " " + filesInUseByScans);
      MasterMetadataUtil.replaceDatafiles(tablet.getTabletServer(), extent, oldDatafiles, filesInUseByScans, newDatafile, compactionId, dfv, tablet
          .getTabletServer().getClientAddressString(), lastLocation, tablet.getTabletServer().getLock());
      removeFilesAfterScan(filesInUseByScans);
    }

    log.debug(String.format("MajC finish lock %.2f secs", (t2 - t1) / 1000.0));
    log.log(TLevel.TABLET_HIST, extent + " MajC " + oldDatafiles + " --> " + newDatafile);
  }

  public SortedMap<FileRef,DataFileValue> getDatafileSizes() {
    synchronized (tablet) {
      TreeMap<FileRef,DataFileValue> copy = new TreeMap<>(datafileSizes);
      return Collections.unmodifiableSortedMap(copy);
    }
  }

  public Set<FileRef> getFiles() {
    synchronized (tablet) {
      HashSet<FileRef> files = new HashSet<>(datafileSizes.keySet());
      return Collections.unmodifiableSet(files);
    }
  }

  public int getNumFiles() {
    return datafileSizes.size();
  }

}

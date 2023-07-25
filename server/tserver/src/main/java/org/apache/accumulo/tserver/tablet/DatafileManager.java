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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DatafileManager {
  private final Logger log = LoggerFactory.getLogger(DatafileManager.class);
  // access to datafilesizes needs to be synchronized: see CompactionRunner#getNumFiles
  private final Map<StoredTabletFile,DataFileValue> datafileSizes =
      Collections.synchronizedMap(new TreeMap<>());
  private final Tablet tablet;

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

  private final Set<StoredTabletFile> filesToDeleteAfterScan = new HashSet<>();
  private final Map<Long,Set<StoredTabletFile>> scanFileReservations = new HashMap<>();
  private final MapCounter<StoredTabletFile> fileScanReferenceCounts = new MapCounter<>();
  private long nextScanReservationId = 0;

  static void rename(VolumeManager fs, Path src, Path dst) throws IOException {
    if (!fs.rename(src, dst)) {
      throw new IOException("Rename " + src + " to " + dst + " returned false ");
    }
  }

  Pair<Long,Map<StoredTabletFile,DataFileValue>> reserveFilesForScan() {
    synchronized (tablet) {

      Set<StoredTabletFile> absFilePaths = new HashSet<>(datafileSizes.keySet());

      long rid = nextScanReservationId++;

      scanFileReservations.put(rid, absFilePaths);

      Map<StoredTabletFile,DataFileValue> ret = new HashMap<>();

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

  /**
   * Returns Optional of the new file created. It is possible that the file was just flushed with no
   * entries so was not inserted into the metadata. In this case empty is returned. If the file was
   * stored in the metadata table, then StoredTableFile will be returned.
   */
  Optional<StoredTabletFile> bringMinorCompactionOnline(ReferencedTabletFile tmpDatafile,
      ReferencedTabletFile newDatafile, DataFileValue dfv, CommitSession commitSession,
      long flushId) {
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
            log.warn("Target data file already exist {}", newDatafile);
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
    // do not place any code here between above stmt and try{}finally
    try {
      Set<String> unusedWalLogs = tablet.beginClearingUnusedLogs();
      try {
        // the order of writing to metadata and walog is important in the face of machine/process
        // failures need to write to metadata before writing to walog, when things are done in the
        // reverse order data could be lost... the minor compaction start even should be written
        // before the following metadata write is made
        newFile = tablet.updateTabletDataFile(commitSession.getMaxCommittedTime(), newDatafile, dfv,
            unusedWalLogs, flushId);
      } finally {
        tablet.finishClearingUnusedLogs();
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

  public SortedMap<StoredTabletFile,DataFileValue> getDatafileSizes() {
    synchronized (tablet) {
      TreeMap<StoredTabletFile,DataFileValue> copy = new TreeMap<>(datafileSizes);
      return Collections.unmodifiableSortedMap(copy);
    }
  }

  public MetadataUpdateCount getUpdateCount() {
    return metadataUpdateCount.get();
  }

  // ELASTICITY_TODO remove this method
  public void setFilesHack(Map<StoredTabletFile,DataFileValue> files) {
    synchronized (tablet) {
      datafileSizes.clear();
      datafileSizes.putAll(files);
    }
  }
}

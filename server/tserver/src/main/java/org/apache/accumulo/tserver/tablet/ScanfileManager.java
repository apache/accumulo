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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScanfileManager {
  private final Logger log = LoggerFactory.getLogger(ScanfileManager.class);
  private final Tablet tablet;

  ScanfileManager(Tablet tablet) {
    this.tablet = tablet;
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

      var tabletsFiles = tablet.getDatafiles();
      Set<StoredTabletFile> absFilePaths = new HashSet<>(tabletsFiles.keySet());

      long rid = nextScanReservationId++;

      scanFileReservations.put(rid, absFilePaths);

      Map<StoredTabletFile,DataFileValue> ret = new HashMap<>();

      for (StoredTabletFile path : absFilePaths) {
        fileScanReferenceCounts.increment(path, 1);
        ret.put(path, tabletsFiles.get(path));
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
      // ELASTICTIY_TODO use conditional mutation
      MetadataTableUtil.removeScanFiles(tablet.getExtent(), filesToDelete, tablet.getContext(),
          tablet.getTabletServer().getLock());
    }
  }

  void removeFilesAfterScan(Collection<StoredTabletFile> scanFiles) {
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
      // ELASTICTIY_TODO use conditional mutation and require the tablet location
      log.debug("Removing scan refs from metadata {} {}", tablet.getExtent(), filesToDelete);
      MetadataTableUtil.removeScanFiles(tablet.getExtent(), filesToDelete, tablet.getContext(),
          tablet.getTabletServer().getLock());
    }
  }
}

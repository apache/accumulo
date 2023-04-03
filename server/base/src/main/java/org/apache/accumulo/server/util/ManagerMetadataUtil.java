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
package org.apache.accumulo.server.util;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagerMetadataUtil {

  private static final Logger log = LoggerFactory.getLogger(ManagerMetadataUtil.class);

  public static void addNewTablet(ServerContext context, KeyExtent extent, String dirName,
      TServerInstance tServerInstance, Map<StoredTabletFile,DataFileValue> datafileSizes,
      Map<Long,? extends Collection<TabletFile>> bulkLoadedFiles, MetadataTime time,
      long lastFlushID, long lastCompactID, ServiceLock zooLock) {

    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putPrevEndRow(extent.prevEndRow());
    tablet.putZooLock(zooLock);
    tablet.putDirName(dirName);
    tablet.putTime(time);

    if (lastFlushID > 0) {
      tablet.putFlushId(lastFlushID);
    }

    if (lastCompactID > 0) {
      tablet.putCompactionId(lastCompactID);
    }

    if (tServerInstance != null) {
      tablet.putLocation(Location.current(tServerInstance));
      tablet.deleteLocation(Location.future(tServerInstance));
    }

    datafileSizes.forEach(tablet::putFile);

    for (Entry<Long,? extends Collection<TabletFile>> entry : bulkLoadedFiles.entrySet()) {
      for (TabletFile ref : entry.getValue()) {
        tablet.putBulkFile(ref, entry.getKey());
      }
    }

    tablet.mutate();
  }

  private static TServerInstance getTServerInstance(String address, ServiceLock zooLock) {
    while (true) {
      try {
        return new TServerInstance(address, zooLock.getSessionId());
      } catch (KeeperException | InterruptedException e) {
        log.error("{}", e.getMessage(), e);
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  public static void replaceDatafiles(ServerContext context, KeyExtent extent,
      Set<StoredTabletFile> datafilesToDelete, Set<StoredTabletFile> scanFiles,
      Optional<StoredTabletFile> path, Long compactionId, DataFileValue size, String address,
      Location lastLocation, ServiceLock zooLock, Optional<ExternalCompactionId> ecid) {

    context.getAmple().putGcCandidates(extent.tableId(), datafilesToDelete);

    TabletMutator tablet = context.getAmple().mutateTablet(extent);

    datafilesToDelete.forEach(tablet::deleteFile);
    scanFiles.forEach(tablet::putScan);

    if (path.isPresent()) {
      tablet.putFile(path.get(), size);
    }

    if (compactionId != null) {
      tablet.putCompactionId(compactionId);
    }

    updateLastForCompactionMode(context, tablet, lastLocation, address, zooLock);

    if (ecid.isPresent()) {
      tablet.deleteExternalCompaction(ecid.get());
    }

    tablet.putZooLock(zooLock);

    tablet.mutate();
  }

  /**
   * Update tablet file data from flush. Returns a StoredTabletFile if there are data entries.
   */
  public static Optional<StoredTabletFile> updateTabletDataFile(ServerContext context,
      KeyExtent extent, TabletFile newDatafile, DataFileValue dfv, MetadataTime time,
      String address, ServiceLock zooLock, Set<String> unusedWalLogs, Location lastLocation,
      long flushId) {

    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    // if there are no entries, the path doesn't get stored in metadata table, only the flush ID
    Optional<StoredTabletFile> newFile = Optional.empty();

    // if entries are present, write to path to metadata table
    if (dfv.getNumEntries() > 0) {
      tablet.putFile(newDatafile, dfv);
      tablet.putTime(time);
      newFile = Optional.of(newDatafile.insert());

      updateLastForCompactionMode(context, tablet, lastLocation, address, zooLock);
    }
    tablet.putFlushId(flushId);

    unusedWalLogs.forEach(tablet::deleteWal);

    tablet.putZooLock(zooLock);

    tablet.mutate();
    return newFile;
  }

  /**
   * Update the last location if the location mode is "assignment". This will delete the previous
   * last location if needed and set the new last location
   *
   * @param context The server context
   * @param ample The metadata persistence layer
   * @param tabletMutator The mutator being built
   * @param extent The tablet extent
   * @param location The new location
   */
  public static void updateLastForAssignmentMode(ClientContext context, Ample ample,
      Ample.TabletMutator tabletMutator, KeyExtent extent, TServerInstance location) {
    // if the location mode is assignment, then preserve the current location in the last
    // location value
    if ("assignment".equals(context.getConfiguration().get(Property.TSERV_LAST_LOCATION_MODE))) {
      TabletMetadata lastMetadata = ample.readTablet(extent, TabletMetadata.ColumnType.LAST);
      Location lastLocation = (lastMetadata == null ? null : lastMetadata.getLast());
      ManagerMetadataUtil.updateLocation(tabletMutator, lastLocation, Location.last(location));
    }
  }

  /**
   * Update the last location if the location mode is "compaction". This will delete the previous
   * last location if needed and set the new last location
   *
   * @param context The server context
   * @param tabletMutator The mutator being built
   * @param lastLocation The last location
   * @param address The server address
   * @param zooLock The zookeeper lock
   */
  public static void updateLastForCompactionMode(ClientContext context, TabletMutator tabletMutator,
      Location lastLocation, String address, ServiceLock zooLock) {
    // if the location mode is 'compaction', then preserve the current compaction location in the
    // last location value
    if ("compaction".equals(context.getConfiguration().get(Property.TSERV_LAST_LOCATION_MODE))) {
      Location newLocation = Location.last(getTServerInstance(address, zooLock));
      updateLocation(tabletMutator, lastLocation, newLocation);
    }
  }

  /**
   * Update the location, deleting the previous location if needed
   *
   * @param tabletMutator The mutator being built
   * @param previousLocation The location (may be null)
   * @param newLocation The new location
   */
  private static void updateLocation(TabletMutator tabletMutator, Location previousLocation,
      Location newLocation) {
    if (previousLocation != null) {
      if (!previousLocation.equals(newLocation)) {
        tabletMutator.deleteLocation(previousLocation);
        tabletMutator.putLocation(newLocation);
      }
    } else {
      tabletMutator.putLocation(newLocation);
    }
  }
}

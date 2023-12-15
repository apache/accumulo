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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ManagerMetadataUtil {

  private static final Logger log = LoggerFactory.getLogger(ManagerMetadataUtil.class);
  @SuppressWarnings("deprecation")
  private static final Property DEPRECATED_TSERV_LAST_LOCATION_MODE_PROPERTY =
      Property.TSERV_LAST_LOCATION_MODE;

  public static void addNewTablet(ServerContext context, KeyExtent extent, String dirName,
      TServerInstance tServerInstance, Map<StoredTabletFile,DataFileValue> datafileSizes,
      Map<Long,? extends Collection<ReferencedTabletFile>> bulkLoadedFiles, MetadataTime time,
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

    datafileSizes.forEach((key, value) -> tablet.putFile(key.getTabletFile(), value));

    for (Entry<Long,? extends Collection<ReferencedTabletFile>> entry : bulkLoadedFiles
        .entrySet()) {
      for (ReferencedTabletFile ref : entry.getValue()) {
        tablet.putBulkFile(ref, entry.getKey());
      }
    }

    tablet.mutate();
  }

  public static KeyExtent fixSplit(ServerContext context, TabletMetadata meta, ServiceLock lock)
      throws AccumuloException {
    log.info("Incomplete split {} attempting to fix", meta.getExtent());

    if (meta.getSplitRatio() == null) {
      throw new IllegalArgumentException(
          "Metadata entry does not have split ratio (" + meta.getExtent() + ")");
    }

    if (meta.getTime() == null) {
      throw new IllegalArgumentException(
          "Metadata entry does not have time (" + meta.getExtent() + ")");
    }

    return fixSplit(context, meta.getTableId(), meta.getExtent().toMetaRow(), meta.getPrevEndRow(),
        meta.getOldPrevEndRow(), meta.getSplitRatio(), lock);
  }

  private static KeyExtent fixSplit(ServerContext context, TableId tableId, Text metadataEntry,
      Text metadataPrevEndRow, Text oper, double splitRatio, ServiceLock lock)
      throws AccumuloException {
    if (metadataPrevEndRow == null) {
      // something is wrong, this should not happen... if a tablet is split, it will always have a
      // prev end row....
      throw new AccumuloException(
          "Split tablet does not have prev end row, something is amiss, extent = " + metadataEntry);
    }

    // check to see if prev tablet exist in metadata tablet
    Key prevRowKey = new Key(new Text(TabletsSection.encodeRow(tableId, metadataPrevEndRow)));

    try (ScannerImpl scanner2 = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY)) {
      scanner2.setRange(new Range(prevRowKey, prevRowKey.followingKey(PartialKey.ROW)));

      if (scanner2.iterator().hasNext()) {
        log.info("Finishing incomplete split {} {}", metadataEntry, metadataPrevEndRow);

        List<StoredTabletFile> highDatafilesToRemove = new ArrayList<>();

        SortedMap<StoredTabletFile,DataFileValue> origDatafileSizes = new TreeMap<>();
        SortedMap<StoredTabletFile,DataFileValue> highDatafileSizes = new TreeMap<>();
        SortedMap<StoredTabletFile,DataFileValue> lowDatafileSizes = new TreeMap<>();

        Key rowKey = new Key(metadataEntry);
        try (Scanner scanner3 = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY)) {

          scanner3.fetchColumnFamily(DataFileColumnFamily.NAME);
          scanner3.setRange(new Range(rowKey, rowKey.followingKey(PartialKey.ROW)));

          for (Entry<Key,Value> entry : scanner3) {
            if (entry.getKey().compareColumnFamily(DataFileColumnFamily.NAME) == 0) {
              StoredTabletFile stf =
                  new StoredTabletFile(entry.getKey().getColumnQualifierData().toString());
              origDatafileSizes.put(stf, new DataFileValue(entry.getValue().get()));
            }
          }
        }

        MetadataTableUtil.splitDatafiles(metadataPrevEndRow, splitRatio, new HashMap<>(),
            origDatafileSizes, lowDatafileSizes, highDatafileSizes, highDatafilesToRemove);

        MetadataTableUtil.finishSplit(metadataEntry, highDatafileSizes, highDatafilesToRemove,
            context, lock);

        return KeyExtent.fromMetaRow(rowKey.getRow(), metadataPrevEndRow);
      } else {
        log.info("Rolling back incomplete split {} {}", metadataEntry, metadataPrevEndRow);
        MetadataTableUtil.rollBackSplit(metadataEntry, oper, context, lock);
        return KeyExtent.fromMetaRow(metadataEntry, oper);
      }
    }
  }

  public static void replaceDatafiles(ServerContext context, KeyExtent extent,
      Set<StoredTabletFile> datafilesToDelete, Set<StoredTabletFile> scanFiles,
      Optional<StoredTabletFile> path, Long compactionId, DataFileValue size,
      TServerInstance tServerInstance, Location lastLocation, ServiceLock zooLock,
      Optional<ExternalCompactionId> ecid) {

    // Write candidates before the mutation to ensure that a process failure after a mutation would
    // not affect candidate creation
    context.getAmple().putGcCandidates(extent.tableId(), datafilesToDelete);

    TabletMutator tablet = context.getAmple().mutateTablet(extent);

    datafilesToDelete.forEach(tablet::deleteFile);
    scanFiles.forEach(tablet::putScan);

    if (path.isPresent()) {
      tablet.putFile(path.orElseThrow(), size);
    }

    if (compactionId != null) {
      tablet.putCompactionId(compactionId);
    }

    updateLastForCompactionMode(context, tablet, lastLocation, tServerInstance);

    if (ecid.isPresent()) {
      tablet.deleteExternalCompaction(ecid.orElseThrow());
    }

    tablet.putZooLock(zooLock);

    tablet.mutate();
    // Write candidates again to avoid a possible race condition when removing InUse candidates
    context.getAmple().putGcCandidates(extent.tableId(), datafilesToDelete);
  }

  /**
   * Update tablet file data from flush. Returns a StoredTabletFile if there are data entries.
   */
  public static Optional<StoredTabletFile> updateTabletDataFile(ServerContext context,
      KeyExtent extent, ReferencedTabletFile newDatafile, DataFileValue dfv, MetadataTime time,
      TServerInstance tServerInstance, ServiceLock zooLock, Set<LogEntry> unusedWalLogs,
      Location lastLocation, long flushId) {

    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    // if there are no entries, the path doesn't get stored in metadata table, only the flush ID
    Optional<StoredTabletFile> newFile = Optional.empty();

    // if entries are present, write to path to metadata table
    if (dfv.getNumEntries() > 0) {
      tablet.putFile(newDatafile, dfv);
      tablet.putTime(time);
      newFile = Optional.of(newDatafile.insert());

      updateLastForCompactionMode(context, tablet, lastLocation, tServerInstance);
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
   * @param tabletMutator The mutator being built
   * @param location The new location
   * @param lastLocation The previous last location, which may be null
   */
  public static void updateLastForAssignmentMode(ClientContext context,
      Ample.TabletMutator tabletMutator, TServerInstance location, Location lastLocation) {
    Preconditions.checkArgument(
        lastLocation == null || lastLocation.getType() == TabletMetadata.LocationType.LAST);

    // if the location mode is assignment, then preserve the current location in the last
    // location value
    if ("assignment"
        .equals(context.getConfiguration().get(DEPRECATED_TSERV_LAST_LOCATION_MODE_PROPERTY))) {
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
   * @param tServerInstance The server address
   */
  public static void updateLastForCompactionMode(ClientContext context, TabletMutator tabletMutator,
      Location lastLocation, TServerInstance tServerInstance) {
    // if the location mode is 'compaction', then preserve the current compaction location in the
    // last location value
    if ("compaction"
        .equals(context.getConfiguration().get(DEPRECATED_TSERV_LAST_LOCATION_MODE_PROPERTY))) {
      Location newLocation = Location.last(tServerInstance);
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

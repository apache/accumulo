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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType.CURRENT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.accumulo.core.clientImpl.bulk.Bulk;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * Make asynchronous load calls to each overlapping Tablet. This RepO does its work on the isReady
 * and will return a linear sleep value based on the largest number of Tablets on a TabletServer.
 */
class LoadFiles extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(LoadFiles.class);

  private final BulkInfo bulkInfo;

  public LoadFiles(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  @Override
  public long isReady(FateId fateId, Manager manager) throws Exception {
    if (manager.onlineTabletServers().isEmpty()) {
      log.warn("There are no tablet server to process bulkDir import, waiting (fateId = " + fateId
          + ")");
      return 100;
    }
    VolumeManager fs = manager.getVolumeManager();
    final Path bulkDir = new Path(bulkInfo.bulkDir);
    manager.updateBulkImportStatus(bulkInfo.sourceDir, BulkImportState.LOADING);
    try (LoadMappingIterator lmi =
        BulkSerialize.getUpdatedLoadMapping(bulkDir.toString(), bulkInfo.tableId, fs::open)) {
      return loadFiles(bulkInfo.tableId, bulkDir, lmi, manager, fateId);
    }
  }

  @Override
  public Repo<Manager> call(final FateId fateId, final Manager manager) {
    return new RefreshTablets(bulkInfo);
  }

  private static class Loader {
    protected Path bulkDir;
    protected Manager manager;
    protected FateId fateId;
    protected boolean setTime;
    Ample.ConditionalTabletsMutator conditionalMutator;
    private Map<KeyExtent,List<TabletFile>> loadingFiles;

    private long skipped = 0;
    private long pauseLimit;

    void start(Path bulkDir, Manager manager, TableId tableId, FateId fateId, boolean setTime)
        throws Exception {
      this.bulkDir = bulkDir;
      this.manager = manager;
      this.fateId = fateId;
      this.setTime = setTime;
      conditionalMutator = manager.getContext().getAmple().conditionallyMutateTablets();
      this.skipped = 0;
      this.loadingFiles = new HashMap<>();
      this.pauseLimit =
          manager.getContext().getTableConfiguration(tableId).getCount(Property.TABLE_FILE_PAUSE);
    }

    void load(List<TabletMetadata> tablets, Files files) {

      Map<ReferencedTabletFile,Bulk.FileInfo> toLoad = new HashMap<>();
      for (var fileInfo : files) {
        toLoad.put(new ReferencedTabletFile(new Path(bulkDir, fileInfo.getFileName())), fileInfo);
      }

      // remove any tablets that already have loaded flags
      tablets = tablets.stream().filter(tabletMeta -> {
        Set<ReferencedTabletFile> loaded = tabletMeta.getLoaded().keySet().stream()
            .map(StoredTabletFile::getTabletFile).collect(Collectors.toSet());
        boolean containsAll = loaded.containsAll(toLoad.keySet());
        // The tablet should either contain all loaded files or none. It should never contain a
        // subset. Loaded files are written in single mutation to accumulo, either all changes in a
        // mutation should go through or none.
        Preconditions.checkState(containsAll || Collections.disjoint(loaded, toLoad.keySet()),
            "Tablet %s has a subset of loaded files %s %s", tabletMeta.getExtent(), loaded,
            toLoad.keySet());
        if (containsAll) {
          log.trace("{} tablet {} has already loaded all files, nothing to do", fateId,
              tabletMeta.getExtent());
        }
        return !containsAll;
      }).collect(Collectors.toList());

      // timestamps from tablets that are hosted on a tablet server
      Map<KeyExtent,Long> hostedTimestamps;
      if (setTime) {
        hostedTimestamps = allocateTimestamps(tablets, toLoad.size());
        hostedTimestamps.forEach((e, t) -> {
          log.trace("{} allocated timestamp {} {}", fateId, e, t);
        });
      } else {
        hostedTimestamps = Map.of();
      }

      List<ColumnType> rsc = new ArrayList<>();
      if (setTime) {
        rsc.add(TIME);
      }

      ColumnType[] requireSameCols = rsc.toArray(new ColumnType[0]);

      for (TabletMetadata tablet : tablets) {
        // Skip any tablets at the beginning of the loop before any work is done.
        if (setTime && tablet.getLocation() != null
            && !hostedTimestamps.containsKey(tablet.getExtent())) {
          skipped++;
          log.debug("{} tablet {} did not have a timestamp allocated, will retry later", fateId,
              tablet.getExtent());
          continue;
        }
        if (pauseLimit > 0 && tablet.getFiles().size() > pauseLimit) {
          skipped++;
          log.debug(
              "{} tablet {} has {} files which exceeds the pause limit of {}, not bulk importing and will retry later",
              fateId, tablet.getExtent(), tablet.getFiles().size(), pauseLimit);
          continue;
        }

        Map<ReferencedTabletFile,DataFileValue> filesToLoad = new HashMap<>();

        var tabletTime = TabletTime.getInstance(tablet.getTime());

        Long fileTime = null;
        if (setTime) {
          if (tablet.getLocation() == null) {
            fileTime = tabletTime.getAndUpdateTime();
          } else {
            fileTime = hostedTimestamps.get(tablet.getExtent());
            tabletTime.updateTimeIfGreater(fileTime);
          }
        }

        for (var entry : toLoad.entrySet()) {
          ReferencedTabletFile refTabFile = entry.getKey();
          Bulk.FileInfo fileInfo = entry.getValue();

          DataFileValue dfv;

          if (setTime) {
            // This should always be set outside the loop when setTime is true and should not be
            // null at this point
            Preconditions.checkState(fileTime != null);
            dfv =
                new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries(), fileTime);
          } else {
            dfv = new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries());
          }

          filesToLoad.put(refTabFile, dfv);
        }

        var tabletMutator = conditionalMutator.mutateTablet(tablet.getExtent())
            .requireAbsentOperation().requireAbsentLoaded(filesToLoad.keySet())
            .requireSame(tablet, LOCATION, requireSameCols);

        if (pauseLimit > 0) {
          tabletMutator.requireLessOrEqualsFiles(pauseLimit);
        }

        filesToLoad.forEach((f, v) -> {
          tabletMutator.putBulkFile(f, fateId);
          tabletMutator.putFile(f, v);
        });

        if (setTime) {
          tabletMutator.putTime(tabletTime.getMetadataTime());
        }

        // Hang on to loaded files for logging purposes in the case where the update is success.
        Preconditions.checkState(
            loadingFiles.put(tablet.getExtent(), List.copyOf(filesToLoad.keySet())) == null);

        tabletMutator.submit(tm -> false, () -> "bulk load files " + fateId);
      }
    }

    private Map<KeyExtent,Long> allocateTimestamps(List<TabletMetadata> tablets, int numStamps) {

      Map<HostAndPort,List<TKeyExtent>> serversToAsk = new HashMap<>();

      Map<KeyExtent,Long> allTimestamps = new HashMap<>();

      for (var tablet : tablets) {
        if (tablet.getLocation() != null && tablet.getLocation().getType() == CURRENT) {
          var location = tablet.getLocation().getHostAndPort();
          serversToAsk.computeIfAbsent(location, l -> new ArrayList<>())
              .add(tablet.getExtent().toThrift());
        }
      }

      for (var entry : serversToAsk.entrySet()) {
        HostAndPort server = entry.getKey();
        List<TKeyExtent> extents = entry.getValue();

        Map<KeyExtent,Long> serversTimestamps = allocateTimestamps(server, extents, numStamps);
        allTimestamps.putAll(serversTimestamps);

      }

      return allTimestamps;
    }

    private Map<KeyExtent,Long> allocateTimestamps(HostAndPort server, List<TKeyExtent> extents,
        int numStamps) {
      TabletServerClientService.Client client = null;
      var context = manager.getContext();
      try {

        log.trace("{} sending allocate timestamps request to {} for {} extents", fateId, server,
            extents.size());
        var timeInMillis =
            context.getConfiguration().getTimeInMillis(Property.MANAGER_BULK_TIMEOUT);
        client =
            ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, server, context, timeInMillis);

        var timestamps =
            client.allocateTimestamps(TraceUtil.traceInfo(), context.rpcCreds(), extents);

        log.trace("{} allocate timestamps request to {} returned {} timestamps", fateId, server,
            timestamps.size());

        var converted = new HashMap<KeyExtent,Long>();
        timestamps.forEach((k, v) -> converted.put(KeyExtent.fromThrift(k), v));
        return converted;
      } catch (TException ex) {
        log.debug("rpc failed server: " + server + ", " + fateId + " " + ex.getMessage(), ex);
        // return an empty map, should retry later
        return Map.of();
      } finally {
        ThriftUtil.returnClient(client, context);
      }

    }

    long finish() {
      var results = conditionalMutator.process();

      AtomicBoolean seenFailure = new AtomicBoolean(false);
      results.forEach((extent, condResult) -> {
        if (condResult.getStatus() == Status.ACCEPTED) {
          loadingFiles.get(extent).forEach(file -> TabletLogger.bulkImported(extent, file));
          // Trigger a check for compaction now that new files were added via bulk load
          manager.getEventCoordinator().event(extent, "Bulk load completed on tablet %s", extent);
        } else {
          seenFailure.set(true);
          var metadata = condResult.readMetadata();
          if (metadata == null) {
            log.debug("Tablet update failed, tablet is gone {} {} {}", fateId, extent,
                condResult.getStatus());
          } else {
            log.debug("Tablet update failed {} {} {} {} {} {}", fateId, extent,
                condResult.getStatus(), metadata.getOperationId(), metadata.getLocation(),
                metadata.getLoaded());
          }
        }
      });

      if (seenFailure.get() || skipped != 0) {
        return 1000;
      } else {
        return 0;
      }
    }
  }

  /**
   * Make asynchronous load calls to each overlapping Tablet in the bulk mapping. Return a sleep
   * time to isReady based on a factor of the TabletServer with the most Tablets. This method will
   * scan the metadata table getting Tablet range and location information. It will return 0 when
   * all files have been loaded.
   */
  private long loadFiles(TableId tableId, Path bulkDir, LoadMappingIterator loadMapIter,
      Manager manager, FateId fateId) throws Exception {
    PeekingIterator<Map.Entry<KeyExtent,Bulk.Files>> lmi = new PeekingIterator<>(loadMapIter);
    Map.Entry<KeyExtent,Bulk.Files> loadMapEntry = lmi.peek();

    Text startRow = loadMapEntry.getKey().prevEndRow();

    String fmtTid = fateId.getTxUUIDStr();
    log.trace("{}: Starting bulk load at row: {}", fmtTid, startRow);

    Loader loader = new Loader();
    long t1;
    loader.start(bulkDir, manager, tableId, fateId, bulkInfo.setTime);

    List<ColumnType> fetchCols = new ArrayList<>(List.of(PREV_ROW, LOCATION, LOADED, TIME));
    if (loader.pauseLimit > 0) {
      fetchCols.add(FILES);
    }

    try (TabletsMetadata tabletsMetadata =
        TabletsMetadata.builder(manager.getContext()).forTable(tableId).overlapping(startRow, null)
            .checkConsistency().fetch(fetchCols.toArray(new ColumnType[0])).build()) {

      // The tablet iterator and load mapping iterator are both iterating over data that is sorted
      // in the same way. The two iterators are each independently advanced to find common points in
      // the sorted data.
      var tabletIter = tabletsMetadata.iterator();

      t1 = System.currentTimeMillis();
      while (lmi.hasNext()) {
        loadMapEntry = lmi.next();
        List<TabletMetadata> tablets =
            findOverlappingTablets(fmtTid, loadMapEntry.getKey(), tabletIter);
        loader.load(tablets, loadMapEntry.getValue());
      }
    }

    log.trace("{}: Completed Finding Overlapping Tablets", fmtTid);

    long sleepTime = loader.finish();
    if (sleepTime > 0) {
      log.trace("{}: Tablet Max Sleep is {}", fmtTid, sleepTime);
      long scanTime = Math.min(System.currentTimeMillis() - t1, 30_000);
      log.trace("{}: Scan time is {}", fmtTid, scanTime);
      sleepTime = Math.max(sleepTime, scanTime * 2);
    }
    log.trace("{}: Sleeping for {}ms", fmtTid, sleepTime);
    return sleepTime;
  }

  private static final Comparator<Text> PREV_COMP = Comparator.nullsFirst(Text::compareTo);
  private static final Comparator<Text> END_COMP = Comparator.nullsLast(Text::compareTo);

  /**
   * Find all the tablets within the provided bulk load mapping range.
   */
  private List<TabletMetadata> findOverlappingTablets(String fmtTid, KeyExtent loadRange,
      Iterator<TabletMetadata> tabletIter) {

    TabletMetadata currTablet = null;

    try {

      List<TabletMetadata> tablets = new ArrayList<>();
      currTablet = tabletIter.next();
      log.trace("{}: Finding Overlapping Tablets for row: {}", fmtTid, currTablet.getExtent());

      int cmp;

      // skip tablets until we find the prevEndRow of loadRange
      while ((cmp = PREV_COMP.compare(currTablet.getPrevEndRow(), loadRange.prevEndRow())) < 0) {
        log.trace("{}: Skipping tablet: {}", fmtTid, currTablet.getExtent());
        currTablet = tabletIter.next();
      }

      if (cmp != 0) {
        throw new IllegalStateException(
            "Unexpected prev end row " + currTablet.getExtent() + " " + loadRange);
      }

      // we have found the first tablet in the range, add it to the list
      tablets.add(currTablet);

      // find the remaining tablets within the loadRange by
      // adding tablets to the list until the endRow matches the loadRange
      while ((cmp = END_COMP.compare(currTablet.getEndRow(), loadRange.endRow())) < 0) {
        currTablet = tabletIter.next();
        log.trace("{}: Adding tablet: {} to overlapping list", fmtTid, currTablet.getExtent());
        tablets.add(currTablet);
      }

      if (cmp != 0) {
        throw new IllegalStateException("Unexpected end row " + currTablet + " " + loadRange);
      }

      return tablets;
    } catch (NoSuchElementException e) {
      NoSuchElementException ne2 = new NoSuchElementException(
          "Failed to find overlapping tablets " + currTablet + " " + loadRange);
      ne2.initCause(e);
      throw ne2;
    }
  }
}

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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType.CURRENT;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
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
import org.apache.accumulo.core.manager.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
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

    private long skipped = 0;

    void start(Path bulkDir, Manager manager, FateId fateId, boolean setTime) throws Exception {
      this.bulkDir = bulkDir;
      this.manager = manager;
      this.fateId = fateId;
      this.setTime = setTime;
      conditionalMutator = manager.getContext().getAmple().conditionallyMutateTablets();
      this.skipped = 0;
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
        return !loaded.containsAll(toLoad.keySet());
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

      for (TabletMetadata tablet : tablets) {
        if (setTime && tablet.getLocation() != null
            && !hostedTimestamps.containsKey(tablet.getExtent())) {
          skipped++;
          log.debug("{} tablet {} did not have a timestamp allocated, will retry later", fateId,
              tablet.getExtent());
          continue;
        }

        Map<ReferencedTabletFile,DataFileValue> filesToLoad = new HashMap<>();

        var tabletTime = TabletTime.getInstance(tablet.getTime());

        int timeOffset = 0;

        for (var entry : toLoad.entrySet()) {
          ReferencedTabletFile refTabFile = entry.getKey();
          Bulk.FileInfo fileInfo = entry.getValue();

          DataFileValue dfv;

          if (setTime) {
            if (tablet.getLocation() == null) {
              dfv = new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries(),
                  tabletTime.getAndUpdateTime());
            } else {
              dfv = new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries(),
                  hostedTimestamps.get(tablet.getExtent()) + timeOffset);
              timeOffset++;
            }
          } else {
            dfv = new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries());
          }

          filesToLoad.put(refTabFile, dfv);

        }

        // remove any files that were already loaded
        tablet.getLoaded().keySet().forEach(stf -> {
          filesToLoad.keySet().remove(stf.getTabletFile());
        });

        if (!filesToLoad.isEmpty()) {
          var tabletMutator = conditionalMutator.mutateTablet(tablet.getExtent())
              .requireAbsentOperation().requireSame(tablet, LOADED, TIME, LOCATION);

          filesToLoad.forEach((f, v) -> {
            // ELASTICITY_TODO DEFERRED - ISSUE 4044
            tabletMutator.putBulkFile(f, fateId.getTid());
            tabletMutator.putFile(f, v);
          });

          if (setTime && tablet.getLocation() == null) {
            tabletMutator.putTime(tabletTime.getMetadataTime());
          }

          tabletMutator.submit(tm -> false);
        }
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

        var timestamps = client.allocateTimestamps(TraceUtil.traceInfo(), context.rpcCreds(),
            extents, numStamps);

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

      boolean allDone =
          results.values().stream().allMatch(result -> result.getStatus() == Status.ACCEPTED)
              && skipped == 0;

      long sleepTime = 0;
      if (!allDone) {
        sleepTime = 1000;

        results.forEach((extent, condResult) -> {
          if (condResult.getStatus() != Status.ACCEPTED) {
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
      }

      return sleepTime;
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

    Loader loader = new Loader();
    long t1;
    loader.start(bulkDir, manager, fateId, bulkInfo.setTime);
    try (TabletsMetadata tabletsMetadata =
        TabletsMetadata.builder(manager.getContext()).forTable(tableId).overlapping(startRow, null)
            .checkConsistency().fetch(PREV_ROW, LOCATION, LOADED, TIME).build()) {

      t1 = System.currentTimeMillis();
      while (lmi.hasNext()) {
        loadMapEntry = lmi.next();
        List<TabletMetadata> tablets =
            findOverlappingTablets(loadMapEntry.getKey(), tabletsMetadata.iterator());
        loader.load(tablets, loadMapEntry.getValue());
      }
    }

    long sleepTime = loader.finish();
    if (sleepTime > 0) {
      long scanTime = Math.min(System.currentTimeMillis() - t1, 30000);
      sleepTime = Math.max(sleepTime, scanTime * 2);
    }
    return sleepTime;
  }

  private static final Comparator<Text> PREV_COMP = Comparator.nullsFirst(Text::compareTo);
  private static final Comparator<Text> END_COMP = Comparator.nullsLast(Text::compareTo);

  /**
   * Find all the tablets within the provided bulk load mapping range.
   */
  private List<TabletMetadata> findOverlappingTablets(KeyExtent loadRange,
      Iterator<TabletMetadata> tabletIter) {

    TabletMetadata currTablet = null;

    try {

      List<TabletMetadata> tablets = new ArrayList<>();
      currTablet = tabletIter.next();

      int cmp;

      // skip tablets until we find the prevEndRow of loadRange
      while ((cmp = PREV_COMP.compare(currTablet.getPrevEndRow(), loadRange.prevEndRow())) < 0) {
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

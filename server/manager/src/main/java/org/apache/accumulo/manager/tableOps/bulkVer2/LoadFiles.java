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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.clientImpl.bulk.Bulk;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Make asynchronous load calls to each overlapping Tablet. This RepO does its work on the isReady
 * and will return a linear sleep value based on the largest number of Tablets on a TabletServer.
 */
class LoadFiles extends ManagerRepo {

  // visible for testing
  interface TabletsMetadataFactory {

    TabletsMetadata newTabletsMetadata(Text startRow);

  }

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(LoadFiles.class);

  private final BulkInfo bulkInfo;

  public LoadFiles(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {
    log.info("Starting bulk import for {} (tid = {})", bulkInfo.sourceDir, FateTxId.formatTid(tid));
    if (manager.onlineTabletServers().isEmpty()) {
      log.warn("There are no tablet server to process bulkDir import, waiting (tid = "
          + FateTxId.formatTid(tid) + ")");
      return 100;
    }
    VolumeManager fs = manager.getVolumeManager();
    final Path bulkDir = new Path(bulkInfo.bulkDir);
    manager.updateBulkImportStatus(bulkInfo.sourceDir, BulkImportState.LOADING);
    try (LoadMappingIterator lmi =
        BulkSerialize.getUpdatedLoadMapping(bulkDir.toString(), bulkInfo.tableId, fs::open)) {

      Loader loader;
      if (bulkInfo.tableState == TableState.ONLINE) {
        loader = new OnlineLoader();
      } else {
        loader = new OfflineLoader();
      }

      TabletsMetadataFactory tmf = (startRow) -> TabletsMetadata.builder(manager.getContext())
          .forTable(bulkInfo.tableId).overlapping(startRow, null).checkConsistency()
          .fetch(PREV_ROW, LOCATION, LOADED).build();

      int skip = manager.getContext().getTableConfiguration(bulkInfo.tableId)
          .getCount(Property.TABLE_BULK_SKIP_THRESHOLD);
      return loadFiles(loader, bulkInfo, bulkDir, lmi, tmf, manager, tid, skip);
    }
  }

  @Override
  public Repo<Manager> call(final long tid, final Manager manager) {
    if (bulkInfo.tableState == TableState.ONLINE) {
      return new CompleteBulkImport(bulkInfo);
    } else {
      return new CleanUpBulkImport(bulkInfo);
    }
  }

  // visible for testing
  public abstract static class Loader {
    protected Path bulkDir;
    protected Manager manager;
    protected long tid;
    protected boolean setTime;

    void start(Path bulkDir, Manager manager, long tid, boolean setTime) throws Exception {
      this.bulkDir = bulkDir;
      this.manager = manager;
      this.tid = tid;
      this.setTime = setTime;
    }

    abstract void load(List<TabletMetadata> tablets, Files files) throws Exception;

    abstract long finish() throws Exception;
  }

  private static class OnlineLoader extends Loader {

    long timeInMillis;
    String fmtTid;
    int locationLess = 0;

    // track how many tablets were sent load messages per tablet server
    MapCounter<HostAndPort> loadMsgs;

    // Each RPC to a tablet server needs to check in zookeeper to see if the transaction is still
    // active. The purpose of this map is to group load request by tablet servers inorder to do less
    // RPCs. Less RPCs will result in less calls to Zookeeper.
    Map<HostAndPort,Map<TKeyExtent,Map<String,MapFileInfo>>> loadQueue;
    private int queuedDataSize = 0;

    @Override
    void start(Path bulkDir, Manager manager, long tid, boolean setTime) throws Exception {
      super.start(bulkDir, manager, tid, setTime);

      timeInMillis = manager.getConfiguration().getTimeInMillis(Property.MANAGER_BULK_TIMEOUT);
      fmtTid = FateTxId.formatTid(tid);

      loadMsgs = new MapCounter<>();

      loadQueue = new HashMap<>();
    }

    private void sendQueued(int threshhold) {
      if (queuedDataSize > threshhold || threshhold == 0) {
        loadQueue.forEach((server, tabletFiles) -> {

          if (log.isTraceEnabled()) {
            log.trace("{} asking {} to bulk import {} files for {} tablets", fmtTid, server,
                tabletFiles.values().stream().mapToInt(Map::size).sum(), tabletFiles.size());
          }

          TabletClientService.Client client = null;
          try {
            client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, server,
                manager.getContext(), timeInMillis);
            client.loadFiles(TraceUtil.traceInfo(), manager.getContext().rpcCreds(), tid,
                bulkDir.toString(), tabletFiles, setTime);
          } catch (TException ex) {
            log.debug("rpc failed server: " + server + ", " + fmtTid + " " + ex.getMessage(), ex);
          } finally {
            ThriftUtil.returnClient(client, manager.getContext());
          }
        });

        loadQueue.clear();
        queuedDataSize = 0;
      }
    }

    private void addToQueue(HostAndPort server, KeyExtent extent,
        Map<String,MapFileInfo> thriftImports) {
      if (!thriftImports.isEmpty()) {
        loadMsgs.increment(server, 1);

        Map<String,MapFileInfo> prev = loadQueue.computeIfAbsent(server, k -> new HashMap<>())
            .putIfAbsent(extent.toThrift(), thriftImports);

        Preconditions.checkState(prev == null, "Unexpectedly saw extent %s twice", extent);

        // keep a very rough estimate of how much is memory so we can send if over a few megs is
        // buffered
        queuedDataSize += thriftImports.keySet().stream().mapToInt(String::length).sum()
            + server.getHost().length() + 4 + thriftImports.size() * 32;
      }
    }

    @Override
    void load(List<TabletMetadata> tablets, Files files) {
      for (TabletMetadata tablet : tablets) {
        // send files to tablet sever
        // ideally there should only be one tablet location to send all the files

        Location location = tablet.getLocation();
        HostAndPort server = null;
        if (location == null) {
          locationLess++;
          continue;
        } else {
          server = location.getHostAndPort();
        }

        Set<TabletFile> loadedFiles = tablet.getLoaded().keySet();

        Map<String,MapFileInfo> thriftImports = new HashMap<>();

        for (final Bulk.FileInfo fileInfo : files) {
          Path fullPath = new Path(bulkDir, fileInfo.getFileName());
          TabletFile bulkFile = new TabletFile(fullPath);

          if (!loadedFiles.contains(bulkFile)) {
            thriftImports.put(fileInfo.getFileName(), new MapFileInfo(fileInfo.getEstFileSize()));
          }
        }

        addToQueue(server, tablet.getExtent(), thriftImports);
      }

      sendQueued(4 * 1024 * 1024);
    }

    @Override
    long finish() {

      sendQueued(0);

      long sleepTime = 0;
      if (loadMsgs.size() > 0) {
        // find which tablet server had the most load messages sent to it and sleep 13ms for each
        // load message
        sleepTime = loadMsgs.max() * 13;
      }

      if (locationLess > 0) {
        sleepTime = Math.max(Math.max(100L, locationLess), sleepTime);
      }

      return sleepTime;
    }

  }

  private static class OfflineLoader extends Loader {

    BatchWriter bw;

    // track how many tablets were sent load messages per tablet server
    MapCounter<HostAndPort> unloadingTablets;

    @Override
    void start(Path bulkDir, Manager manager, long tid, boolean setTime) throws Exception {
      Preconditions.checkArgument(!setTime);
      super.start(bulkDir, manager, tid, setTime);
      bw = manager.getContext().createBatchWriter(MetadataTable.NAME);
      unloadingTablets = new MapCounter<>();
    }

    @Override
    void load(List<TabletMetadata> tablets, Files files) throws MutationsRejectedException {
      byte[] fam = TextUtil.getBytes(DataFileColumnFamily.NAME);

      for (TabletMetadata tablet : tablets) {
        if (tablet.getLocation() != null) {
          unloadingTablets.increment(tablet.getLocation().getHostAndPort(), 1L);
          continue;
        }

        Mutation mutation = new Mutation(tablet.getExtent().toMetaRow());

        for (final Bulk.FileInfo fileInfo : files) {
          String fullPath = new Path(bulkDir, fileInfo.getFileName()).toString();
          byte[] val =
              new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries()).encode();
          mutation.put(fam, fullPath.getBytes(UTF_8), val);
        }

        bw.addMutation(mutation);
      }
    }

    @Override
    long finish() throws Exception {

      bw.close();

      long sleepTime = 0;
      if (unloadingTablets.size() > 0) {
        // find which tablet server had the most tablets to unload and sleep 13ms for each tablet
        sleepTime = unloadingTablets.max() * 13;
      }

      return sleepTime;
    }
  }

  /**
   * Stats for the loadFiles method. Helps track wasted time and iterations.
   */
  static class ImportTimingStats {
    Duration totalWastedTime = Duration.ZERO;
    long wastedIterations = 0;
    long tabletCount = 0;
    long callCount = 0;
  }

  /**
   * Make asynchronous load calls to each overlapping Tablet in the bulk mapping. Return a sleep
   * time to isReady based on a factor of the TabletServer with the most Tablets. This method will
   * scan the metadata table getting Tablet range and location information. It will return 0 when
   * all files have been loaded.
   */
  // visible for testing
  static long loadFiles(Loader loader, BulkInfo bulkInfo, Path bulkDir,
      LoadMappingIterator loadMapIter, TabletsMetadataFactory factory, Manager manager, long tid,
      int skipDistance) throws Exception {
    PeekingIterator<Map.Entry<KeyExtent,Bulk.Files>> lmi = new PeekingIterator<>(loadMapIter);
    Map.Entry<KeyExtent,Bulk.Files> loadMapEntry = lmi.peek();

    Text startRow = loadMapEntry.getKey().prevEndRow();

    String fmtTid = FateTxId.formatTid(tid);
    log.trace("{}: Starting bulk load at row: {}", fmtTid, startRow);

    loader.start(bulkDir, manager, tid, bulkInfo.setTime);

    ImportTimingStats importTimingStats = new ImportTimingStats();
    Timer timer = Timer.startNew();

    TabletsMetadata tabletsMetadata = factory.newTabletsMetadata(startRow);
    try {
      PeekingIterator<TabletMetadata> pi = new PeekingIterator<>(tabletsMetadata.iterator());
      while (lmi.hasNext()) {
        loadMapEntry = lmi.next();
        // If the user set the TABLE_BULK_SKIP_THRESHOLD property, then only look
        // at the next skipDistance tablets before recreating the iterator
        if (skipDistance > 0) {
          final KeyExtent loadMapKey = loadMapEntry.getKey();
          if (!pi.advanceTo(
              tm -> PREV_COMP.compare(tm.getPrevEndRow(), loadMapKey.prevEndRow()) >= 0,
              skipDistance)) {
            log.debug(
                "Next load mapping range {} not found in {} tablets, recreating TabletMetadata to jump ahead",
                loadMapKey.prevEndRow(), skipDistance);
            tabletsMetadata.close();
            tabletsMetadata = factory.newTabletsMetadata(loadMapKey.prevEndRow());
            pi = new PeekingIterator<>(tabletsMetadata.iterator());
          }
        }
        List<TabletMetadata> tablets =
            findOverlappingTablets(fmtTid, loadMapEntry.getKey(), pi, importTimingStats);
        loader.load(tablets, loadMapEntry.getValue());
      }
    } finally {
      tabletsMetadata.close();
    }
    Duration totalProcessingTime = timer.elapsed();

    log.trace("{}: Completed Finding Overlapping Tablets", fmtTid);

    if (importTimingStats.callCount > 0) {
      log.debug(
          "Bulk import stats for {} (tid = {}): processed {} tablets in {} calls which took {}ms ({} nanos). Skipped {} iterations which took {}ms ({} nanos) or {}% of the processing time.",
          bulkInfo.sourceDir, FateTxId.formatTid(tid), importTimingStats.tabletCount,
          importTimingStats.callCount, totalProcessingTime.toMillis(),
          totalProcessingTime.toNanos(), importTimingStats.wastedIterations,
          importTimingStats.totalWastedTime.toMillis(), importTimingStats.totalWastedTime.toNanos(),
          (importTimingStats.totalWastedTime.toNanos() * 100) / totalProcessingTime.toNanos());
    }

    long sleepTime = loader.finish();
    if (sleepTime > 0) {
      log.trace("{}: Tablet Max Sleep is {}", fmtTid, sleepTime);
      long scanTime = Math.min(totalProcessingTime.toMillis(), 30_000);
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
  // visible for testing
  static List<TabletMetadata> findOverlappingTablets(String fmtTid, KeyExtent loadRange,
      Iterator<TabletMetadata> tabletIter, ImportTimingStats importTimingStats) {

    TabletMetadata currTablet = null;

    try {

      List<TabletMetadata> tablets = new ArrayList<>();
      currTablet = tabletIter.next();
      log.trace("{}: Finding Overlapping Tablets for row: {}", fmtTid, currTablet.getExtent());

      int cmp;

      long wastedIterations = 0;
      Timer timer = Timer.startNew();

      // skip tablets until we find the prevEndRow of loadRange
      while ((cmp = PREV_COMP.compare(currTablet.getPrevEndRow(), loadRange.prevEndRow())) < 0) {
        wastedIterations++;
        log.trace("{}: Skipping tablet: {}", fmtTid, currTablet.getExtent());
        currTablet = tabletIter.next();
      }

      Duration wastedTime = timer.elapsed();

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

      importTimingStats.wastedIterations += wastedIterations;
      importTimingStats.totalWastedTime = importTimingStats.totalWastedTime.plus(wastedTime);
      importTimingStats.tabletCount += tablets.size();
      importTimingStats.callCount++;

      return tablets;
    } catch (NoSuchElementException e) {
      NoSuchElementException ne2 = new NoSuchElementException(
          "Failed to find overlapping tablets " + currTablet + " " + loadRange);
      ne2.initCause(e);
      throw ne2;
    }
  }
}

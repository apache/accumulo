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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MasterMetadataUtil {

  private static final Logger log = LoggerFactory.getLogger(MasterMetadataUtil.class);

  public static void addNewTablet(ClientContext context, KeyExtent extent, String path, TServerInstance location, Map<FileRef,DataFileValue> datafileSizes,
      Map<Long,? extends Collection<FileRef>> bulkLoadedFiles, String time, long lastFlushID, long lastCompactID, ZooLock zooLock) {
    Mutation m = extent.getPrevRowUpdateMutation();

    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(path.getBytes(UTF_8)));
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value(time.getBytes(UTF_8)));
    if (lastFlushID > 0)
      TabletsSection.ServerColumnFamily.FLUSH_COLUMN.put(m, new Value(("" + lastFlushID).getBytes()));
    if (lastCompactID > 0)
      TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(m, new Value(("" + lastCompactID).getBytes()));

    if (location != null) {
      location.putLocation(m);
      location.clearFutureLocation(m);
    }

    for (Entry<FileRef,DataFileValue> entry : datafileSizes.entrySet()) {
      m.put(DataFileColumnFamily.NAME, entry.getKey().meta(), new Value(entry.getValue().encode()));
    }

    for (Entry<Long,? extends Collection<FileRef>> entry : bulkLoadedFiles.entrySet()) {
      Value tidBytes = new Value(Long.toString(entry.getKey()).getBytes());
      for (FileRef ref : entry.getValue()) {
        m.put(TabletsSection.BulkFileColumnFamily.NAME, ref.meta(), new Value(tidBytes));
      }
    }

    MetadataTableUtil.update(context, zooLock, m, extent);
  }

  public static KeyExtent fixSplit(ClientContext context, Text metadataEntry, SortedMap<ColumnFQ,Value> columns, TServerInstance tserver, ZooLock lock)
      throws AccumuloException, IOException {
    log.info("Incomplete split {} attempting to fix", metadataEntry);

    Value oper = columns.get(TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN);

    if (columns.get(TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN) == null) {
      throw new IllegalArgumentException("Metadata entry does not have split ratio (" + metadataEntry + ")");
    }

    double splitRatio = Double.parseDouble(new String(columns.get(TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN).get(), UTF_8));

    Value prevEndRowIBW = columns.get(TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN);

    if (prevEndRowIBW == null) {
      throw new IllegalArgumentException("Metadata entry does not have prev row (" + metadataEntry + ")");
    }

    Value time = columns.get(TabletsSection.ServerColumnFamily.TIME_COLUMN);

    if (time == null) {
      throw new IllegalArgumentException("Metadata entry does not have time (" + metadataEntry + ")");
    }

    Value flushID = columns.get(TabletsSection.ServerColumnFamily.FLUSH_COLUMN);
    long initFlushID = -1;
    if (flushID != null)
      initFlushID = Long.parseLong(flushID.toString());

    Value compactID = columns.get(TabletsSection.ServerColumnFamily.COMPACT_COLUMN);
    long initCompactID = -1;
    if (compactID != null)
      initCompactID = Long.parseLong(compactID.toString());

    Text metadataPrevEndRow = KeyExtent.decodePrevEndRow(prevEndRowIBW);

    Table.ID tableId = (new KeyExtent(metadataEntry, (Text) null)).getTableId();

    return fixSplit(context, tableId, metadataEntry, metadataPrevEndRow, oper, splitRatio, tserver, time.toString(), initFlushID, initCompactID, lock);
  }

  private static KeyExtent fixSplit(ClientContext context, Table.ID tableId, Text metadataEntry, Text metadataPrevEndRow, Value oper, double splitRatio,
      TServerInstance tserver, String time, long initFlushID, long initCompactID, ZooLock lock) throws AccumuloException, IOException {
    if (metadataPrevEndRow == null)
      // something is wrong, this should not happen... if a tablet is split, it will always have a
      // prev end row....
      throw new AccumuloException("Split tablet does not have prev end row, something is amiss, extent = " + metadataEntry);

    // check to see if prev tablet exist in metadata tablet
    Key prevRowKey = new Key(new Text(KeyExtent.getMetadataEntry(tableId, metadataPrevEndRow)));

    try (ScannerImpl scanner2 = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY)) {
      scanner2.setRange(new Range(prevRowKey, prevRowKey.followingKey(PartialKey.ROW)));

      VolumeManager fs = VolumeManagerImpl.get();
      if (!scanner2.iterator().hasNext()) {
        log.info("Rolling back incomplete split {} {}", metadataEntry, metadataPrevEndRow);
        MetadataTableUtil.rollBackSplit(metadataEntry, KeyExtent.decodePrevEndRow(oper), context, lock);
        return new KeyExtent(metadataEntry, KeyExtent.decodePrevEndRow(oper));
      } else {
        log.info("Finishing incomplete split {} {}", metadataEntry, metadataPrevEndRow);

        List<FileRef> highDatafilesToRemove = new ArrayList<>();

        SortedMap<FileRef,DataFileValue> origDatafileSizes = new TreeMap<>();
        SortedMap<FileRef,DataFileValue> highDatafileSizes = new TreeMap<>();
        SortedMap<FileRef,DataFileValue> lowDatafileSizes = new TreeMap<>();

        try (Scanner scanner3 = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY)) {
          Key rowKey = new Key(metadataEntry);

          scanner3.fetchColumnFamily(DataFileColumnFamily.NAME);
          scanner3.setRange(new Range(rowKey, rowKey.followingKey(PartialKey.ROW)));

          for (Entry<Key,Value> entry : scanner3) {
            if (entry.getKey().compareColumnFamily(DataFileColumnFamily.NAME) == 0) {
              origDatafileSizes.put(new FileRef(fs, entry.getKey()), new DataFileValue(entry.getValue().get()));
            }
          }
        }

        MetadataTableUtil.splitDatafiles(metadataPrevEndRow, splitRatio, new HashMap<FileRef,FileUtil.FileInfo>(), origDatafileSizes, lowDatafileSizes,
            highDatafileSizes, highDatafilesToRemove);

        MetadataTableUtil.finishSplit(metadataEntry, highDatafileSizes, highDatafilesToRemove, context, lock);

        return new KeyExtent(metadataEntry, KeyExtent.encodePrevEndRow(metadataPrevEndRow));
      }
    }
  }

  private static TServerInstance getTServerInstance(String address, ZooLock zooLock) {
    while (true) {
      try {
        return new TServerInstance(address, zooLock.getSessionId());
      } catch (KeeperException e) {
        log.error("{}", e.getMessage(), e);
      } catch (InterruptedException e) {
        log.error("{}", e.getMessage(), e);
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  public static void replaceDatafiles(ClientContext context, KeyExtent extent, Set<FileRef> datafilesToDelete, Set<FileRef> scanFiles, FileRef path,
      Long compactionId, DataFileValue size, String address, TServerInstance lastLocation, ZooLock zooLock) throws IOException {
    replaceDatafiles(context, extent, datafilesToDelete, scanFiles, path, compactionId, size, address, lastLocation, zooLock, true);
  }

  public static void replaceDatafiles(ClientContext context, KeyExtent extent, Set<FileRef> datafilesToDelete, Set<FileRef> scanFiles, FileRef path,
      Long compactionId, DataFileValue size, String address, TServerInstance lastLocation, ZooLock zooLock, boolean insertDeleteFlags) throws IOException {

    if (insertDeleteFlags) {
      // add delete flags for those paths before the data file reference is removed
      MetadataTableUtil.addDeleteEntries(extent, datafilesToDelete, context);
    }

    // replace data file references to old mapfiles with the new mapfiles
    Mutation m = new Mutation(extent.getMetadataEntry());

    for (FileRef pathToRemove : datafilesToDelete)
      m.putDelete(DataFileColumnFamily.NAME, pathToRemove.meta());

    for (FileRef scanFile : scanFiles)
      m.put(ScanFileColumnFamily.NAME, scanFile.meta(), new Value(new byte[0]));

    if (size.getNumEntries() > 0)
      m.put(DataFileColumnFamily.NAME, path.meta(), new Value(size.encode()));

    if (compactionId != null)
      TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(m, new Value(("" + compactionId).getBytes()));

    TServerInstance self = getTServerInstance(address, zooLock);
    self.putLastLocation(m);

    // remove the old location
    if (lastLocation != null && !lastLocation.equals(self))
      lastLocation.clearLastLocation(m);

    MetadataTableUtil.update(context, zooLock, m, extent);
  }

  /**
   * new data file update function adds one data file to a tablet's list
   *
   * @param path
   *          should be relative to the table directory
   *
   */
  public static void updateTabletDataFile(ClientContext context, KeyExtent extent, FileRef path, FileRef mergeFile, DataFileValue dfv, String time,
      Set<FileRef> filesInUseByScans, String address, ZooLock zooLock, Set<String> unusedWalLogs, TServerInstance lastLocation, long flushId) {
    if (extent.isRootTablet()) {
      if (unusedWalLogs != null) {
        updateRootTabletDataFile(extent, path, mergeFile, dfv, time, filesInUseByScans, address, zooLock, unusedWalLogs, lastLocation, flushId);
      }
      return;
    }
    Mutation m = getUpdateForTabletDataFile(extent, path, mergeFile, dfv, time, filesInUseByScans, address, zooLock, unusedWalLogs, lastLocation, flushId);
    MetadataTableUtil.update(context, zooLock, m, extent);
  }

  /**
   * Update the data file for the root tablet
   */
  private static void updateRootTabletDataFile(KeyExtent extent, FileRef path, FileRef mergeFile, DataFileValue dfv, String time,
      Set<FileRef> filesInUseByScans, String address, ZooLock zooLock, Set<String> unusedWalLogs, TServerInstance lastLocation, long flushId) {
    IZooReaderWriter zk = ZooReaderWriter.getInstance();
    String root = MetadataTableUtil.getZookeeperLogLocation();
    for (String entry : unusedWalLogs) {
      String[] parts = entry.split("/");
      String zpath = root + "/" + parts[parts.length - 1];
      while (true) {
        try {
          if (zk.exists(zpath)) {
            log.debug("Removing WAL reference for root table {}", zpath);
            zk.recursiveDelete(zpath, NodeMissingPolicy.SKIP);
          }
          break;
        } catch (KeeperException e) {
          log.error("{}", e.getMessage(), e);
        } catch (InterruptedException e) {
          log.error("{}", e.getMessage(), e);
        }
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Create an update that updates a tablet
   *
   * @return A Mutation to update a tablet from the given information
   */
  private static Mutation getUpdateForTabletDataFile(KeyExtent extent, FileRef path, FileRef mergeFile, DataFileValue dfv, String time,
      Set<FileRef> filesInUseByScans, String address, ZooLock zooLock, Set<String> unusedWalLogs, TServerInstance lastLocation, long flushId) {
    Mutation m = new Mutation(extent.getMetadataEntry());

    if (dfv.getNumEntries() > 0) {
      m.put(DataFileColumnFamily.NAME, path.meta(), new Value(dfv.encode()));
      TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value(time.getBytes(UTF_8)));
      // stuff in this location
      TServerInstance self = getTServerInstance(address, zooLock);
      self.putLastLocation(m);
      // erase the old location
      if (lastLocation != null && !lastLocation.equals(self))
        lastLocation.clearLastLocation(m);
    }
    if (unusedWalLogs != null) {
      for (String entry : unusedWalLogs) {
        m.putDelete(LogColumnFamily.NAME, new Text(entry));
      }
    }

    for (FileRef scanFile : filesInUseByScans)
      m.put(ScanFileColumnFamily.NAME, scanFile.meta(), new Value(new byte[0]));

    if (mergeFile != null)
      m.putDelete(DataFileColumnFamily.NAME, mergeFile.meta());

    TabletsSection.ServerColumnFamily.FLUSH_COLUMN.put(m, new Value(Long.toString(flushId).getBytes(UTF_8)));

    return m;
  }
}

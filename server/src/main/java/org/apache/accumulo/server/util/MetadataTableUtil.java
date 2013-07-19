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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.BatchWriterImpl;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

/**
 * provides a reference to the metadata table for updates by tablet servers
 */
public class MetadataTableUtil {
  
  private static final Text EMPTY_TEXT = new Text();
  private static Map<TCredentials,Writer> root_tables = new HashMap<TCredentials,Writer>();
  private static Map<TCredentials,Writer> metadata_tables = new HashMap<TCredentials,Writer>();
  private static final Logger log = Logger.getLogger(MetadataTableUtil.class);
  
  private static final int SAVE_ROOT_TABLET_RETRIES = 3;
  
  private MetadataTableUtil() {}
  
  public synchronized static Writer getMetadataTable(TCredentials credentials) {
    Writer metadataTable = metadata_tables.get(credentials);
    if (metadataTable == null) {
      metadataTable = new Writer(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID);
      metadata_tables.put(credentials, metadataTable);
    }
    return metadataTable;
  }
  
  public synchronized static Writer getRootTable(TCredentials credentials) {
    Writer rootTable = root_tables.get(credentials);
    if (rootTable == null) {
      rootTable = new Writer(HdfsZooInstance.getInstance(), credentials, RootTable.ID);
      root_tables.put(credentials, rootTable);
    }
    return rootTable;
  }
  
  public static void putLockID(ZooLock zooLock, Mutation m) {
    TabletsSection.ServerColumnFamily.LOCK_COLUMN.put(m, new Value(zooLock.getLockID().serialize(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + "/")
        .getBytes()));
  }
  
  public static void update(TCredentials credentials, Mutation m, KeyExtent extent) {
    update(credentials, null, m, extent);
  }
  
  public static void update(TCredentials credentials, ZooLock zooLock, Mutation m, KeyExtent extent) {
    Writer t = extent.isMeta() ? getRootTable(credentials) : getMetadataTable(credentials);
    if (zooLock != null)
      putLockID(zooLock, m);
    while (true) {
      try {
        t.update(m);
        return;
      } catch (AccumuloException e) {
        log.error(e, e);
      } catch (AccumuloSecurityException e) {
        log.error(e, e);
      } catch (ConstraintViolationException e) {
        log.error(e, e);
      } catch (TableNotFoundException e) {
        log.error(e, e);
      }
      UtilWaitThread.sleep(1000);
    }
    
  }
  
  /**
   * new data file update function adds one data file to a tablet's list
   * 
   * @param path
   *          should be relative to the table directory
   * 
   */
  public static void updateTabletDataFile(KeyExtent extent, FileRef path, FileRef mergeFile, DataFileValue dfv, String time, TCredentials credentials,
      Set<FileRef> filesInUseByScans, String address, ZooLock zooLock, Set<String> unusedWalLogs, TServerInstance lastLocation, long flushId) {
    if (extent.equals(RootTable.EXTENT)) {
      if (unusedWalLogs != null) {
        IZooReaderWriter zk = ZooReaderWriter.getInstance();
        // unusedWalLogs will contain the location/name of each log in a log set
        // the log set is stored under one of the log names, but not both
        // find the entry under one of the names and delete it.
        String root = getZookeeperLogLocation();
        boolean foundEntry = false;
        for (String entry : unusedWalLogs) {
          String[] parts = entry.split("/");
          String zpath = root + "/" + parts[parts.length - 1];
          while (true) {
            try {
              if (zk.exists(zpath)) {
                zk.recursiveDelete(zpath, NodeMissingPolicy.SKIP);
                foundEntry = true;
              }
              break;
            } catch (KeeperException e) {
              log.error(e, e);
            } catch (InterruptedException e) {
              log.error(e, e);
            }
            UtilWaitThread.sleep(1000);
          }
        }
        if (unusedWalLogs.size() > 0 && !foundEntry)
          log.warn("WALog entry for root tablet did not exist " + unusedWalLogs);
      }
      return;
    }
    
    Mutation m = new Mutation(extent.getMetadataEntry());
    
    if (dfv.getNumEntries() > 0) {
      m.put(DataFileColumnFamily.NAME, path.meta(), new Value(dfv.encode()));
      TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value(time.getBytes()));
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
      m.put(ScanFileColumnFamily.NAME, scanFile.meta(), new Value("".getBytes()));
    
    if (mergeFile != null)
      m.putDelete(DataFileColumnFamily.NAME, mergeFile.meta());
    
    TabletsSection.ServerColumnFamily.FLUSH_COLUMN.put(m, new Value((flushId + "").getBytes()));
    
    update(credentials, zooLock, m, extent);
    
  }
  
  private static TServerInstance getTServerInstance(String address, ZooLock zooLock) {
    while (true) {
      try {
        return new TServerInstance(address, zooLock.getSessionId());
      } catch (KeeperException e) {
        log.error(e, e);
      } catch (InterruptedException e) {
        log.error(e, e);
      }
      UtilWaitThread.sleep(1000);
    }
  }
  
  public static void updateTabletFlushID(KeyExtent extent, long flushID, TCredentials credentials, ZooLock zooLock) {
    if (!extent.isRootTablet()) {
      Mutation m = new Mutation(extent.getMetadataEntry());
      TabletsSection.ServerColumnFamily.FLUSH_COLUMN.put(m, new Value((flushID + "").getBytes()));
      update(credentials, zooLock, m, extent);
    }
  }
  
  public static void updateTabletCompactID(KeyExtent extent, long compactID, TCredentials credentials, ZooLock zooLock) {
    if (!extent.isRootTablet()) {
      Mutation m = new Mutation(extent.getMetadataEntry());
      TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(m, new Value((compactID + "").getBytes()));
      update(credentials, zooLock, m, extent);
    }
  }
  
  public static void updateTabletDataFile(long tid, KeyExtent extent, Map<FileRef,DataFileValue> estSizes, String time, TCredentials credentials,
      ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    byte[] tidBytes = Long.toString(tid).getBytes();
    
    for (Entry<FileRef,DataFileValue> entry : estSizes.entrySet()) {
      Text file = entry.getKey().meta();
      m.put(DataFileColumnFamily.NAME, file, new Value(entry.getValue().encode()));
      m.put(TabletsSection.BulkFileColumnFamily.NAME, file, new Value(tidBytes));
    }
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value(time.getBytes()));
    update(credentials, zooLock, m, extent);
  }
  
  public static void addTablet(KeyExtent extent, String path, TCredentials credentials, char timeType, ZooLock lock) {
    Mutation m = extent.getPrevRowUpdateMutation();
    
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(path.getBytes()));
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value((timeType + "0").getBytes()));
    
    update(credentials, lock, m, extent);
  }
  
  public static void updateTabletPrevEndRow(KeyExtent extent, TCredentials credentials) {
    Mutation m = extent.getPrevRowUpdateMutation(); //
    update(credentials, m, extent);
  }
  
  /**
   * convenience method for reading entries from the metadata table
   */
  public static SortedMap<KeyExtent,Text> getMetadataDirectoryEntries(SortedMap<Key,Value> entries) {
    Key key;
    Value val;
    Text datafile = null;
    Value prevRow = null;
    KeyExtent ke;
    
    SortedMap<KeyExtent,Text> results = new TreeMap<KeyExtent,Text>();
    
    Text lastRowFromKey = new Text();
    
    // text obj below is meant to be reused in loop for efficiency
    Text colf = new Text();
    Text colq = new Text();
    
    for (Entry<Key,Value> entry : entries.entrySet()) {
      key = entry.getKey();
      val = entry.getValue();
      
      if (key.compareRow(lastRowFromKey) != 0) {
        prevRow = null;
        datafile = null;
        key.getRow(lastRowFromKey);
      }
      
      colf = key.getColumnFamily(colf);
      colq = key.getColumnQualifier(colq);
      
      // interpret the row id as a key extent
      if (TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.equals(colf, colq))
        datafile = new Text(val.toString());
      
      else if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.equals(colf, colq))
        prevRow = new Value(val);
      
      if (datafile != null && prevRow != null) {
        ke = new KeyExtent(key.getRow(), prevRow);
        results.put(ke, datafile);
        
        datafile = null;
        prevRow = null;
      }
    }
    return results;
  }
  
  public static boolean recordRootTabletLocation(String address) {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    for (int i = 0; i < SAVE_ROOT_TABLET_RETRIES; i++) {
      try {
        log.info("trying to write root tablet location to ZooKeeper as " + address);
        String zRootLocPath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + RootTable.ZROOT_TABLET_LOCATION;
        zoo.putPersistentData(zRootLocPath, address.getBytes(), NodeExistsPolicy.OVERWRITE);
        return true;
      } catch (Exception e) {
        log.error("Master: unable to save root tablet location in zookeeper. exception: " + e, e);
      }
    }
    log.error("Giving up after " + SAVE_ROOT_TABLET_RETRIES + " retries");
    return false;
  }
  
  public static SortedMap<FileRef,DataFileValue> getDataFileSizes(KeyExtent extent, TCredentials credentials) throws IOException {
    TreeMap<FileRef,DataFileValue> sizes = new TreeMap<FileRef,DataFileValue>();
    
    Scanner mdScanner = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID, Authorizations.EMPTY);
    mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    Text row = extent.getMetadataEntry();
    VolumeManager fs = VolumeManagerImpl.get();
    
    Key endKey = new Key(row, DataFileColumnFamily.NAME, new Text(""));
    endKey = endKey.followingKey(PartialKey.ROW_COLFAM);
    
    mdScanner.setRange(new Range(new Key(row), endKey));
    for (Entry<Key,Value> entry : mdScanner) {
      
      if (!entry.getKey().getRow().equals(row))
        break;
      DataFileValue dfv = new DataFileValue(entry.getValue().get());
      sizes.put(new FileRef(fs, entry.getKey()), dfv);
    }
    
    return sizes;
  }
  
  public static void addNewTablet(KeyExtent extent, String path, TServerInstance location, Map<FileRef,DataFileValue> datafileSizes,
      Map<FileRef,Long> bulkLoadedFiles, TCredentials credentials, String time, long lastFlushID, long lastCompactID, ZooLock zooLock) {
    Mutation m = extent.getPrevRowUpdateMutation();
    
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(path.getBytes()));
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value(time.getBytes()));
    if (lastFlushID > 0)
      TabletsSection.ServerColumnFamily.FLUSH_COLUMN.put(m, new Value(("" + lastFlushID).getBytes()));
    if (lastCompactID > 0)
      TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(m, new Value(("" + lastCompactID).getBytes()));
    
    if (location != null) {
      m.put(TabletsSection.CurrentLocationColumnFamily.NAME, location.asColumnQualifier(), location.asMutationValue());
      m.putDelete(TabletsSection.FutureLocationColumnFamily.NAME, location.asColumnQualifier());
    }
    
    for (Entry<FileRef,DataFileValue> entry : datafileSizes.entrySet()) {
      m.put(DataFileColumnFamily.NAME, entry.getKey().meta(), new Value(entry.getValue().encode()));
    }
    
    for (Entry<FileRef,Long> entry : bulkLoadedFiles.entrySet()) {
      byte[] tidBytes = Long.toString(entry.getValue()).getBytes();
      m.put(TabletsSection.BulkFileColumnFamily.NAME, entry.getKey().meta(), new Value(tidBytes));
    }
    
    update(credentials, zooLock, m, extent);
  }
  
  public static void rollBackSplit(Text metadataEntry, Text oldPrevEndRow, TCredentials credentials, ZooLock zooLock) {
    KeyExtent ke = new KeyExtent(metadataEntry, oldPrevEndRow);
    Mutation m = ke.getPrevRowUpdateMutation();
    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.putDelete(m);
    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.putDelete(m);
    update(credentials, zooLock, m, new KeyExtent(metadataEntry, (Text) null));
  }
  
  public static void splitTablet(KeyExtent extent, Text oldPrevEndRow, double splitRatio, TCredentials credentials, ZooLock zooLock) {
    Mutation m = extent.getPrevRowUpdateMutation(); //
    
    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value(Double.toString(splitRatio).getBytes()));
    
    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m, KeyExtent.encodePrevEndRow(oldPrevEndRow));
    ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);
    update(credentials, zooLock, m, extent);
  }
  
  public static void finishSplit(Text metadataEntry, Map<FileRef,DataFileValue> datafileSizes, List<FileRef> highDatafilesToRemove, TCredentials credentials,
      ZooLock zooLock) {
    Mutation m = new Mutation(metadataEntry);
    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.putDelete(m);
    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.putDelete(m);
    ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);
    
    for (Entry<FileRef,DataFileValue> entry : datafileSizes.entrySet()) {
      m.put(DataFileColumnFamily.NAME, entry.getKey().meta(), new Value(entry.getValue().encode()));
    }
    
    for (FileRef pathToRemove : highDatafilesToRemove) {
      m.putDelete(DataFileColumnFamily.NAME, pathToRemove.meta());
    }
    
    update(credentials, zooLock, m, new KeyExtent(metadataEntry, (Text) null));
  }
  
  public static void finishSplit(KeyExtent extent, Map<FileRef,DataFileValue> datafileSizes, List<FileRef> highDatafilesToRemove, TCredentials credentials,
      ZooLock zooLock) {
    finishSplit(extent.getMetadataEntry(), datafileSizes, highDatafilesToRemove, credentials, zooLock);
  }
  
  public static void replaceDatafiles(KeyExtent extent, Set<FileRef> datafilesToDelete, Set<FileRef> scanFiles, FileRef path, Long compactionId,
      DataFileValue size, TCredentials credentials, String address, TServerInstance lastLocation, ZooLock zooLock) throws IOException {
    replaceDatafiles(extent, datafilesToDelete, scanFiles, path, compactionId, size, credentials, address, lastLocation, zooLock, true);
  }
  
  public static void replaceDatafiles(KeyExtent extent, Set<FileRef> datafilesToDelete, Set<FileRef> scanFiles, FileRef path, Long compactionId,
      DataFileValue size, TCredentials credentials, String address, TServerInstance lastLocation, ZooLock zooLock, boolean insertDeleteFlags)
      throws IOException {
    
    if (insertDeleteFlags) {
      // add delete flags for those paths before the data file reference is removed
      addDeleteEntries(extent, datafilesToDelete, credentials);
    }
    
    // replace data file references to old mapfiles with the new mapfiles
    Mutation m = new Mutation(extent.getMetadataEntry());
    
    for (FileRef pathToRemove : datafilesToDelete)
      m.putDelete(DataFileColumnFamily.NAME, pathToRemove.meta());
    
    for (FileRef scanFile : scanFiles)
      m.put(ScanFileColumnFamily.NAME, scanFile.meta(), new Value("".getBytes()));
    
    if (size.getNumEntries() > 0)
      m.put(DataFileColumnFamily.NAME, path.meta(), new Value(size.encode()));
    
    if (compactionId != null)
      TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(m, new Value(("" + compactionId).getBytes()));
    
    TServerInstance self = getTServerInstance(address, zooLock);
    self.putLastLocation(m);
    
    // remove the old location
    if (lastLocation != null && !lastLocation.equals(self))
      lastLocation.clearLastLocation(m);
    
    update(credentials, zooLock, m, extent);
  }
  
  public static void addDeleteEntries(KeyExtent extent, Set<FileRef> datafilesToDelete, TCredentials credentials) throws IOException {
    
    String tableId = extent.getTableId().toString();
    
    // TODO could use batch writer,would need to handle failure and retry like update does - ACCUMULO-1294
    for (FileRef pathToRemove : datafilesToDelete) {
      update(credentials, createDeleteMutation(tableId, pathToRemove.path().toString()), extent);
    }
  }
  
  public static void addDeleteEntry(String tableId, String path) throws IOException {
    update(SystemCredentials.get().getAsThrift(), createDeleteMutation(tableId, path), new KeyExtent(new Text(tableId), null, null));
  }
  
  public static Mutation createDeleteMutation(String tableId, String pathToRemove) throws IOException {
    if (!pathToRemove.contains(":")) {
      if (pathToRemove.startsWith("../"))
        pathToRemove = pathToRemove.substring(2);
      else
        pathToRemove = "/" + tableId + "/" + pathToRemove;
    }
    
    Path path = VolumeManagerImpl.get().getFullPath(ServerConstants.getTablesDirs(), pathToRemove);
    Mutation delFlag = new Mutation(new Text(MetadataSchema.DeletesSection.getRowPrefix() + path.toString()));
    delFlag.put(EMPTY_TEXT, EMPTY_TEXT, new Value(new byte[] {}));
    return delFlag;
  }
  
  public static void removeScanFiles(KeyExtent extent, Set<FileRef> scanFiles, TCredentials credentials, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    
    for (FileRef pathToRemove : scanFiles)
      m.putDelete(ScanFileColumnFamily.NAME, pathToRemove.meta());
    
    update(credentials, zooLock, m, extent);
  }
  
  private static KeyExtent fixSplit(Text table, Text metadataEntry, Text metadataPrevEndRow, Value oper, double splitRatio, TServerInstance tserver,
      TCredentials credentials, String time, long initFlushID, long initCompactID, ZooLock lock) throws AccumuloException, IOException {
    if (metadataPrevEndRow == null)
      // something is wrong, this should not happen... if a tablet is split, it will always have a
      // prev end row....
      throw new AccumuloException("Split tablet does not have prev end row, something is amiss, extent = " + metadataEntry);
    
    // check to see if prev tablet exist in metadata tablet
    Key prevRowKey = new Key(new Text(KeyExtent.getMetadataEntry(table, metadataPrevEndRow)));
    
    ScannerImpl scanner2 = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID, Authorizations.EMPTY);
    scanner2.setRange(new Range(prevRowKey, prevRowKey.followingKey(PartialKey.ROW)));
    
    VolumeManager fs = VolumeManagerImpl.get();
    if (!scanner2.iterator().hasNext()) {
      log.info("Rolling back incomplete split " + metadataEntry + " " + metadataPrevEndRow);
      rollBackSplit(metadataEntry, KeyExtent.decodePrevEndRow(oper), credentials, lock);
      return new KeyExtent(metadataEntry, KeyExtent.decodePrevEndRow(oper));
    } else {
      log.info("Finishing incomplete split " + metadataEntry + " " + metadataPrevEndRow);
      
      List<FileRef> highDatafilesToRemove = new ArrayList<FileRef>();
      
      Scanner scanner3 = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID, Authorizations.EMPTY);
      Key rowKey = new Key(metadataEntry);
      
      SortedMap<FileRef,DataFileValue> origDatafileSizes = new TreeMap<FileRef,DataFileValue>();
      SortedMap<FileRef,DataFileValue> highDatafileSizes = new TreeMap<FileRef,DataFileValue>();
      SortedMap<FileRef,DataFileValue> lowDatafileSizes = new TreeMap<FileRef,DataFileValue>();
      scanner3.fetchColumnFamily(DataFileColumnFamily.NAME);
      scanner3.setRange(new Range(rowKey, rowKey.followingKey(PartialKey.ROW)));
      
      for (Entry<Key,Value> entry : scanner3) {
        if (entry.getKey().compareColumnFamily(DataFileColumnFamily.NAME) == 0) {
          origDatafileSizes.put(new FileRef(fs, entry.getKey()), new DataFileValue(entry.getValue().get()));
        }
      }
      
      splitDatafiles(table, metadataPrevEndRow, splitRatio, new HashMap<FileRef,FileUtil.FileInfo>(), origDatafileSizes, lowDatafileSizes, highDatafileSizes,
          highDatafilesToRemove);
      
      MetadataTableUtil.finishSplit(metadataEntry, highDatafileSizes, highDatafilesToRemove, credentials, lock);
      
      return new KeyExtent(metadataEntry, KeyExtent.encodePrevEndRow(metadataPrevEndRow));
    }
    
  }
  
  public static void splitDatafiles(Text table, Text midRow, double splitRatio, Map<FileRef,FileUtil.FileInfo> firstAndLastRows,
      SortedMap<FileRef,DataFileValue> datafiles, SortedMap<FileRef,DataFileValue> lowDatafileSizes, SortedMap<FileRef,DataFileValue> highDatafileSizes,
      List<FileRef> highDatafilesToRemove) {
    
    for (Entry<FileRef,DataFileValue> entry : datafiles.entrySet()) {
      
      Text firstRow = null;
      Text lastRow = null;
      
      boolean rowsKnown = false;
      
      FileUtil.FileInfo mfi = firstAndLastRows.get(entry.getKey());
      
      if (mfi != null) {
        firstRow = mfi.getFirstRow();
        lastRow = mfi.getLastRow();
        rowsKnown = true;
      }
      
      if (rowsKnown && firstRow.compareTo(midRow) > 0) {
        // only in high
        long highSize = entry.getValue().getSize();
        long highEntries = entry.getValue().getNumEntries();
        highDatafileSizes.put(entry.getKey(), new DataFileValue(highSize, highEntries, entry.getValue().getTime()));
      } else if (rowsKnown && lastRow.compareTo(midRow) <= 0) {
        // only in low
        long lowSize = entry.getValue().getSize();
        long lowEntries = entry.getValue().getNumEntries();
        lowDatafileSizes.put(entry.getKey(), new DataFileValue(lowSize, lowEntries, entry.getValue().getTime()));
        
        highDatafilesToRemove.add(entry.getKey());
      } else {
        long lowSize = (long) Math.floor((entry.getValue().getSize() * splitRatio));
        long lowEntries = (long) Math.floor((entry.getValue().getNumEntries() * splitRatio));
        lowDatafileSizes.put(entry.getKey(), new DataFileValue(lowSize, lowEntries, entry.getValue().getTime()));
        
        long highSize = (long) Math.ceil((entry.getValue().getSize() * (1.0 - splitRatio)));
        long highEntries = (long) Math.ceil((entry.getValue().getNumEntries() * (1.0 - splitRatio)));
        highDatafileSizes.put(entry.getKey(), new DataFileValue(highSize, highEntries, entry.getValue().getTime()));
      }
    }
  }
  
  public static KeyExtent fixSplit(Text metadataEntry, SortedMap<ColumnFQ,Value> columns, TServerInstance tserver, TCredentials credentials, ZooLock lock)
      throws AccumuloException, IOException {
    log.info("Incomplete split " + metadataEntry + " attempting to fix");
    
    Value oper = columns.get(TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN);
    
    if (columns.get(TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN) == null) {
      throw new IllegalArgumentException("Metadata entry does not have split ratio (" + metadataEntry + ")");
    }
    
    double splitRatio = Double.parseDouble(new String(columns.get(TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN).get()));
    
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
    
    Text table = (new KeyExtent(metadataEntry, (Text) null)).getTableId();
    
    return fixSplit(table, metadataEntry, metadataPrevEndRow, oper, splitRatio, tserver, credentials, time.toString(), initFlushID, initCompactID, lock);
  }
  
  public static void deleteTable(String tableId, boolean insertDeletes, TCredentials credentials, ZooLock lock) throws AccumuloException, IOException {
    Scanner ms = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID, Authorizations.EMPTY);
    Text tableIdText = new Text(tableId);
    BatchWriter bw = new BatchWriterImpl(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID, new BatchWriterConfig().setMaxMemory(1000000)
        .setMaxLatency(120000l, TimeUnit.MILLISECONDS).setMaxWriteThreads(2));
    
    // scan metadata for our table and delete everything we find
    Mutation m = null;
    ms.setRange(new KeyExtent(tableIdText, null, null).toMetadataRange());
    
    // insert deletes before deleting data from !METADATA... this makes the code fault tolerant
    if (insertDeletes) {
      
      ms.fetchColumnFamily(DataFileColumnFamily.NAME);
      TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(ms);
      
      for (Entry<Key,Value> cell : ms) {
        Key key = cell.getKey();
        
        if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
          FileRef ref = new FileRef(VolumeManagerImpl.get(), key);
          bw.addMutation(createDeleteMutation(tableId, ref.meta().toString()));
        }
        
        if (TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
          bw.addMutation(createDeleteMutation(tableId, cell.getValue().toString()));
        }
      }
      
      bw.flush();
      
      ms.clearColumns();
    }
    
    for (Entry<Key,Value> cell : ms) {
      Key key = cell.getKey();
      
      if (m == null) {
        m = new Mutation(key.getRow());
        if (lock != null)
          putLockID(lock, m);
      }
      
      if (key.getRow().compareTo(m.getRow(), 0, m.getRow().length) != 0) {
        bw.addMutation(m);
        m = new Mutation(key.getRow());
        if (lock != null)
          putLockID(lock, m);
      }
      m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
    }
    
    if (m != null)
      bw.addMutation(m);
    
    bw.close();
  }
  
  public static class LogEntry {
    public KeyExtent extent;
    public long timestamp;
    public String server;
    public String filename;
    public int tabletId;
    public Collection<String> logSet;
    
    @Override
    public String toString() {
      return extent.toString() + " " + filename + " (" + tabletId + ")";
    }
    
    public String getName() {
      return server + "/" + filename;
    }
    
    public byte[] toBytes() throws IOException {
      DataOutputBuffer out = new DataOutputBuffer();
      extent.write(out);
      out.writeLong(timestamp);
      out.writeUTF(server);
      out.writeUTF(filename.toString());
      out.write(tabletId);
      out.write(logSet.size());
      for (String s : logSet) {
        out.writeUTF(s);
      }
      return Arrays.copyOf(out.getData(), out.getLength());
    }
    
    public void fromBytes(byte bytes[]) throws IOException {
      DataInputBuffer inp = new DataInputBuffer();
      inp.reset(bytes, bytes.length);
      extent = new KeyExtent();
      extent.readFields(inp);
      timestamp = inp.readLong();
      server = inp.readUTF();
      filename = inp.readUTF();
      tabletId = inp.read();
      int count = inp.read();
      ArrayList<String> logSet = new ArrayList<String>(count);
      for (int i = 0; i < count; i++)
        logSet.add(inp.readUTF());
      this.logSet = logSet;
    }
    
  }
  
  private static String getZookeeperLogLocation() {
    return ZooUtil.getRoot(HdfsZooInstance.getInstance()) + RootTable.ZROOT_TABLET_WALOGS;
  }
  
  public static void addLogEntry(TCredentials credentials, LogEntry entry, ZooLock zooLock) {
    if (entry.extent.isRootTablet()) {
      String root = getZookeeperLogLocation();
      while (true) {
        try {
          IZooReaderWriter zoo = ZooReaderWriter.getInstance();
          if (zoo.isLockHeld(zooLock.getLockID())) {
            String[] parts = entry.filename.split("/");
            String uniqueId = parts[parts.length - 1];
            zoo.putPersistentData(root + "/" + uniqueId, entry.toBytes(), NodeExistsPolicy.OVERWRITE);
          }
          break;
        } catch (KeeperException e) {
          log.error(e, e);
        } catch (InterruptedException e) {
          log.error(e, e);
        } catch (IOException e) {
          log.error(e, e);
        }
        UtilWaitThread.sleep(1000);
      }
    } else {
      String value = StringUtil.join(entry.logSet, ";") + "|" + entry.tabletId;
      Mutation m = new Mutation(entry.extent.getMetadataEntry());
      m.put(LogColumnFamily.NAME, new Text(entry.server + "/" + entry.filename), new Value(value.getBytes()));
      update(credentials, zooLock, m, entry.extent);
    }
  }
  
  public static LogEntry entryFromKeyValue(Key key, Value value) {
    MetadataTableUtil.LogEntry e = new MetadataTableUtil.LogEntry();
    e.extent = new KeyExtent(key.getRow(), EMPTY_TEXT);
    String[] parts = key.getColumnQualifier().toString().split("/", 2);
    e.server = parts[0];
    e.filename = parts[1];
    parts = value.toString().split("\\|");
    e.tabletId = Integer.parseInt(parts[1]);
    e.logSet = Arrays.asList(parts[0].split(";"));
    e.timestamp = key.getTimestamp();
    return e;
  }
  
  public static Pair<List<LogEntry>,SortedMap<FileRef,DataFileValue>> getFileAndLogEntries(TCredentials credentials, KeyExtent extent) throws KeeperException,
      InterruptedException, IOException {
    ArrayList<LogEntry> result = new ArrayList<LogEntry>();
    TreeMap<FileRef,DataFileValue> sizes = new TreeMap<FileRef,DataFileValue>();
    
    VolumeManager fs = VolumeManagerImpl.get();
    if (extent.isRootTablet()) {
      getRootLogEntries(result);
      Path rootDir = new Path(ServerConstants.getRootTabletDir());
      rootDir = rootDir.makeQualified(fs.getDefaultVolume());
      FileStatus[] files = fs.listStatus(rootDir);
      for (FileStatus fileStatus : files) {
        if (fileStatus.getPath().toString().endsWith("_tmp")) {
          continue;
        }
        DataFileValue dfv = new DataFileValue(0, 0);
        sizes.put(new FileRef(fileStatus.getPath().toString(), fileStatus.getPath()), dfv);
      }
      
    } else {
      String systemTableToCheck = extent.isMeta() ? RootTable.ID : MetadataTable.ID;
      Scanner scanner = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, systemTableToCheck, Authorizations.EMPTY);
      scanner.fetchColumnFamily(LogColumnFamily.NAME);
      scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      scanner.setRange(extent.toMetadataRange());
      
      for (Entry<Key,Value> entry : scanner) {
        if (!entry.getKey().getRow().equals(extent.getMetadataEntry())) {
          throw new RuntimeException("Unexpected row " + entry.getKey().getRow() + " expected " + extent.getMetadataEntry());
        }
        
        if (entry.getKey().getColumnFamily().equals(LogColumnFamily.NAME)) {
          result.add(entryFromKeyValue(entry.getKey(), entry.getValue()));
        } else if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
          DataFileValue dfv = new DataFileValue(entry.getValue().get());
          sizes.put(new FileRef(fs, entry.getKey()), dfv);
        } else {
          throw new RuntimeException("Unexpected col fam " + entry.getKey().getColumnFamily());
        }
      }
    }
    
    return new Pair<List<LogEntry>,SortedMap<FileRef,DataFileValue>>(result, sizes);
  }
  
  public static List<LogEntry> getLogEntries(TCredentials credentials, KeyExtent extent) throws IOException, KeeperException, InterruptedException {
    log.info("Scanning logging entries for " + extent);
    ArrayList<LogEntry> result = new ArrayList<LogEntry>();
    if (extent.equals(RootTable.EXTENT)) {
      log.info("Getting logs for root tablet from zookeeper");
      getRootLogEntries(result);
    } else {
      log.info("Scanning metadata for logs used for tablet " + extent);
      Scanner scanner = getTabletLogScanner(credentials, extent);
      Text pattern = extent.getMetadataEntry();
      for (Entry<Key,Value> entry : scanner) {
        Text row = entry.getKey().getRow();
        if (entry.getKey().getColumnFamily().equals(LogColumnFamily.NAME)) {
          if (row.equals(pattern)) {
            result.add(entryFromKeyValue(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
    
    Collections.sort(result, new Comparator<LogEntry>() {
      @Override
      public int compare(LogEntry o1, LogEntry o2) {
        long diff = o1.timestamp - o2.timestamp;
        if (diff < 0)
          return -1;
        if (diff > 0)
          return 1;
        return 0;
      }
    });
    log.info("Returning logs " + result + " for extent " + extent);
    return result;
  }
  
  private static void getRootLogEntries(ArrayList<LogEntry> result) throws KeeperException, InterruptedException, IOException {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    String root = getZookeeperLogLocation();
    // there's a little race between getting the children and fetching
    // the data. The log can be removed in between.
    while (true) {
      result.clear();
      for (String child : zoo.getChildren(root)) {
        LogEntry e = new LogEntry();
        try {
          e.fromBytes(zoo.getData(root + "/" + child, null));
          result.add(e);
        } catch (KeeperException.NoNodeException ex) {
          continue;
        }
      }
      break;
    }
  }
  
  private static Scanner getTabletLogScanner(TCredentials credentials, KeyExtent extent) {
    String tableId = MetadataTable.ID;
    if (extent.isMeta())
      tableId = RootTable.ID;
    Scanner scanner = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, tableId, Authorizations.EMPTY);
    scanner.fetchColumnFamily(LogColumnFamily.NAME);
    Text start = extent.getMetadataEntry();
    Key endKey = new Key(start, LogColumnFamily.NAME);
    endKey = endKey.followingKey(PartialKey.ROW_COLFAM);
    scanner.setRange(new Range(new Key(start), endKey));
    return scanner;
  }
  
  static class LogEntryIterator implements Iterator<LogEntry> {
    
    Iterator<LogEntry> rootTabletEntries = null;
    Iterator<Entry<Key,Value>> metadataEntries = null;
    
    LogEntryIterator(TCredentials creds) throws IOException, KeeperException, InterruptedException {
      rootTabletEntries = getLogEntries(creds, RootTable.EXTENT).iterator();
      try {
        Scanner scanner = HdfsZooInstance.getInstance().getConnector(creds.getPrincipal(), CredentialHelper.extractToken(creds))
            .createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        log.info("Setting range to " + MetadataSchema.TabletsSection.getRange());
        scanner.setRange(MetadataSchema.TabletsSection.getRange());
        scanner.fetchColumnFamily(LogColumnFamily.NAME);
        metadataEntries = scanner.iterator();
      } catch (Exception ex) {
        throw new IOException(ex);
      }
    }
    
    @Override
    public boolean hasNext() {
      return rootTabletEntries.hasNext() || metadataEntries.hasNext();
    }
    
    @Override
    public LogEntry next() {
      if (rootTabletEntries.hasNext()) {
        return rootTabletEntries.next();
      }
      Entry<Key,Value> entry = metadataEntries.next();
      return entryFromKeyValue(entry.getKey(), entry.getValue());
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  public static Iterator<LogEntry> getLogEntries(TCredentials creds) throws IOException, KeeperException, InterruptedException {
    return new LogEntryIterator(creds);
  }
  
  public static void removeUnusedWALEntries(KeyExtent extent, List<LogEntry> logEntries, ZooLock zooLock) {
    for (LogEntry entry : logEntries) {
      if (entry.extent.isRootTablet()) {
        String root = getZookeeperLogLocation();
        while (true) {
          try {
            IZooReaderWriter zoo = ZooReaderWriter.getInstance();
            if (zoo.isLockHeld(zooLock.getLockID()))
              zoo.recursiveDelete(root + "/" + entry.filename, NodeMissingPolicy.SKIP);
            break;
          } catch (Exception e) {
            log.error(e, e);
          }
          UtilWaitThread.sleep(1000);
        }
      } else {
        Mutation m = new Mutation(entry.extent.getMetadataEntry());
        m.putDelete(LogColumnFamily.NAME, new Text(entry.server + "/" + entry.filename));
        update(SystemCredentials.get().getAsThrift(), zooLock, m, entry.extent);
      }
    }
  }
  
  private static void getFiles(Set<String> files, Map<Key,Value> tablet, String srcTableId) {
    for (Entry<Key,Value> entry : tablet.entrySet()) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        String cf = entry.getKey().getColumnQualifier().toString();
        if (srcTableId != null && !cf.startsWith("../") && !cf.contains(":")) {
          cf = "../" + srcTableId + entry.getKey().getColumnQualifier();
        }
        files.add(cf);
      }
    }
  }
  
  private static Mutation createCloneMutation(String srcTableId, String tableId, Map<Key,Value> tablet) {
    
    KeyExtent ke = new KeyExtent(tablet.keySet().iterator().next().getRow(), (Text) null);
    Mutation m = new Mutation(KeyExtent.getMetadataEntry(new Text(tableId), ke.getEndRow()));
    
    for (Entry<Key,Value> entry : tablet.entrySet()) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        String cf = entry.getKey().getColumnQualifier().toString();
        if (!cf.startsWith("../") && !cf.contains(":"))
          cf = "../" + srcTableId + entry.getKey().getColumnQualifier();
        m.put(entry.getKey().getColumnFamily(), new Text(cf), entry.getValue());
      } else if (entry.getKey().getColumnFamily().equals(TabletsSection.CurrentLocationColumnFamily.NAME)) {
        m.put(TabletsSection.LastLocationColumnFamily.NAME, entry.getKey().getColumnQualifier(), entry.getValue());
      } else if (entry.getKey().getColumnFamily().equals(TabletsSection.LastLocationColumnFamily.NAME)) {
        // skip
      } else {
        m.put(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(), entry.getValue());
      }
    }
    return m;
  }
  
  private static Scanner createCloneScanner(String tableId, Connector conn) throws TableNotFoundException {
    Scanner mscanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
    mscanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange());
    mscanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    mscanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
    mscanner.fetchColumnFamily(TabletsSection.LastLocationColumnFamily.NAME);
    mscanner.fetchColumnFamily(ClonedColumnFamily.NAME);
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(mscanner);
    TabletsSection.ServerColumnFamily.TIME_COLUMN.fetch(mscanner);
    return mscanner;
  }
  
  static void initializeClone(String srcTableId, String tableId, Connector conn, BatchWriter bw) throws TableNotFoundException, MutationsRejectedException {
    TabletIterator ti = new TabletIterator(createCloneScanner(srcTableId, conn), new KeyExtent(new Text(srcTableId), null, null).toMetadataRange(), true, true);
    
    if (!ti.hasNext())
      throw new RuntimeException(" table deleted during clone?  srcTableId = " + srcTableId);
    
    while (ti.hasNext())
      bw.addMutation(createCloneMutation(srcTableId, tableId, ti.next()));
    
    bw.flush();
  }
  
  static int compareEndRows(Text endRow1, Text endRow2) {
    return new KeyExtent(new Text("0"), endRow1, null).compareTo(new KeyExtent(new Text("0"), endRow2, null));
  }
  
  static int checkClone(String srcTableId, String tableId, Connector conn, BatchWriter bw) throws TableNotFoundException, MutationsRejectedException {
    TabletIterator srcIter = new TabletIterator(createCloneScanner(srcTableId, conn), new KeyExtent(new Text(srcTableId), null, null).toMetadataRange(), true,
        true);
    TabletIterator cloneIter = new TabletIterator(createCloneScanner(tableId, conn), new KeyExtent(new Text(tableId), null, null).toMetadataRange(), true, true);
    
    if (!cloneIter.hasNext() || !srcIter.hasNext())
      throw new RuntimeException(" table deleted during clone?  srcTableId = " + srcTableId + " tableId=" + tableId);
    
    int rewrites = 0;
    
    while (cloneIter.hasNext()) {
      Map<Key,Value> cloneTablet = cloneIter.next();
      Text cloneEndRow = new KeyExtent(cloneTablet.keySet().iterator().next().getRow(), (Text) null).getEndRow();
      HashSet<String> cloneFiles = new HashSet<String>();
      
      boolean cloneSuccessful = false;
      for (Entry<Key,Value> entry : cloneTablet.entrySet()) {
        if (entry.getKey().getColumnFamily().equals(ClonedColumnFamily.NAME)) {
          cloneSuccessful = true;
          break;
        }
      }
      
      if (!cloneSuccessful)
        getFiles(cloneFiles, cloneTablet, null);
      
      List<Map<Key,Value>> srcTablets = new ArrayList<Map<Key,Value>>();
      Map<Key,Value> srcTablet = srcIter.next();
      srcTablets.add(srcTablet);
      
      Text srcEndRow = new KeyExtent(srcTablet.keySet().iterator().next().getRow(), (Text) null).getEndRow();
      
      int cmp = compareEndRows(cloneEndRow, srcEndRow);
      if (cmp < 0)
        throw new TabletIterator.TabletDeletedException("Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);
      
      HashSet<String> srcFiles = new HashSet<String>();
      if (!cloneSuccessful)
        getFiles(srcFiles, srcTablet, srcTableId);
      
      while (cmp > 0) {
        srcTablet = srcIter.next();
        srcTablets.add(srcTablet);
        srcEndRow = new KeyExtent(srcTablet.keySet().iterator().next().getRow(), (Text) null).getEndRow();
        cmp = compareEndRows(cloneEndRow, srcEndRow);
        if (cmp < 0)
          throw new TabletIterator.TabletDeletedException("Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);
        
        if (!cloneSuccessful)
          getFiles(srcFiles, srcTablet, srcTableId);
      }
      
      if (cloneSuccessful)
        continue;
      
      if (!srcFiles.containsAll(cloneFiles)) {
        // delete existing cloned tablet entry
        Mutation m = new Mutation(cloneTablet.keySet().iterator().next().getRow());
        
        for (Entry<Key,Value> entry : cloneTablet.entrySet()) {
          Key k = entry.getKey();
          m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), k.getTimestamp());
        }
        
        bw.addMutation(m);
        
        for (Map<Key,Value> st : srcTablets)
          bw.addMutation(createCloneMutation(srcTableId, tableId, st));
        
        rewrites++;
      } else {
        // write out marker that this tablet was successfully cloned
        Mutation m = new Mutation(cloneTablet.keySet().iterator().next().getRow());
        m.put(ClonedColumnFamily.NAME, new Text(""), new Value("OK".getBytes()));
        bw.addMutation(m);
      }
    }
    
    bw.flush();
    return rewrites;
  }
  
  public static void cloneTable(Instance instance, String srcTableId, String tableId) throws Exception {
    
    Connector conn = instance.getConnector(SystemCredentials.get().getPrincipal(), SystemCredentials.get().getToken());
    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    
    while (true) {
      
      try {
        initializeClone(srcTableId, tableId, conn, bw);
        
        // the following loop looks changes in the file that occurred during the copy.. if files were dereferenced then they could have been GCed
        
        while (true) {
          int rewrites = checkClone(srcTableId, tableId, conn, bw);
          
          if (rewrites == 0)
            break;
        }
        
        bw.flush();
        break;
        
      } catch (TabletIterator.TabletDeletedException tde) {
        // tablets were merged in the src table
        bw.flush();
        
        // delete what we have cloned and try again
        deleteTable(tableId, false, SystemCredentials.get().getAsThrift(), null);
        
        log.debug("Tablets merged in table " + srcTableId + " while attempting to clone, trying again");
        
        UtilWaitThread.sleep(100);
      }
    }
    
    // delete the clone markers and create directory entries
    Scanner mscanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    mscanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange());
    mscanner.fetchColumnFamily(ClonedColumnFamily.NAME);
    
    int dirCount = 0;
    
    for (Entry<Key,Value> entry : mscanner) {
      Key k = entry.getKey();
      Mutation m = new Mutation(k.getRow());
      m.putDelete(k.getColumnFamily(), k.getColumnQualifier());
      TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(FastFormat.toZeroPaddedString(dirCount++, 8, 16, "/c-".getBytes())));
      bw.addMutation(m);
    }
    
    bw.close();
    
  }
  
  public static void chopped(KeyExtent extent, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    ChoppedColumnFamily.CHOPPED_COLUMN.put(m, new Value("chopped".getBytes()));
    update(SystemCredentials.get().getAsThrift(), zooLock, m, extent);
  }
  
  public static void removeBulkLoadEntries(Connector conn, String tableId, long tid) throws Exception {
    Scanner mscanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
    mscanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange());
    mscanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);
    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    for (Entry<Key,Value> entry : mscanner) {
      log.debug("Looking at entry " + entry + " with tid " + tid);
      if (Long.parseLong(entry.getValue().toString()) == tid) {
        log.debug("deleting entry " + entry);
        Mutation m = new Mutation(entry.getKey().getRow());
        m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
        bw.addMutation(m);
      }
    }
    bw.close();
  }
  
  public static List<FileRef> getBulkFilesLoaded(Connector conn, KeyExtent extent, long tid) throws IOException {
    List<FileRef> result = new ArrayList<FileRef>();
    try {
      VolumeManager fs = VolumeManagerImpl.get();
      Scanner mscanner = new IsolatedScanner(conn.createScanner(extent.isMeta() ? RootTable.NAME : MetadataTable.NAME, Authorizations.EMPTY));
      mscanner.setRange(extent.toMetadataRange());
      mscanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);
      for (Entry<Key,Value> entry : mscanner) {
        if (Long.parseLong(entry.getValue().toString()) == tid) {
          result.add(new FileRef(fs, entry.getKey()));
        }
      }
      return result;
    } catch (TableNotFoundException ex) {
      // unlikely
      throw new RuntimeException("Onos! teh metadata table has vanished!!");
    }
  }
  
  public static Map<FileRef,Long> getBulkFilesLoaded(TCredentials credentials, KeyExtent extent) throws IOException {
    Text metadataRow = extent.getMetadataEntry();
    Map<FileRef,Long> ret = new HashMap<FileRef,Long>();
    
    VolumeManager fs = VolumeManagerImpl.get();
    Scanner scanner = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, extent.isMeta() ? RootTable.ID : MetadataTable.ID, Authorizations.EMPTY);
    scanner.setRange(new Range(metadataRow));
    scanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);
    for (Entry<Key,Value> entry : scanner) {
      Long tid = Long.parseLong(entry.getValue().toString());
      ret.put(new FileRef(fs, entry.getKey()), tid);
    }
    return ret;
  }
  
  public static void addBulkLoadInProgressFlag(String path) {
    
    Mutation m = new Mutation(MetadataSchema.BlipSection.getRowPrefix() + path);
    m.put(EMPTY_TEXT, EMPTY_TEXT, new Value(new byte[] {}));
    
    // new KeyExtent is only added to force update to write to the metadata table, not the root table
    // because bulk loads aren't supported to the metadata table
    update(SystemCredentials.get().getAsThrift(), m, new KeyExtent(new Text("anythingNotMetadata"), null, null));
  }
  
  public static void removeBulkLoadInProgressFlag(String path) {
    
    Mutation m = new Mutation(MetadataSchema.BlipSection.getRowPrefix() + path);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
    
    // new KeyExtent is only added to force update to write to the metadata table, not the root table
    // because bulk loads aren't supported to the metadata table
    update(SystemCredentials.get().getAsThrift(), m, new KeyExtent(new Text("anythingNotMetadata"), null, null));
  }
  
  public static void moveMetaDeleteMarkers(Instance instance, TCredentials creds) {
    // move old delete markers to new location, to standardize table schema between all metadata tables
    byte[] EMPTY_BYTES = new byte[0];
    Scanner scanner = new ScannerImpl(instance, creds, RootTable.ID, Authorizations.EMPTY);
    String oldDeletesPrefix = "!!~del";
    Range oldDeletesRange = new Range(oldDeletesPrefix, true, "!!~dem", false);
    scanner.setRange(oldDeletesRange);
    for (Entry<Key,Value> entry : scanner) {
      String row = entry.getKey().getRow().toString();
      if (row.startsWith(oldDeletesPrefix)) {
        String filename = row.substring(oldDeletesPrefix.length());
        // add the new entry first
        log.info("Moving " + filename + " marker in " + RootTable.NAME);
        Mutation m = new Mutation(MetadataSchema.DeletesSection.getRowPrefix() + filename);
        m.put(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES);
        update(creds, m, null);
        // remove the old entry
        m = new Mutation(entry.getKey().getRow());
        m.putDelete(EMPTY_BYTES, EMPTY_BYTES);
        update(creds, m, null);
      } else {
        break;
      }
    }
    
  }
  
  public static SortedMap<Text,SortedMap<ColumnFQ,Value>> getTabletEntries(SortedMap<Key,Value> tabletKeyValues, List<ColumnFQ> columns) {
    TreeMap<Text,SortedMap<ColumnFQ,Value>> tabletEntries = new TreeMap<Text,SortedMap<ColumnFQ,Value>>();
    
    HashSet<ColumnFQ> colSet = null;
    if (columns != null) {
      colSet = new HashSet<ColumnFQ>(columns);
    }
    
    for (Entry<Key,Value> entry : tabletKeyValues.entrySet()) {
      
      if (columns != null && !colSet.contains(new ColumnFQ(entry.getKey()))) {
        continue;
      }
      
      Text row = entry.getKey().getRow();
      
      SortedMap<ColumnFQ,Value> colVals = tabletEntries.get(row);
      if (colVals == null) {
        colVals = new TreeMap<ColumnFQ,Value>();
        tabletEntries.put(row, colVals);
      }
      
      colVals.put(new ColumnFQ(entry.getKey()), entry.getValue());
    }
    
    return tabletEntries;
  }
  
}

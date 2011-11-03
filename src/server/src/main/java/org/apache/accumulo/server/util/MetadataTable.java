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
/**
 * provides a reference to the metadata table for updates by tablet servers
 */
package org.apache.accumulo.server.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.BatchWriterImpl;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.ThriftScanner;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.TabletOperations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooLock;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class MetadataTable extends org.apache.accumulo.core.util.MetadataTable {
  
  private static Map<AuthInfo,Writer> metadata_tables = new HashMap<AuthInfo,Writer>();
  private static final Logger log = Logger.getLogger(MetadataTable.class);
  
  private static final int SAVE_ROOT_TABLET_RETRIES = 3;
  
  private MetadataTable() {
    
  }
  
  public synchronized static Writer getMetadataTable(AuthInfo credentials) {
    Writer metadataTable = metadata_tables.get(credentials);
    if (metadataTable == null) {
      metadataTable = new Writer(HdfsZooInstance.getInstance(), credentials, Constants.METADATA_TABLE_ID);
      metadata_tables.put(credentials, metadataTable);
    }
    return metadataTable;
  }
  
  public static void putLockID(ZooLock zooLock, Mutation m) {
    ColumnFQ.put(m, Constants.METADATA_LOCK_COLUMN, new Value(zooLock.getLockID().serialize(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + "/").getBytes()));
  }
  
  public static void update(AuthInfo credentials, Mutation m) {
    update(credentials, null, m);
  }
  
  public static void update(AuthInfo credentials, ZooLock zooLock, Mutation m) {
    Writer t;
    t = getMetadataTable(credentials);
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
   * path should be relative to the table directory
   * 
   * @param time
   * @param zooLock
   * 
   */
  public static void updateTabletDataFile(KeyExtent extent, String path, DataFileValue dfv, String time, AuthInfo credentials, String address, ZooLock zooLock,
      Set<String> unusedWalLogs, TServerInstance lastLocation) {
    if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
      if (unusedWalLogs != null) {
        // unusedWalLogs will contain the location/name of each log in a log set
        // the log set is stored under one of the log names, but not both
        // find the entry under one of the names and delete it.
        ZooKeeper session = ZooSession.getSession();
        String root = getZookeeperLogLocation();
        boolean foundEntry = false;
        for (String entry : unusedWalLogs) {
          String[] parts = entry.split("/");
          String zpath = root + "/" + parts[1];
          while (true) {
            try {
              if (session.exists(zpath, null) != null) {
                session.delete(zpath, -1);
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
      m.put(Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(path), new Value(dfv.encode()));
      ColumnFQ.put(m, Constants.METADATA_TIME_COLUMN, new Value(time.getBytes()));
      // erase the old location
      if (lastLocation != null)
        lastLocation.clearLastLocation(m);
      // stuff in this location
      TServerInstance self = getTServerInstance(address, zooLock);
      self.putLastLocation(m);
    }
    if (unusedWalLogs != null) {
      for (String entry : unusedWalLogs) {
        m.putDelete(Constants.METADATA_LOG_COLUMN_FAMILY, new Text(entry));
      }
    }
    
    update(credentials, zooLock, m);
    
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
  
  public static void updateTabletDataFile(KeyExtent extent, Map<String,DataFileValue> estSizes, AuthInfo credentials, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    
    for (Entry<String,DataFileValue> entry : estSizes.entrySet()) {
      m.put(Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(entry.getKey()), new Value(entry.getValue().encode()));
    }
    
    update(credentials, zooLock, m);
  }
  
  public static void addTablet(KeyExtent extent, String path, AuthInfo credentials, char timeType, ZooLock lock) {
    Mutation m = extent.getPrevRowUpdateMutation();
    
    ColumnFQ.put(m, Constants.METADATA_DIRECTORY_COLUMN, new Value(path.getBytes()));
    ColumnFQ.put(m, Constants.METADATA_TIME_COLUMN, new Value((timeType + "0").getBytes()));
    
    update(credentials, lock, m);
  }
  
  public static void updateTabletPrevEndRow(KeyExtent extent, AuthInfo credentials) {
    Mutation m = extent.getPrevRowUpdateMutation(); //
    update(credentials, m);
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
      if (Constants.METADATA_DIRECTORY_COLUMN.equals(colf, colq))
        datafile = new Text(val.toString());
      
      else if (Constants.METADATA_PREV_ROW_COLUMN.equals(colf, colq))
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
  
  public static boolean getBatchFromRootTablet(AuthInfo credentials, Text startRow, SortedMap<Key,Value> results, SortedSet<Column> columns,
      boolean skipStartRow, int size) throws AccumuloSecurityException {
    while (true) {
      try {
        return ThriftScanner.getBatchFromServer(credentials, startRow, Constants.ROOT_TABLET_EXTENT, HdfsZooInstance.getInstance().getRootTabletLocation(),
            results, columns, skipStartRow, size, Constants.NO_AUTHS, true, AccumuloConfiguration.getSystemConfiguration());
      } catch (NotServingTabletException e) {
        UtilWaitThread.sleep(100);
      } catch (AccumuloException e) {
        UtilWaitThread.sleep(100);
      }
    }
    
  }
  
  /**
   * convenience method for reading a metadata tablet's data file entries from the root tablet
   * 
   */
  public static SortedMap<Key,Value> getRootMetadataDataFileEntries(KeyExtent extent, AuthInfo credentials) {
    SortedSet<Column> columns = new TreeSet<Column>();
    columns.add(new Column(TextUtil.getBytes(Constants.METADATA_DATAFILE_COLUMN_FAMILY), null, null));
    return getRootMetadataDataEntries(extent, columns, credentials);
  }
  
  public static SortedMap<Key,Value> getRootMetadataDataEntries(KeyExtent extent, SortedSet<Column> columns, AuthInfo credentials) {
    
    try {
      SortedMap<Key,Value> entries = new TreeMap<Key,Value>();
      
      Text metadataEntry = extent.getMetadataEntry();
      Text startRow;
      boolean more = getBatchFromRootTablet(credentials, metadataEntry, entries, columns, false, Constants.SCAN_BATCH_SIZE);
      
      while (more) {
        startRow = entries.lastKey().getRow(); // set end row
        more = getBatchFromRootTablet(credentials, startRow, entries, columns, false, Constants.SCAN_BATCH_SIZE);
      }
      
      Iterator<Key> iter = entries.keySet().iterator();
      while (iter.hasNext()) {
        Key key = iter.next();
        if (key.compareRow(metadataEntry) != 0) {
          iter.remove();
        }
      }
      
      return entries;
      
    } catch (AccumuloSecurityException e) {
      log.warn("Unauthorized access...");
      return new TreeMap<Key,Value>();
    }
    
  }
  
  public static boolean recordRootTabletLocation(String address) {
    for (int i = 0; i < SAVE_ROOT_TABLET_RETRIES; i++) {
      try {
        log.info("trying to write root tablet location to ZooKeeper as " + address);
        String zRootLocPath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZROOT_TABLET_LOCATION;
        ZooUtil.putPersistentData(zRootLocPath, address.getBytes(), NodeExistsPolicy.OVERWRITE);
        return true;
      } catch (Exception e) {
        log.error("Master: unable to save root tablet location in zookeeper. exception: " + e, e);
      }
    }
    log.error("Giving up after " + SAVE_ROOT_TABLET_RETRIES + " retries");
    return false;
  }
  
  public static SortedMap<String,DataFileValue> getDataFileSizes(KeyExtent extent, AuthInfo credentials) {
    TreeMap<String,DataFileValue> sizes = new TreeMap<String,DataFileValue>();
    
    Scanner mdScanner = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
    mdScanner.setRange(Constants.METADATA_KEYSPACE);
    mdScanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
    Text row = extent.getMetadataEntry();
    
    Key endKey = new Key(row, Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(""));
    endKey = endKey.followingKey(PartialKey.ROW_COLFAM);
    
    mdScanner.setRange(new Range(new Key(row), endKey));
    for (Entry<Key,Value> entry : mdScanner) {
      
      if (!entry.getKey().getRow().equals(row))
        break;
      DataFileValue dfv = new DataFileValue(entry.getValue().get());
      sizes.put(entry.getKey().getColumnQualifier().toString(), dfv);
    }
    
    return sizes;
  }
  
  public static void addNewTablet(KeyExtent extent, String path, TServerInstance location, Map<String,DataFileValue> datafileSizes, AuthInfo credentials,
      String time, ZooLock zooLock) {
    Mutation m = extent.getPrevRowUpdateMutation(); //
    
    ColumnFQ.put(m, Constants.METADATA_DIRECTORY_COLUMN, new Value(path.getBytes()));
    ColumnFQ.put(m, Constants.METADATA_TIME_COLUMN, new Value(time.getBytes()));
    if (location != null) {
      m.put(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, location.asColumnQualifier(), location.asMutationValue());
      m.putDelete(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, location.asColumnQualifier());
    }
    
    for (Entry<String,DataFileValue> entry : datafileSizes.entrySet()) {
      m.put(Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(entry.getKey()), new Value(entry.getValue().encode()));
    }
    
    update(credentials, zooLock, m);
  }
  
  public static void splitTablet(KeyExtent extent, Text oldPrevEndRow, double splitRatio, AuthInfo credentials, ZooLock zooLock) {
    Mutation m = extent.getPrevRowUpdateMutation(); //
    
    ColumnFQ.put(m, Constants.METADATA_SPLIT_RATIO_COLUMN, new Value(Double.toString(splitRatio).getBytes()));
    
    ColumnFQ.put(m, Constants.METADATA_OLD_PREV_ROW_COLUMN, KeyExtent.encodePrevEndRow(oldPrevEndRow));
    
    update(credentials, zooLock, m);
  }
  
  public static void finishSplit(Text metadataEntry, Map<String,DataFileValue> datafileSizes, List<String> highDatafilesToRemove, AuthInfo credentials,
      ZooLock zooLock) {
    Mutation m = new Mutation(metadataEntry);
    ColumnFQ.putDelete(m, Constants.METADATA_SPLIT_RATIO_COLUMN);
    ColumnFQ.putDelete(m, Constants.METADATA_OLD_PREV_ROW_COLUMN);
    
    for (Entry<String,DataFileValue> entry : datafileSizes.entrySet()) {
      m.put(Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(entry.getKey()), new Value(entry.getValue().encode()));
    }
    
    for (String pathToRemove : highDatafilesToRemove) {
      m.putDelete(Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(pathToRemove));
    }
    
    update(credentials, zooLock, m);
  }
  
  public static void finishSplit(KeyExtent extent, Map<String,DataFileValue> datafileSizes, List<String> highDatafilesToRemove, AuthInfo credentials,
      ZooLock zooLock) {
    finishSplit(extent.getMetadataEntry(), datafileSizes, highDatafilesToRemove, credentials, zooLock);
  }
  
  public static void replaceDatafiles(KeyExtent extent, Set<String> datafilesToDelete, Set<String> scanFiles, String path, DataFileValue size,
      AuthInfo credentials, String address, TServerInstance lastLocation, ZooLock zooLock) {
    replaceDatafiles(extent, datafilesToDelete, scanFiles, path, size, credentials, address, lastLocation, zooLock, true);
  }
  
  public static void replaceDatafiles(KeyExtent extent, Set<String> datafilesToDelete, Set<String> scanFiles, String path, DataFileValue size,
      AuthInfo credentials, String address, TServerInstance lastLocation, ZooLock zooLock, boolean insertDeleteFlags) {
    Text emptyText = new Text();
    
    if (insertDeleteFlags) {
      // add delete flags for those paths before the data file reference is removed
      for (String pathToRemove : datafilesToDelete) {
        Mutation delFlag = new Mutation(new Text(Constants.METADATA_DELETE_FLAG_PREFIX + "/" + extent.getTableId().toString() + pathToRemove));
        delFlag.put(emptyText, emptyText, new Value(new byte[] {}));
        update(credentials, delFlag);
      }
    }
    
    // replace data file references to old mapfiles with the new mapfiles
    Mutation m = new Mutation(extent.getMetadataEntry());
    
    for (String pathToRemove : datafilesToDelete)
      m.putDelete(Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(pathToRemove));
    
    for (String scanFile : scanFiles)
      m.put(Constants.METADATA_SCANFILE_COLUMN_FAMILY, new Text(scanFile), new Value("".getBytes()));
    
    if (size.getNumEntries() > 0)
      m.put(Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(path), new Value(size.encode()));
    
    // remove the old location
    if (lastLocation != null)
      lastLocation.clearLastLocation(m);
    TServerInstance self = getTServerInstance(address, zooLock);
    self.putLastLocation(m);
    
    update(credentials, zooLock, m);
  }
  
  public static void removeScanFiles(KeyExtent extent, Set<String> scanFiles, AuthInfo credentials, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    
    for (String pathToRemove : scanFiles)
      m.putDelete(Constants.METADATA_SCANFILE_COLUMN_FAMILY, new Text(pathToRemove));
    
    update(credentials, zooLock, m);
  }
  
  public static void getTabletAndPrevTabletKeyValues(SortedMap<Key,Value> tkv, KeyExtent ke, List<ColumnFQ> columns, AuthInfo credentials) {
    getTabletAndPrevTabletKeyValues(HdfsZooInstance.getInstance(), tkv, ke, columns, credentials);
  }
  
  public static SortedMap<Text,SortedMap<ColumnFQ,Value>> getTabletEntries(KeyExtent ke, List<ColumnFQ> columns, AuthInfo credentials) {
    return getTabletEntries(HdfsZooInstance.getInstance(), ke, columns, credentials);
  }
  
  private static KeyExtent fixSplit(Text table, Text metadataEntry, Text metadataPrevEndRow, Value oper, double splitRatio, TServerInstance tserver,
      AuthInfo credentials, String time, ZooLock lock) throws AccumuloException {
    if (metadataPrevEndRow == null)
      // something is wrong, this should not happen... if a tablet is split, it will always have a
      // prev end row....
      throw new AccumuloException("Split tablet does not have prev end row, something is amiss, extent = " + metadataEntry);
    
    KeyExtent low = null;
    
    List<String> highDatafilesToRemove = new ArrayList<String>();
    
    String lowDirectory = TabletOperations.createTabletDirectory(Constants.getTablesDir() + "/" + table, metadataPrevEndRow);
    
    Text prevPrevEndRow = KeyExtent.decodePrevEndRow(oper);
    
    low = new KeyExtent(table, metadataPrevEndRow, prevPrevEndRow);
    
    Scanner scanner3 = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
    Key rowKey = new Key(metadataEntry);
    
    SortedMap<String,DataFileValue> origDatafileSizes = new TreeMap<String,DataFileValue>();
    SortedMap<String,DataFileValue> highDatafileSizes = new TreeMap<String,DataFileValue>();
    SortedMap<String,DataFileValue> lowDatafileSizes = new TreeMap<String,DataFileValue>();
    scanner3.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
    scanner3.setRange(new Range(rowKey, rowKey.followingKey(PartialKey.ROW)));
    
    for (Entry<Key,Value> entry : scanner3) {
      if (entry.getKey().compareColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY) == 0) {
        origDatafileSizes.put(entry.getKey().getColumnQualifier().toString(), new DataFileValue(entry.getValue().get()));
      }
    }
    
    splitDatafiles(table, metadataPrevEndRow, splitRatio, new HashMap<String,FileUtil.FileInfo>(), origDatafileSizes, lowDatafileSizes, highDatafileSizes,
        highDatafilesToRemove);
    
    // check to see if prev tablet exist in metadata tablet
    Key prevRowKey = new Key(new Text(KeyExtent.getMetadataEntry(table, metadataPrevEndRow)));
    
    ScannerImpl scanner2 = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
    scanner2.setRange(new Range(prevRowKey, prevRowKey.followingKey(PartialKey.ROW)));
    
    if (!scanner2.iterator().hasNext()) {
      log.debug("Prev tablet " + prevRowKey + " does not exist, need to create it " + metadataPrevEndRow + " " + prevPrevEndRow + " " + splitRatio);
      MetadataTable.addNewTablet(low, lowDirectory, tserver, lowDatafileSizes, credentials, time, lock);
    } else {
      log.debug("Prev tablet " + prevRowKey + " exist, do not need to add it");
    }
    
    MetadataTable.finishSplit(metadataEntry, highDatafileSizes, highDatafilesToRemove, credentials, lock);
    
    return low;
  }
  
  public static void splitDatafiles(Text table, Text midRow, double splitRatio, Map<String,FileUtil.FileInfo> firstAndLastRows,
      SortedMap<String,DataFileValue> datafiles, SortedMap<String,DataFileValue> lowDatafileSizes, SortedMap<String,DataFileValue> highDatafileSizes,
      List<String> highDatafilesToRemove) {
    
    for (Entry<String,DataFileValue> entry : datafiles.entrySet()) {
      
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
        highDatafileSizes.put(entry.getKey(), new DataFileValue(highSize, highEntries));
      } else if (rowsKnown && lastRow.compareTo(midRow) <= 0) {
        // only in low
        long lowSize = entry.getValue().getSize();
        long lowEntries = entry.getValue().getNumEntries();
        lowDatafileSizes.put(entry.getKey(), new DataFileValue(lowSize, lowEntries));
        
        highDatafilesToRemove.add(entry.getKey());
      } else {
        long lowSize = (long) Math.floor((entry.getValue().getSize() * splitRatio));
        long lowEntries = (long) Math.floor((entry.getValue().getNumEntries() * splitRatio));
        lowDatafileSizes.put(entry.getKey(), new DataFileValue(lowSize, lowEntries));
        
        long highSize = (long) Math.ceil((entry.getValue().getSize() * (1.0 - splitRatio)));
        long highEntries = (long) Math.ceil((entry.getValue().getNumEntries() * (1.0 - splitRatio)));
        highDatafileSizes.put(entry.getKey(), new DataFileValue(highSize, highEntries));
      }
    }
  }
  
  public static KeyExtent fixSplit(Text metadataEntry, SortedMap<ColumnFQ,Value> columns, TServerInstance tserver, AuthInfo credentials, ZooLock lock)
      throws AccumuloException {
    log.warn("Incomplete split " + metadataEntry + " attempting to fix");
    
    Value oper = columns.get(Constants.METADATA_OLD_PREV_ROW_COLUMN);
    
    if (columns.get(Constants.METADATA_SPLIT_RATIO_COLUMN) == null) {
      log.warn("Metadata entry does not have split ratio (" + metadataEntry + ")");
      return null;
    }
    
    double splitRatio = Double.parseDouble(new String(columns.get(Constants.METADATA_SPLIT_RATIO_COLUMN).get()));
    
    Value prevEndRowIBW = columns.get(Constants.METADATA_PREV_ROW_COLUMN);
    
    if (prevEndRowIBW == null) {
      log.warn("Metadata entry does not have prev row (" + metadataEntry + ")");
      return null;
    }
    
    Value time = columns.get(Constants.METADATA_TIME_COLUMN);
    
    if (time == null) {
      log.warn("Metadata entry does not have time (" + metadataEntry + ")");
      return null;
    }
    
    Text metadataPrevEndRow = KeyExtent.decodePrevEndRow(prevEndRowIBW);
    
    Text table = (new KeyExtent(metadataEntry, (Text) null)).getTableId();
    
    return fixSplit(table, metadataEntry, metadataPrevEndRow, oper, splitRatio, tserver, credentials, time.toString(), lock);
  }
  
  public static void deleteTable(String table, AuthInfo credentials, ZooLock lock) throws AccumuloException {
    Scanner ms = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
    Text tableText = new Text(table);
    BatchWriter bw = new BatchWriterImpl(HdfsZooInstance.getInstance(), credentials, Constants.METADATA_TABLE_ID, 1000000, 120000l, 1);
    
    // scan metadata for our table and delete everything we find
    Mutation m = null;
    ms.setRange(new Range(new Key(new Text(KeyExtent.getMetadataEntry(tableText, new Text()))), true, Constants.METADATA_RESERVED_KEYSPACE_START_KEY, false));
    
    for (Entry<Key,Value> cell : ms) {
      Key key = cell.getKey();
      String rowTable = new String(KeyExtent.tableOfMetadataRow(key.getRow()));
      
      if (!rowTable.equals(table))
        break;
      
      if (m == null) {
        m = new Mutation(key.getRow());
        putLockID(lock, m);
      }
      
      if (key.getRow().compareTo(m.getRow(), 0, m.getRow().length) != 0) {
        bw.addMutation(m);
        m = new Mutation(key.getRow());
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
      out.writeUTF(filename);
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
    return ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZROOT_TABLET_WALOGS;
  }
  
  public static void addLogEntries(AuthInfo credentials, List<LogEntry> entries, ZooLock zooLock) {
    if (entries.size() == 0)
      return;
    // entries should be a complete log set, so we should only need to write the first entry
    LogEntry entry = entries.get(0);
    if (entry.extent.equals(Constants.ROOT_TABLET_EXTENT)) {
      String root = getZookeeperLogLocation();
      while (true) {
        try {
          if (ZooLock.isLockHeld(ZooSession.getSession(), zooLock.getLockID()))
            ZooUtil.putPersistentData(root + "/" + entry.filename, entry.toBytes(), NodeExistsPolicy.OVERWRITE);
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
      m.put(Constants.METADATA_LOG_COLUMN_FAMILY, new Text(entry.server + "/" + entry.filename), new Value(value.getBytes()));
      update(credentials, zooLock, m);
    }
  }
  
  public static LogEntry entryFromKeyValue(Key key, Value value) {
    MetadataTable.LogEntry e = new MetadataTable.LogEntry();
    e.extent = new KeyExtent(key.getRow(), new Text());
    String[] parts = key.getColumnQualifier().toString().split("/");
    e.server = parts[0];
    e.filename = parts[1];
    parts = value.toString().split("\\|");
    e.tabletId = Integer.parseInt(parts[1]);
    e.logSet = Arrays.asList(parts[0].split(";"));
    e.timestamp = key.getTimestamp();
    return e;
  }
  
  public static Pair<List<LogEntry>,SortedMap<String,DataFileValue>> getFileAndLogEntries(AuthInfo credentials, KeyExtent extent) throws KeeperException,
      InterruptedException, IOException {
    ArrayList<LogEntry> result = new ArrayList<LogEntry>();
    TreeMap<String,DataFileValue> sizes = new TreeMap<String,DataFileValue>();
    
    if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
      getRootLogEntries(result);
      FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
      FileStatus[] files = fs.listStatus(new Path(Constants.getRootTabletDir()));
      
      for (FileStatus fileStatus : files) {
        if (fileStatus.getPath().toString().endsWith("_tmp")) {
          continue;
        }
        DataFileValue dfv = new DataFileValue(0, 0);
        sizes.put(Constants.ZROOT_TABLET + "/" + fileStatus.getPath().getName(), dfv);
      }
      
    } else {
      Scanner scanner = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
      scanner.fetchColumnFamily(Constants.METADATA_LOG_COLUMN_FAMILY);
      scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
      scanner.setRange(extent.toMetadataRange());
      
      for (Entry<Key,Value> entry : scanner) {
        if (!entry.getKey().getRow().equals(extent.getMetadataEntry())) {
          throw new RuntimeException("Unexpected row " + entry.getKey().getRow() + " expected " + extent.getMetadataEntry());
        }
        
        if (entry.getKey().getColumnFamily().equals(Constants.METADATA_LOG_COLUMN_FAMILY)) {
          result.add(entryFromKeyValue(entry.getKey(), entry.getValue()));
        } else if (entry.getKey().getColumnFamily().equals(Constants.METADATA_DATAFILE_COLUMN_FAMILY)) {
          DataFileValue dfv = new DataFileValue(entry.getValue().get());
          sizes.put(entry.getKey().getColumnQualifier().toString(), dfv);
        } else {
          throw new RuntimeException("Unexpected col fam " + entry.getKey().getColumnFamily());
        }
      }
    }
    
    return new Pair<List<LogEntry>,SortedMap<String,DataFileValue>>(result, sizes);
  }
  
  public static List<LogEntry> getLogEntries(AuthInfo credentials, KeyExtent extent) throws IOException, KeeperException, InterruptedException {
    log.info("Scanning logging entries for " + extent);
    ArrayList<LogEntry> result = new ArrayList<LogEntry>();
    if (extent.equals(Constants.ROOT_TABLET_EXTENT)) {
      log.info("Getting logs for root tablet from zookeeper");
      getRootLogEntries(result);
    } else {
      log.info("Scanning metadata for logs used for tablet " + extent);
      Scanner scanner = getTabletLogScanner(credentials, extent);
      Text pattern = extent.getMetadataEntry();
      for (Entry<Key,Value> entry : scanner) {
        Text row = entry.getKey().getRow();
        if (entry.getKey().getColumnFamily().equals(Constants.METADATA_LOG_COLUMN_FAMILY)) {
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
    ZooKeeper session = ZooSession.getSession();
    String root = getZookeeperLogLocation();
    for (String child : session.getChildren(root, null)) {
      LogEntry e = new LogEntry();
      e.fromBytes(session.getData(root + "/" + child, null, null));
      result.add(e);
    }
  }
  
  private static Scanner getTabletLogScanner(AuthInfo credentials, KeyExtent extent) {
    Scanner scanner = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
    scanner.fetchColumnFamily(Constants.METADATA_LOG_COLUMN_FAMILY);
    Text start = extent.getMetadataEntry();
    Key endKey = new Key(start, Constants.METADATA_LOG_COLUMN_FAMILY);
    endKey = endKey.followingKey(PartialKey.ROW_COLFAM);
    scanner.setRange(new Range(new Key(start), endKey));
    return scanner;
  }
  
  static class LogEntryIterator implements Iterator<LogEntry> {
    
    Iterator<LogEntry> rootTabletEntries = null;
    Iterator<Entry<Key,Value>> metadataEntries = null;
    
    LogEntryIterator(AuthInfo creds) throws IOException, KeeperException, InterruptedException {
      rootTabletEntries = getLogEntries(creds, Constants.ROOT_TABLET_EXTENT).iterator();
      try {
        Scanner scanner = HdfsZooInstance.getInstance().getConnector(creds.user, creds.password)
            .createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
        scanner.fetchColumnFamily(Constants.METADATA_LOG_COLUMN_FAMILY);
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
  
  public static Iterator<LogEntry> getLogEntries(AuthInfo creds) throws IOException, KeeperException, InterruptedException {
    return new LogEntryIterator(creds);
  }
  
  public static void removeUnusedWALEntries(KeyExtent extent, List<LogEntry> logEntries, ZooLock zooLock) {
    for (LogEntry entry : logEntries) {
      if (entry.extent.equals(Constants.ROOT_TABLET_EXTENT)) {
        String root = getZookeeperLogLocation();
        while (true) {
          try {
            if (ZooLock.isLockHeld(ZooSession.getSession(), zooLock.getLockID()))
              ZooUtil.recursiveDelete(root + "/" + entry.filename, NodeMissingPolicy.SKIP);
            break;
          } catch (Exception e) {
            log.error(e, e);
          }
          UtilWaitThread.sleep(1000);
        }
      } else {
        Mutation m = new Mutation(entry.extent.getMetadataEntry());
        m.putDelete(Constants.METADATA_LOG_COLUMN_FAMILY, new Text(entry.server + "/" + entry.filename));
        update(SecurityConstants.systemCredentials, zooLock, m);
      }
    }
  }
  
}

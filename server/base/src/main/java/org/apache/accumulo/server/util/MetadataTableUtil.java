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

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
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

import org.apache.accumulo.core.Constants;
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
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Pair;
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
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

/**
 * provides a reference to the metadata table for updates by tablet servers
 */
public class MetadataTableUtil {

  private static final Text EMPTY_TEXT = new Text();
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static Map<Credentials,Writer> root_tables = new HashMap<Credentials,Writer>();
  private static Map<Credentials,Writer> metadata_tables = new HashMap<Credentials,Writer>();
  private static final Logger log = Logger.getLogger(MetadataTableUtil.class);

  private MetadataTableUtil() {}

  public synchronized static Writer getMetadataTable(Credentials credentials) {
    Writer metadataTable = metadata_tables.get(credentials);
    if (metadataTable == null) {
      metadataTable = new Writer(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID);
      metadata_tables.put(credentials, metadataTable);
    }
    return metadataTable;
  }

  private synchronized static Writer getRootTable(Credentials credentials) {
    Writer rootTable = root_tables.get(credentials);
    if (rootTable == null) {
      rootTable = new Writer(HdfsZooInstance.getInstance(), credentials, RootTable.ID);
      root_tables.put(credentials, rootTable);
    }
    return rootTable;
  }

  private static void putLockID(ZooLock zooLock, Mutation m) {
    TabletsSection.ServerColumnFamily.LOCK_COLUMN.put(m, new Value(zooLock.getLockID().serialize(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + "/")
        .getBytes(UTF_8)));
  }

  private static void update(Credentials credentials, Mutation m, KeyExtent extent) {
    update(credentials, null, m, extent);
  }

  public static void update(Credentials credentials, ZooLock zooLock, Mutation m, KeyExtent extent) {
    Writer t = extent.isMeta() ? getRootTable(credentials) : getMetadataTable(credentials);
    update(t, credentials, zooLock, m);
  }

  public static void update(Writer t, Credentials credentials, ZooLock zooLock, Mutation m) {
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
        // retrying when a CVE occurs is probably futile and can cause problems, see ACCUMULO-3096
        throw new RuntimeException(e);
      } catch (TableNotFoundException e) {
        log.error(e, e);
      }
      UtilWaitThread.sleep(1000);
    }
  }

  public static void updateTabletFlushID(KeyExtent extent, long flushID, Credentials credentials, ZooLock zooLock) {
    if (!extent.isRootTablet()) {
      Mutation m = new Mutation(extent.getMetadataEntry());
      TabletsSection.ServerColumnFamily.FLUSH_COLUMN.put(m, new Value((flushID + "").getBytes(UTF_8)));
      update(credentials, zooLock, m, extent);
    }
  }

  public static void updateTabletCompactID(KeyExtent extent, long compactID, Credentials credentials, ZooLock zooLock) {
    if (!extent.isRootTablet()) {
      Mutation m = new Mutation(extent.getMetadataEntry());
      TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(m, new Value((compactID + "").getBytes(UTF_8)));
      update(credentials, zooLock, m, extent);
    }
  }

  public static void updateTabletDataFile(long tid, KeyExtent extent, Map<FileRef,DataFileValue> estSizes, String time, Credentials credentials, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    byte[] tidBytes = Long.toString(tid).getBytes(UTF_8);

    for (Entry<FileRef,DataFileValue> entry : estSizes.entrySet()) {
      Text file = entry.getKey().meta();
      m.put(DataFileColumnFamily.NAME, file, new Value(entry.getValue().encode()));
      m.put(TabletsSection.BulkFileColumnFamily.NAME, file, new Value(tidBytes));
    }
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value(time.getBytes(UTF_8)));
    update(credentials, zooLock, m, extent);
  }

  public static void updateTabletDir(KeyExtent extent, String newDir, Credentials creds, ZooLock lock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(newDir.getBytes(UTF_8)));
    update(creds, lock, m, extent);
  }

  public static void addTablet(KeyExtent extent, String path, Credentials credentials, char timeType, ZooLock lock) {
    Mutation m = extent.getPrevRowUpdateMutation();

    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(path.getBytes(UTF_8)));
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value((timeType + "0").getBytes(UTF_8)));

    update(credentials, lock, m, extent);
  }

  public static void updateTabletPrevEndRow(KeyExtent extent, Credentials credentials) {
    Mutation m = extent.getPrevRowUpdateMutation(); //
    update(credentials, m, extent);
  }

  public static void updateTabletVolumes(KeyExtent extent, List<LogEntry> logsToRemove, List<LogEntry> logsToAdd, List<FileRef> filesToRemove,
      SortedMap<FileRef,DataFileValue> filesToAdd, String newDir, ZooLock zooLock, Credentials credentials) {

    if (extent.isRootTablet()) {
      if (newDir != null)
        throw new IllegalArgumentException("newDir not expected for " + extent);

      if (filesToRemove.size() != 0 || filesToAdd.size() != 0)
        throw new IllegalArgumentException("files not expected for " + extent);

      // add before removing in case of process death
      for (LogEntry logEntry : logsToAdd)
        addLogEntry(credentials, logEntry, zooLock);

      removeUnusedWALEntries(extent, logsToRemove, zooLock);
    } else {
      Mutation m = new Mutation(extent.getMetadataEntry());

      for (LogEntry logEntry : logsToRemove)
        m.putDelete(logEntry.getColumnFamily(), logEntry.getColumnQualifier());

      for (LogEntry logEntry : logsToAdd)
        m.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());

      for (FileRef fileRef : filesToRemove)
        m.putDelete(DataFileColumnFamily.NAME, fileRef.meta());

      for (Entry<FileRef,DataFileValue> entry : filesToAdd.entrySet())
        m.put(DataFileColumnFamily.NAME, entry.getKey().meta(), new Value(entry.getValue().encode()));

      if (newDir != null)
        ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(newDir.getBytes(UTF_8)));

      update(credentials, m, extent);
    }
  }

  public static SortedMap<FileRef,DataFileValue> getDataFileSizes(KeyExtent extent, Credentials credentials) throws IOException {
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

  public static void rollBackSplit(Text metadataEntry, Text oldPrevEndRow, Credentials credentials, ZooLock zooLock) {
    KeyExtent ke = new KeyExtent(metadataEntry, oldPrevEndRow);
    Mutation m = ke.getPrevRowUpdateMutation();
    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.putDelete(m);
    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.putDelete(m);
    update(credentials, zooLock, m, new KeyExtent(metadataEntry, (Text) null));
  }

  public static void splitTablet(KeyExtent extent, Text oldPrevEndRow, double splitRatio, Credentials credentials, ZooLock zooLock) {
    Mutation m = extent.getPrevRowUpdateMutation(); //

    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value(Double.toString(splitRatio).getBytes(UTF_8)));

    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m, KeyExtent.encodePrevEndRow(oldPrevEndRow));
    ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);
    update(credentials, zooLock, m, extent);
  }

  public static void finishSplit(Text metadataEntry, Map<FileRef,DataFileValue> datafileSizes, List<FileRef> highDatafilesToRemove, Credentials credentials,
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

  public static void finishSplit(KeyExtent extent, Map<FileRef,DataFileValue> datafileSizes, List<FileRef> highDatafilesToRemove, Credentials credentials,
      ZooLock zooLock) {
    finishSplit(extent.getMetadataEntry(), datafileSizes, highDatafilesToRemove, credentials, zooLock);
  }

  public static void addDeleteEntries(KeyExtent extent, Set<FileRef> datafilesToDelete, Credentials credentials) throws IOException {

    String tableId = extent.getTableId().toString();

    // TODO could use batch writer,would need to handle failure and retry like update does - ACCUMULO-1294
    for (FileRef pathToRemove : datafilesToDelete) {
      update(credentials, createDeleteMutation(tableId, pathToRemove.path().toString()), extent);
    }
  }

  public static void addDeleteEntry(String tableId, String path) throws IOException {
    update(SystemCredentials.get(), createDeleteMutation(tableId, path), new KeyExtent(new Text(tableId), null, null));
  }

  public static Mutation createDeleteMutation(String tableId, String pathToRemove) throws IOException {
    Path path = VolumeManagerImpl.get().getFullPath(tableId, pathToRemove);
    Mutation delFlag = new Mutation(new Text(MetadataSchema.DeletesSection.getRowPrefix() + path.toString()));
    delFlag.put(EMPTY_TEXT, EMPTY_TEXT, new Value(new byte[] {}));
    return delFlag;
  }

  public static void removeScanFiles(KeyExtent extent, Set<FileRef> scanFiles, Credentials credentials, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());

    for (FileRef pathToRemove : scanFiles)
      m.putDelete(ScanFileColumnFamily.NAME, pathToRemove.meta());

    update(credentials, zooLock, m, extent);
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

  public static void deleteTable(String tableId, boolean insertDeletes, Credentials credentials, ZooLock lock) throws AccumuloException, IOException {
    Scanner ms = new ScannerImpl(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID, Authorizations.EMPTY);
    Text tableIdText = new Text(tableId);
    BatchWriter bw = new BatchWriterImpl(HdfsZooInstance.getInstance(), credentials, MetadataTable.ID, new BatchWriterConfig().setMaxMemory(1000000)
        .setMaxLatency(120000l, TimeUnit.MILLISECONDS).setMaxWriteThreads(2));

    // scan metadata for our table and delete everything we find
    Mutation m = null;
    ms.setRange(new KeyExtent(tableIdText, null, null).toMetadataRange());

    // insert deletes before deleting data from metadata... this makes the code fault tolerant
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

  static String getZookeeperLogLocation() {
    return ZooUtil.getRoot(HdfsZooInstance.getInstance()) + RootTable.ZROOT_TABLET_WALOGS;
  }

  public static void addLogEntry(Credentials credentials, LogEntry entry, ZooLock zooLock) {
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
      Mutation m = new Mutation(entry.getRow());
      m.put(entry.getColumnFamily(), entry.getColumnQualifier(), entry.getValue());
      update(credentials, zooLock, m, entry.extent);
    }
  }

  public static void setRootTabletDir(String dir) throws IOException {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    String zpath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + RootTable.ZROOT_TABLET_PATH;
    try {
      zoo.putPersistentData(zpath, dir.getBytes(UTF_8), -1, NodeExistsPolicy.OVERWRITE);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  public static String getRootTabletDir() throws IOException {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    String zpath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + RootTable.ZROOT_TABLET_PATH;
    try {
      return new String(zoo.getData(zpath, null), UTF_8);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  public static Pair<List<LogEntry>,SortedMap<FileRef,DataFileValue>> getFileAndLogEntries(Credentials credentials, KeyExtent extent) throws KeeperException,
      InterruptedException, IOException {
    ArrayList<LogEntry> result = new ArrayList<LogEntry>();
    TreeMap<FileRef,DataFileValue> sizes = new TreeMap<FileRef,DataFileValue>();

    VolumeManager fs = VolumeManagerImpl.get();
    if (extent.isRootTablet()) {
      getRootLogEntries(result);
      Path rootDir = new Path(getRootTabletDir());
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
          result.add(LogEntry.fromKeyValue(entry.getKey(), entry.getValue()));
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

  public static List<LogEntry> getLogEntries(Credentials credentials, KeyExtent extent) throws IOException, KeeperException, InterruptedException {
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
            result.add(LogEntry.fromKeyValue(entry.getKey(), entry.getValue()));
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

  static void getRootLogEntries(ArrayList<LogEntry> result) throws KeeperException, InterruptedException, IOException {
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
          // upgrade from !0;!0<< -> +r<<
          e.extent = RootTable.EXTENT;
          result.add(e);
        } catch (KeeperException.NoNodeException ex) {
          continue;
        }
      }
      break;
    }
  }

  private static Scanner getTabletLogScanner(Credentials credentials, KeyExtent extent) {
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

  private static class LogEntryIterator implements Iterator<LogEntry> {

    Iterator<LogEntry> zookeeperEntries = null;
    Iterator<LogEntry> rootTableEntries = null;
    Iterator<Entry<Key,Value>> metadataEntries = null;

    LogEntryIterator(Credentials creds) throws IOException, KeeperException, InterruptedException {
      zookeeperEntries = getLogEntries(creds, RootTable.EXTENT).iterator();
      rootTableEntries = getLogEntries(creds, new KeyExtent(new Text(MetadataTable.ID), null, null)).iterator();
      try {
        Scanner scanner = HdfsZooInstance.getInstance().getConnector(creds.getPrincipal(), creds.getToken())
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
      return zookeeperEntries.hasNext() || rootTableEntries.hasNext() || metadataEntries.hasNext();
    }

    @Override
    public LogEntry next() {
      if (zookeeperEntries.hasNext()) {
        return zookeeperEntries.next();
      }
      if (rootTableEntries.hasNext()) {
        return rootTableEntries.next();
      }
      Entry<Key,Value> entry = metadataEntries.next();
      return LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  public static Iterator<LogEntry> getLogEntries(Credentials creds) throws IOException, KeeperException, InterruptedException {
    return new LogEntryIterator(creds);
  }

  public static void removeUnusedWALEntries(KeyExtent extent, List<LogEntry> logEntries, ZooLock zooLock) {
    if (extent.isRootTablet()) {
      for (LogEntry entry : logEntries) {
        String root = getZookeeperLogLocation();
        while (true) {
          try {
            IZooReaderWriter zoo = ZooReaderWriter.getInstance();
            if (zoo.isLockHeld(zooLock.getLockID())) {
              String parts[] = entry.filename.split("/");
              zoo.recursiveDelete(root + "/" + parts[parts.length - 1], NodeMissingPolicy.SKIP);
            }
            break;
          } catch (Exception e) {
            log.error(e, e);
          }
          UtilWaitThread.sleep(1000);
        }
      }
    } else {
      Mutation m = new Mutation(extent.getMetadataEntry());
      for (LogEntry entry : logEntries) {
        m.putDelete(LogColumnFamily.NAME, new Text(entry.getName()));
      }
      update(SystemCredentials.get(), zooLock, m, extent);
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
    String tableName = MetadataTable.NAME;
    if (tableId.equals(MetadataTable.ID))
      tableName = RootTable.NAME;
    Scanner mscanner = new IsolatedScanner(conn.createScanner(tableName, Authorizations.EMPTY));
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
    TabletIterator ti;
    if (srcTableId.equals(MetadataTable.ID))
      ti = new TabletIterator(createCloneScanner(srcTableId, conn), new Range(), true, true);
    else
      ti = new TabletIterator(createCloneScanner(srcTableId, conn), new KeyExtent(new Text(srcTableId), null, null).toMetadataRange(), true, true);

    if (!ti.hasNext())
      throw new RuntimeException(" table deleted during clone?  srcTableId = " + srcTableId);

    while (ti.hasNext())
      bw.addMutation(createCloneMutation(srcTableId, tableId, ti.next()));

    bw.flush();
  }

  private static int compareEndRows(Text endRow1, Text endRow2) {
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
        m.put(ClonedColumnFamily.NAME, new Text(""), new Value("OK".getBytes(UTF_8)));
        bw.addMutation(m);
      }
    }

    bw.flush();
    return rewrites;
  }

  public static void cloneTable(Instance instance, String srcTableId, String tableId, VolumeManager volumeManager) throws Exception {

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
        deleteTable(tableId, false, SystemCredentials.get(), null);

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
      String dir = volumeManager.choose(ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableId + Path.SEPARATOR
          + new String(FastFormat.toZeroPaddedString(dirCount++, 8, 16, Constants.CLONE_PREFIX_BYTES));
      TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(dir.getBytes(UTF_8)));

      bw.addMutation(m);
    }

    bw.close();

  }

  public static void chopped(KeyExtent extent, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    ChoppedColumnFamily.CHOPPED_COLUMN.put(m, new Value("chopped".getBytes(UTF_8)));
    update(SystemCredentials.get(), zooLock, m, extent);
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

  public static Map<FileRef,Long> getBulkFilesLoaded(Credentials credentials, KeyExtent extent) throws IOException {
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
    update(SystemCredentials.get(), m, new KeyExtent(new Text("anythingNotMetadata"), null, null));
  }

  public static void removeBulkLoadInProgressFlag(String path) {

    Mutation m = new Mutation(MetadataSchema.BlipSection.getRowPrefix() + path);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);

    // new KeyExtent is only added to force update to write to the metadata table, not the root table
    // because bulk loads aren't supported to the metadata table
    update(SystemCredentials.get(), m, new KeyExtent(new Text("anythingNotMetadata"), null, null));
  }

  /**
   * During an upgrade we need to move deletion requests for files under the !METADATA table to the root tablet.
   */
  public static void moveMetaDeleteMarkers(Instance instance, Credentials creds) {
    String oldDeletesPrefix = "!!~del";
    Range oldDeletesRange = new Range(oldDeletesPrefix, true, "!!~dem", false);

    // move old delete markers to new location, to standardize table schema between all metadata tables
    Scanner scanner = new ScannerImpl(instance, creds, RootTable.ID, Authorizations.EMPTY);
    scanner.setRange(oldDeletesRange);
    for (Entry<Key,Value> entry : scanner) {
      String row = entry.getKey().getRow().toString();
      if (row.startsWith(oldDeletesPrefix)) {
        moveDeleteEntry(creds, RootTable.OLD_EXTENT, entry, row, oldDeletesPrefix);
      } else {
        break;
      }
    }

  }

  public static void moveMetaDeleteMarkersFrom14(Instance instance, Credentials creds) {
    // new KeyExtent is only added to force update to write to the metadata table, not the root table
    KeyExtent notMetadata = new KeyExtent(new Text("anythingNotMetadata"), null, null);

    // move delete markers from the normal delete keyspace to the root tablet delete keyspace if the files are for the !METADATA table
    Scanner scanner = new ScannerImpl(instance, creds, MetadataTable.ID, Authorizations.EMPTY);
    scanner.setRange(MetadataSchema.DeletesSection.getRange());
    for (Entry<Key,Value> entry : scanner) {
      String row = entry.getKey().getRow().toString();
      if (row.startsWith(MetadataSchema.DeletesSection.getRowPrefix() + "/" + MetadataTable.ID)) {
        moveDeleteEntry(creds, notMetadata, entry, row, MetadataSchema.DeletesSection.getRowPrefix());
      } else {
        break;
      }
    }
  }

  private static void moveDeleteEntry(Credentials creds, KeyExtent oldExtent, Entry<Key,Value> entry, String rowID, String prefix) {
    String filename = rowID.substring(prefix.length());

    // add the new entry first
    log.info("Moving " + filename + " marker in " + RootTable.NAME);
    Mutation m = new Mutation(MetadataSchema.DeletesSection.getRowPrefix() + filename);
    m.put(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES);
    update(creds, m, RootTable.EXTENT);

    // then remove the old entry
    m = new Mutation(entry.getKey().getRow());
    m.putDelete(EMPTY_BYTES, EMPTY_BYTES);
    update(creds, m, oldExtent);
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

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
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.BatchWriterImpl;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
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
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * provides a reference to the metadata table for updates by tablet servers
 */
public class MetadataTableUtil {

  private static final Text EMPTY_TEXT = new Text();
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static Map<Credentials,Writer> root_tables = new HashMap<>();
  private static Map<Credentials,Writer> metadata_tables = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(MetadataTableUtil.class);

  private MetadataTableUtil() {}

  public synchronized static Writer getMetadataTable(ClientContext context) {
    Credentials credentials = context.getCredentials();
    Writer metadataTable = metadata_tables.get(credentials);
    if (metadataTable == null) {
      metadataTable = new Writer(context, MetadataTable.ID);
      metadata_tables.put(credentials, metadataTable);
    }
    return metadataTable;
  }

  public synchronized static Writer getRootTable(ClientContext context) {
    Credentials credentials = context.getCredentials();
    Writer rootTable = root_tables.get(credentials);
    if (rootTable == null) {
      rootTable = new Writer(context, RootTable.ID);
      root_tables.put(credentials, rootTable);
    }
    return rootTable;
  }

  public static void putLockID(ZooLock zooLock, Mutation m) {
    TabletsSection.ServerColumnFamily.LOCK_COLUMN.put(m, new Value(zooLock.getLockID().serialize(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + "/")
        .getBytes(UTF_8)));
  }

  private static void update(ClientContext context, Mutation m, KeyExtent extent) {
    update(context, null, m, extent);
  }

  public static void update(ClientContext context, ZooLock zooLock, Mutation m, KeyExtent extent) {
    Writer t = extent.isMeta() ? getRootTable(context) : getMetadataTable(context);
    update(t, zooLock, m);
  }

  public static void update(Writer t, ZooLock zooLock, Mutation m) {
    if (zooLock != null)
      putLockID(zooLock, m);
    while (true) {
      try {
        t.update(m);
        return;
      } catch (AccumuloException e) {
        log.error("{}", e.getMessage(), e);
      } catch (AccumuloSecurityException e) {
        log.error("{}", e.getMessage(), e);
      } catch (ConstraintViolationException e) {
        log.error("{}", e.getMessage(), e);
        // retrying when a CVE occurs is probably futile and can cause problems, see ACCUMULO-3096
        throw new RuntimeException(e);
      } catch (TableNotFoundException e) {
        log.error("{}", e.getMessage(), e);
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  public static void updateTabletFlushID(KeyExtent extent, long flushID, ClientContext context, ZooLock zooLock) {
    if (!extent.isRootTablet()) {
      Mutation m = new Mutation(extent.getMetadataEntry());
      TabletsSection.ServerColumnFamily.FLUSH_COLUMN.put(m, new Value((flushID + "").getBytes(UTF_8)));
      update(context, zooLock, m, extent);
    }
  }

  public static void updateTabletCompactID(KeyExtent extent, long compactID, ClientContext context, ZooLock zooLock) {
    if (!extent.isRootTablet()) {
      Mutation m = new Mutation(extent.getMetadataEntry());
      TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(m, new Value((compactID + "").getBytes(UTF_8)));
      update(context, zooLock, m, extent);
    }
  }

  public static void updateTabletDataFile(long tid, KeyExtent extent, Map<FileRef,DataFileValue> estSizes, String time, ClientContext context, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    byte[] tidBytes = Long.toString(tid).getBytes(UTF_8);

    for (Entry<FileRef,DataFileValue> entry : estSizes.entrySet()) {
      Text file = entry.getKey().meta();
      m.put(DataFileColumnFamily.NAME, file, new Value(entry.getValue().encode()));
      m.put(TabletsSection.BulkFileColumnFamily.NAME, file, new Value(tidBytes));
    }
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value(time.getBytes(UTF_8)));
    update(context, zooLock, m, extent);
  }

  public static void updateTabletDir(KeyExtent extent, String newDir, ClientContext context, ZooLock lock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(newDir.getBytes(UTF_8)));
    update(context, lock, m, extent);
  }

  public static void addTablet(KeyExtent extent, String path, ClientContext context, char timeType, ZooLock lock) {
    Mutation m = extent.getPrevRowUpdateMutation();

    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(path.getBytes(UTF_8)));
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(m, new Value((timeType + "0").getBytes(UTF_8)));

    update(context, lock, m, extent);
  }

  public static void updateTabletVolumes(KeyExtent extent, List<LogEntry> logsToRemove, List<LogEntry> logsToAdd, List<FileRef> filesToRemove,
      SortedMap<FileRef,DataFileValue> filesToAdd, String newDir, ZooLock zooLock, AccumuloServerContext context) {

    if (extent.isRootTablet()) {
      if (newDir != null)
        throw new IllegalArgumentException("newDir not expected for " + extent);

      if (filesToRemove.size() != 0 || filesToAdd.size() != 0)
        throw new IllegalArgumentException("files not expected for " + extent);

      // add before removing in case of process death
      for (LogEntry logEntry : logsToAdd)
        addRootLogEntry(context, zooLock, logEntry);

      removeUnusedWALEntries(context, extent, logsToRemove, zooLock);
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

      update(context, m, extent);
    }
  }

  private static interface ZooOperation {
    void run(IZooReaderWriter rw) throws KeeperException, InterruptedException, IOException;
  }

  private static void retryZooKeeperUpdate(ClientContext context, ZooLock zooLock, ZooOperation op) {
    while (true) {
      try {
        IZooReaderWriter zoo = ZooReaderWriter.getInstance();
        if (zoo.isLockHeld(zooLock.getLockID())) {
          op.run(zoo);
        }
        break;
      } catch (Exception e) {
        log.error("Unexpected exception {}", e.getMessage(), e);
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  private static void addRootLogEntry(AccumuloServerContext context, ZooLock zooLock, final LogEntry entry) {
    retryZooKeeperUpdate(context, zooLock, new ZooOperation() {
      @Override
      public void run(IZooReaderWriter rw) throws KeeperException, InterruptedException, IOException {
        String root = getZookeeperLogLocation();
        rw.putPersistentData(root + "/" + entry.getUniqueID(), entry.toBytes(), NodeExistsPolicy.OVERWRITE);
      }
    });
  }

  public static SortedMap<FileRef,DataFileValue> getDataFileSizes(KeyExtent extent, ClientContext context) throws IOException {
    TreeMap<FileRef,DataFileValue> sizes = new TreeMap<>();

    try (Scanner mdScanner = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY)) {
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
  }

  public static void rollBackSplit(Text metadataEntry, Text oldPrevEndRow, ClientContext context, ZooLock zooLock) {
    KeyExtent ke = new KeyExtent(metadataEntry, oldPrevEndRow);
    Mutation m = ke.getPrevRowUpdateMutation();
    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.putDelete(m);
    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.putDelete(m);
    update(context, zooLock, m, new KeyExtent(metadataEntry, (Text) null));
  }

  public static void splitTablet(KeyExtent extent, Text oldPrevEndRow, double splitRatio, ClientContext context, ZooLock zooLock) {
    Mutation m = extent.getPrevRowUpdateMutation(); //

    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value(Double.toString(splitRatio).getBytes(UTF_8)));

    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m, KeyExtent.encodePrevEndRow(oldPrevEndRow));
    ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);
    update(context, zooLock, m, extent);
  }

  public static void finishSplit(Text metadataEntry, Map<FileRef,DataFileValue> datafileSizes, List<FileRef> highDatafilesToRemove,
      final ClientContext context, ZooLock zooLock) {
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

    update(context, zooLock, m, new KeyExtent(metadataEntry, (Text) null));
  }

  public static void finishSplit(KeyExtent extent, Map<FileRef,DataFileValue> datafileSizes, List<FileRef> highDatafilesToRemove, ClientContext context,
      ZooLock zooLock) {
    finishSplit(extent.getMetadataEntry(), datafileSizes, highDatafilesToRemove, context, zooLock);
  }

  public static void addDeleteEntries(KeyExtent extent, Set<FileRef> datafilesToDelete, ClientContext context) throws IOException {

    Table.ID tableId = extent.getTableId();

    // TODO could use batch writer,would need to handle failure and retry like update does - ACCUMULO-1294
    for (FileRef pathToRemove : datafilesToDelete) {
      update(context, createDeleteMutation(tableId, pathToRemove.path().toString()), extent);
    }
  }

  public static void addDeleteEntry(AccumuloServerContext context, Table.ID tableId, String path) throws IOException {
    update(context, createDeleteMutation(tableId, path), new KeyExtent(tableId, null, null));
  }

  public static Mutation createDeleteMutation(Table.ID tableId, String pathToRemove) throws IOException {
    Path path = VolumeManagerImpl.get().getFullPath(tableId, pathToRemove);
    Mutation delFlag = new Mutation(new Text(MetadataSchema.DeletesSection.getRowPrefix() + path.toString()));
    delFlag.put(EMPTY_TEXT, EMPTY_TEXT, new Value(new byte[] {}));
    return delFlag;
  }

  public static void removeScanFiles(KeyExtent extent, Set<FileRef> scanFiles, ClientContext context, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());

    for (FileRef pathToRemove : scanFiles)
      m.putDelete(ScanFileColumnFamily.NAME, pathToRemove.meta());

    update(context, zooLock, m, extent);
  }

  public static void splitDatafiles(Text midRow, double splitRatio, Map<FileRef,FileUtil.FileInfo> firstAndLastRows,
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

  public static void deleteTable(Table.ID tableId, boolean insertDeletes, ClientContext context, ZooLock lock) throws AccumuloException, IOException {
    try (Scanner ms = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY);
        BatchWriter bw = new BatchWriterImpl(context, MetadataTable.ID, new BatchWriterConfig().setMaxMemory(1000000)
            .setMaxLatency(120000l, TimeUnit.MILLISECONDS).setMaxWriteThreads(2))) {

      // scan metadata for our table and delete everything we find
      Mutation m = null;
      ms.setRange(new KeyExtent(tableId, null, null).toMetadataRange());

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
    }
  }

  static String getZookeeperLogLocation() {
    return ZooUtil.getRoot(HdfsZooInstance.getInstance()) + RootTable.ZROOT_TABLET_WALOGS;
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

  public static Pair<List<LogEntry>,SortedMap<FileRef,DataFileValue>> getFileAndLogEntries(ClientContext context, KeyExtent extent) throws KeeperException,
      InterruptedException, IOException {
    ArrayList<LogEntry> result = new ArrayList<>();
    TreeMap<FileRef,DataFileValue> sizes = new TreeMap<>();

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
      Table.ID systemTableToCheck = extent.isMeta() ? RootTable.ID : MetadataTable.ID;
      try (Scanner scanner = new ScannerImpl(context, systemTableToCheck, Authorizations.EMPTY)) {
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
    }

    return new Pair<>(result, sizes);
  }

  public static List<LogEntry> getLogEntries(ClientContext context, KeyExtent extent) throws IOException, KeeperException, InterruptedException {
    log.info("Scanning logging entries for {}", extent);
    ArrayList<LogEntry> result = new ArrayList<>();
    if (extent.equals(RootTable.EXTENT)) {
      log.info("Getting logs for root tablet from zookeeper");
      getRootLogEntries(result);
    } else {
      log.info("Scanning metadata for logs used for tablet {}", extent);
      Scanner scanner = getTabletLogScanner(context, extent);
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

    log.info("Returning logs {} for extent {}", result, extent);
    return result;
  }

  static void getRootLogEntries(final ArrayList<LogEntry> result) throws KeeperException, InterruptedException, IOException {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    String root = getZookeeperLogLocation();
    // there's a little race between getting the children and fetching
    // the data. The log can be removed in between.
    while (true) {
      result.clear();
      for (String child : zoo.getChildren(root)) {
        try {
          LogEntry e = LogEntry.fromBytes(zoo.getData(root + "/" + child, null));
          // upgrade from !0;!0<< -> +r<<
          e = new LogEntry(RootTable.EXTENT, 0, e.server, e.filename);
          result.add(e);
        } catch (KeeperException.NoNodeException ex) {
          continue;
        }
      }
      break;
    }
  }

  private static Scanner getTabletLogScanner(ClientContext context, KeyExtent extent) {
    Table.ID tableId = MetadataTable.ID;
    if (extent.isMeta())
      tableId = RootTable.ID;
    Scanner scanner = new ScannerImpl(context, tableId, Authorizations.EMPTY);
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

    LogEntryIterator(ClientContext context) throws IOException, KeeperException, InterruptedException {
      zookeeperEntries = getLogEntries(context, RootTable.EXTENT).iterator();
      rootTableEntries = getLogEntries(context, new KeyExtent(MetadataTable.ID, null, null)).iterator();
      try {
        Scanner scanner = context.getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        log.info("Setting range to {}", MetadataSchema.TabletsSection.getRange());
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

  public static Iterator<LogEntry> getLogEntries(ClientContext context) throws IOException, KeeperException, InterruptedException {
    return new LogEntryIterator(context);
  }

  public static void removeUnusedWALEntries(AccumuloServerContext context, KeyExtent extent, final List<LogEntry> entries, ZooLock zooLock) {
    if (extent.isRootTablet()) {
      retryZooKeeperUpdate(context, zooLock, new ZooOperation() {
        @Override
        public void run(IZooReaderWriter rw) throws KeeperException, InterruptedException, IOException {
          String root = getZookeeperLogLocation();
          for (LogEntry entry : entries) {
            String path = root + "/" + entry.getUniqueID();
            log.debug("Removing " + path + " from zookeeper");
            rw.recursiveDelete(path, NodeMissingPolicy.SKIP);
          }
        }
      });
    } else {
      Mutation m = new Mutation(extent.getMetadataEntry());
      for (LogEntry entry : entries) {
        m.putDelete(entry.getColumnFamily(), entry.getColumnQualifier());
      }
      update(context, zooLock, m, extent);
    }
  }

  private static void getFiles(Set<String> files, Map<Key,Value> tablet, Table.ID srcTableId) {
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

  private static Mutation createCloneMutation(Table.ID srcTableId, Table.ID tableId, Map<Key,Value> tablet) {

    KeyExtent ke = new KeyExtent(tablet.keySet().iterator().next().getRow(), (Text) null);
    Mutation m = new Mutation(KeyExtent.getMetadataEntry(tableId, ke.getEndRow()));

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

  private static Scanner createCloneScanner(String tableName, Table.ID tableId, Connector conn) throws TableNotFoundException {
    if (tableId.equals(MetadataTable.ID))
      tableName = RootTable.NAME;
    Scanner mscanner = new IsolatedScanner(conn.createScanner(tableName, Authorizations.EMPTY));
    mscanner.setRange(new KeyExtent(tableId, null, null).toMetadataRange());
    mscanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    mscanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
    mscanner.fetchColumnFamily(TabletsSection.LastLocationColumnFamily.NAME);
    mscanner.fetchColumnFamily(ClonedColumnFamily.NAME);
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(mscanner);
    TabletsSection.ServerColumnFamily.TIME_COLUMN.fetch(mscanner);
    return mscanner;
  }

  @VisibleForTesting
  public static void initializeClone(String tableName, Table.ID srcTableId, Table.ID tableId, Connector conn, BatchWriter bw) throws TableNotFoundException,
      MutationsRejectedException {
    TabletIterator ti;
    if (srcTableId.equals(MetadataTable.ID))
      ti = new TabletIterator(createCloneScanner(tableName, srcTableId, conn), new Range(), true, true);
    else
      ti = new TabletIterator(createCloneScanner(tableName, srcTableId, conn), new KeyExtent(srcTableId, null, null).toMetadataRange(), true, true);

    if (!ti.hasNext())
      throw new RuntimeException(" table deleted during clone?  srcTableId = " + srcTableId);

    while (ti.hasNext())
      bw.addMutation(createCloneMutation(srcTableId, tableId, ti.next()));

    bw.flush();
  }

  private static int compareEndRows(Text endRow1, Text endRow2) {
    return new KeyExtent(Table.ID.of("0"), endRow1, null).compareTo(new KeyExtent(Table.ID.of("0"), endRow2, null));
  }

  @VisibleForTesting
  public static int checkClone(String tableName, Table.ID srcTableId, Table.ID tableId, Connector conn, BatchWriter bw) throws TableNotFoundException,
      MutationsRejectedException {
    TabletIterator srcIter = new TabletIterator(createCloneScanner(tableName, srcTableId, conn), new KeyExtent(srcTableId, null, null).toMetadataRange(), true,
        true);
    TabletIterator cloneIter = new TabletIterator(createCloneScanner(tableName, tableId, conn), new KeyExtent(tableId, null, null).toMetadataRange(), true,
        true);

    if (!cloneIter.hasNext() || !srcIter.hasNext())
      throw new RuntimeException(" table deleted during clone?  srcTableId = " + srcTableId + " tableId=" + tableId);

    int rewrites = 0;

    while (cloneIter.hasNext()) {
      Map<Key,Value> cloneTablet = cloneIter.next();
      Text cloneEndRow = new KeyExtent(cloneTablet.keySet().iterator().next().getRow(), (Text) null).getEndRow();
      HashSet<String> cloneFiles = new HashSet<>();

      boolean cloneSuccessful = false;
      for (Entry<Key,Value> entry : cloneTablet.entrySet()) {
        if (entry.getKey().getColumnFamily().equals(ClonedColumnFamily.NAME)) {
          cloneSuccessful = true;
          break;
        }
      }

      if (!cloneSuccessful)
        getFiles(cloneFiles, cloneTablet, null);

      List<Map<Key,Value>> srcTablets = new ArrayList<>();
      Map<Key,Value> srcTablet = srcIter.next();
      srcTablets.add(srcTablet);

      Text srcEndRow = new KeyExtent(srcTablet.keySet().iterator().next().getRow(), (Text) null).getEndRow();

      int cmp = compareEndRows(cloneEndRow, srcEndRow);
      if (cmp < 0)
        throw new TabletIterator.TabletDeletedException("Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);

      HashSet<String> srcFiles = new HashSet<>();
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

  public static void cloneTable(ClientContext context, Table.ID srcTableId, Table.ID tableId, VolumeManager volumeManager) throws Exception {

    Connector conn = context.getConnector();
    try (BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig())) {

      while (true) {

        try {
          initializeClone(MetadataTable.NAME, srcTableId, tableId, conn, bw);

          // the following loop looks changes in the file that occurred during the copy.. if files were dereferenced then they could have been GCed

          while (true) {
            int rewrites = checkClone(MetadataTable.NAME, srcTableId, tableId, conn, bw);

            if (rewrites == 0)
              break;
          }

          bw.flush();
          break;

        } catch (TabletIterator.TabletDeletedException tde) {
          // tablets were merged in the src table
          bw.flush();

          // delete what we have cloned and try again
          deleteTable(tableId, false, context, null);

          log.debug("Tablets merged in table {} while attempting to clone, trying again", srcTableId);

          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      }

      // delete the clone markers and create directory entries
      Scanner mscanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      mscanner.setRange(new KeyExtent(tableId, null, null).toMetadataRange());
      mscanner.fetchColumnFamily(ClonedColumnFamily.NAME);

      int dirCount = 0;

      for (Entry<Key,Value> entry : mscanner) {
        Key k = entry.getKey();
        Mutation m = new Mutation(k.getRow());
        m.putDelete(k.getColumnFamily(), k.getColumnQualifier());
        VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(tableId);
        String dir = volumeManager.choose(chooserEnv, ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableId + Path.SEPARATOR
            + new String(FastFormat.toZeroPaddedString(dirCount++, 8, 16, Constants.CLONE_PREFIX_BYTES));
        TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(dir.getBytes(UTF_8)));

        bw.addMutation(m);
      }
    }
  }

  public static void chopped(AccumuloServerContext context, KeyExtent extent, ZooLock zooLock) {
    Mutation m = new Mutation(extent.getMetadataEntry());
    ChoppedColumnFamily.CHOPPED_COLUMN.put(m, new Value("chopped".getBytes(UTF_8)));
    update(context, zooLock, m, extent);
  }

  public static void removeBulkLoadEntries(Connector conn, Table.ID tableId, long tid) throws Exception {
    try (Scanner mscanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
        BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig())) {
      mscanner.setRange(new KeyExtent(tableId, null, null).toMetadataRange());
      mscanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);
      byte[] tidAsBytes = Long.toString(tid).getBytes(UTF_8);
      for (Entry<Key,Value> entry : mscanner) {
        log.trace("Looking at entry {} with tid {}", entry, tid);
        if (Arrays.equals(entry.getValue().get(), tidAsBytes)) {
          log.trace("deleting entry {}", entry);
          Key key = entry.getKey();
          Mutation m = new Mutation(key.getRow());
          m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
          bw.addMutation(m);
        }
      }
    }
  }

  public static List<FileRef> getBulkFilesLoaded(Connector conn, KeyExtent extent, long tid) throws IOException {
    List<FileRef> result = new ArrayList<>();
    try (Scanner mscanner = new IsolatedScanner(conn.createScanner(extent.isMeta() ? RootTable.NAME : MetadataTable.NAME, Authorizations.EMPTY))) {
      VolumeManager fs = VolumeManagerImpl.get();
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

  public static Map<Long,? extends Collection<FileRef>> getBulkFilesLoaded(ClientContext context, KeyExtent extent) throws IOException {
    Text metadataRow = extent.getMetadataEntry();
    Map<Long,List<FileRef>> result = new HashMap<>();

    VolumeManager fs = VolumeManagerImpl.get();
    try (Scanner scanner = new ScannerImpl(context, extent.isMeta() ? RootTable.ID : MetadataTable.ID, Authorizations.EMPTY)) {
      scanner.setRange(new Range(metadataRow));
      scanner.fetchColumnFamily(TabletsSection.BulkFileColumnFamily.NAME);
      for (Entry<Key,Value> entry : scanner) {
        Long tid = Long.parseLong(entry.getValue().toString());
        List<FileRef> lst = result.get(tid);
        if (lst == null) {
          result.put(tid, lst = new ArrayList<>());
        }
        lst.add(new FileRef(fs, entry.getKey()));
      }
    }
    return result;
  }

  public static void addBulkLoadInProgressFlag(AccumuloServerContext context, String path) {

    Mutation m = new Mutation(MetadataSchema.BlipSection.getRowPrefix() + path);
    m.put(EMPTY_TEXT, EMPTY_TEXT, new Value(new byte[] {}));

    // new KeyExtent is only added to force update to write to the metadata table, not the root table
    // because bulk loads aren't supported to the metadata table
    update(context, m, new KeyExtent(Table.ID.of("anythingNotMetadata"), null, null));
  }

  public static void removeBulkLoadInProgressFlag(AccumuloServerContext context, String path) {

    Mutation m = new Mutation(MetadataSchema.BlipSection.getRowPrefix() + path);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);

    // new KeyExtent is only added to force update to write to the metadata table, not the root table
    // because bulk loads aren't supported to the metadata table
    update(context, m, new KeyExtent(Table.ID.of("anythingNotMetadata"), null, null));
  }

  /**
   * During an upgrade from 1.6 to 1.7, we need to add the replication table
   */
  public static void createReplicationTable(ClientContext context) throws IOException {

    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(ReplicationTable.ID);
    String dir = VolumeManagerImpl.get().choose(chooserEnv, ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + ReplicationTable.ID
        + Constants.DEFAULT_TABLET_LOCATION;

    Mutation m = new Mutation(new Text(KeyExtent.getMetadataEntry(ReplicationTable.ID, null)));
    m.put(DIRECTORY_COLUMN.getColumnFamily(), DIRECTORY_COLUMN.getColumnQualifier(), 0, new Value(dir.getBytes(UTF_8)));
    m.put(TIME_COLUMN.getColumnFamily(), TIME_COLUMN.getColumnQualifier(), 0, new Value((TabletTime.LOGICAL_TIME_ID + "0").getBytes(UTF_8)));
    m.put(PREV_ROW_COLUMN.getColumnFamily(), PREV_ROW_COLUMN.getColumnQualifier(), 0, KeyExtent.encodePrevEndRow(null));
    update(getMetadataTable(context), null, m);
  }

  /**
   * During an upgrade we need to move deletion requests for files under the !METADATA table to the root tablet.
   */
  public static void moveMetaDeleteMarkers(ClientContext context) {
    String oldDeletesPrefix = "!!~del";
    Range oldDeletesRange = new Range(oldDeletesPrefix, true, "!!~dem", false);

    // move old delete markers to new location, to standardize table schema between all metadata tables
    try (Scanner scanner = new ScannerImpl(context, RootTable.ID, Authorizations.EMPTY)) {
      scanner.setRange(oldDeletesRange);
      for (Entry<Key,Value> entry : scanner) {
        String row = entry.getKey().getRow().toString();
        if (row.startsWith(oldDeletesPrefix)) {
          moveDeleteEntry(context, RootTable.OLD_EXTENT, entry, row, oldDeletesPrefix);
        } else {
          break;
        }
      }
    }
  }

  public static void moveMetaDeleteMarkersFrom14(ClientContext context) {
    // new KeyExtent is only added to force update to write to the metadata table, not the root table
    KeyExtent notMetadata = new KeyExtent(Table.ID.of("anythingNotMetadata"), null, null);

    // move delete markers from the normal delete keyspace to the root tablet delete keyspace if the files are for the !METADATA table
    try (Scanner scanner = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY)) {
      scanner.setRange(MetadataSchema.DeletesSection.getRange());
      for (Entry<Key,Value> entry : scanner) {
        String row = entry.getKey().getRow().toString();
        if (row.startsWith(MetadataSchema.DeletesSection.getRowPrefix() + "/" + MetadataTable.ID)) {
          moveDeleteEntry(context, notMetadata, entry, row, MetadataSchema.DeletesSection.getRowPrefix());
        } else {
          break;
        }
      }
    }
  }

  private static void moveDeleteEntry(ClientContext context, KeyExtent oldExtent, Entry<Key,Value> entry, String rowID, String prefix) {
    String filename = rowID.substring(prefix.length());

    // add the new entry first
    log.info("Moving {} marker in {}", filename, RootTable.NAME);
    Mutation m = new Mutation(MetadataSchema.DeletesSection.getRowPrefix() + filename);
    m.put(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES);
    update(context, m, RootTable.EXTENT);

    // then remove the old entry
    m = new Mutation(entry.getKey().getRow());
    m.putDelete(EMPTY_BYTES, EMPTY_BYTES);
    update(context, m, oldExtent);
  }

  public static SortedMap<Text,SortedMap<ColumnFQ,Value>> getTabletEntries(SortedMap<Key,Value> tabletKeyValues, List<ColumnFQ> columns) {
    TreeMap<Text,SortedMap<ColumnFQ,Value>> tabletEntries = new TreeMap<>();

    HashSet<ColumnFQ> colSet = null;
    if (columns != null) {
      colSet = new HashSet<>(columns);
    }

    for (Entry<Key,Value> entry : tabletKeyValues.entrySet()) {
      ColumnFQ currentKey = new ColumnFQ(entry.getKey());
      if (columns != null && !colSet.contains(currentKey)) {
        continue;
      }

      Text row = entry.getKey().getRow();

      SortedMap<ColumnFQ,Value> colVals = tabletEntries.get(row);
      if (colVals == null) {
        colVals = new TreeMap<>();
        tabletEntries.put(row, colVals);
      }

      colVals.put(currentKey, entry.getValue());
    }

    return tabletEntries;
  }

}

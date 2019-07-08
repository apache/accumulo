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
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.CLONED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
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
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.BatchWriterImpl;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.clientImpl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.metadata.ServerAmpleImpl;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * provides a reference to the metadata table for updates by tablet servers
 */
public class MetadataTableUtil {

  public static final Text EMPTY_TEXT = new Text();
  private static Map<Credentials,Writer> root_tables = new HashMap<>();
  private static Map<Credentials,Writer> metadata_tables = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(MetadataTableUtil.class);

  private MetadataTableUtil() {}

  public static synchronized Writer getMetadataTable(ServerContext context) {
    Credentials credentials = context.getCredentials();
    Writer metadataTable = metadata_tables.get(credentials);
    if (metadataTable == null) {
      metadataTable = new Writer(context, MetadataTable.ID);
      metadata_tables.put(credentials, metadataTable);
    }
    return metadataTable;
  }

  public static synchronized Writer getRootTable(ServerContext context) {
    Credentials credentials = context.getCredentials();
    Writer rootTable = root_tables.get(credentials);
    if (rootTable == null) {
      rootTable = new Writer(context, RootTable.ID);
      root_tables.put(credentials, rootTable);
    }
    return rootTable;
  }

  public static void putLockID(ServerContext context, ZooLock zooLock, Mutation m) {
    TabletsSection.ServerColumnFamily.LOCK_COLUMN.put(m,
        new Value(zooLock.getLockID().serialize(context.getZooKeeperRoot() + "/").getBytes(UTF_8)));
  }

  private static void update(ServerContext context, Mutation m, KeyExtent extent) {
    update(context, null, m, extent);
  }

  public static void update(ServerContext context, ZooLock zooLock, Mutation m, KeyExtent extent) {
    Writer t = extent.isMeta() ? getRootTable(context) : getMetadataTable(context);
    update(context, t, zooLock, m);
  }

  public static void update(ServerContext context, Writer t, ZooLock zooLock, Mutation m) {
    if (zooLock != null)
      putLockID(context, zooLock, m);
    while (true) {
      try {
        t.update(m);
        return;
      } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
        log.error("{}", e.getMessage(), e);
      } catch (ConstraintViolationException e) {
        log.error("{}", e.getMessage(), e);
        // retrying when a CVE occurs is probably futile and can cause problems, see ACCUMULO-3096
        throw new RuntimeException(e);
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  public static void updateTabletFlushID(KeyExtent extent, long flushID, ServerContext context,
      ZooLock zooLock) {
    if (!extent.isRootTablet()) {
      TabletMutator tablet = context.getAmple().mutateTablet(extent);
      tablet.putFlushId(flushID);
      tablet.putZooLock(zooLock);
      tablet.mutate();
    }
  }

  public static void updateTabletCompactID(KeyExtent extent, long compactID, ServerContext context,
      ZooLock zooLock) {
    if (!extent.isRootTablet()) {
      TabletMutator tablet = context.getAmple().mutateTablet(extent);
      tablet.putCompactionId(compactID);
      tablet.putZooLock(zooLock);
      tablet.mutate();
    }
  }

  public static void updateTabletDataFile(long tid, KeyExtent extent,
      Map<FileRef,DataFileValue> estSizes, String time, ServerContext context, ZooLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putTime(time);
    estSizes.forEach(tablet::putFile);

    for (FileRef file : estSizes.keySet()) {
      tablet.putBulkFile(file, tid);
    }
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  public static void updateTabletDir(KeyExtent extent, String newDir, ServerContext context,
      ZooLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putDir(newDir);
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  public static void addTablet(KeyExtent extent, String path, ServerContext context, char timeType,
      ZooLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putPrevEndRow(extent.getPrevEndRow());
    tablet.putDir(path);
    tablet.putTime(timeType + "0");
    tablet.putZooLock(zooLock);
    tablet.mutate();

  }

  public static void updateTabletVolumes(KeyExtent extent, List<LogEntry> logsToRemove,
      List<LogEntry> logsToAdd, List<FileRef> filesToRemove,
      SortedMap<FileRef,DataFileValue> filesToAdd, String newDir, ZooLock zooLock,
      ServerContext context) {

    if (extent.isRootTablet()) {
      if (newDir != null)
        throw new IllegalArgumentException("newDir not expected for " + extent);

      if (filesToRemove.size() != 0 || filesToAdd.size() != 0)
        throw new IllegalArgumentException("files not expected for " + extent);
    }

    TabletMutator tabletMutator = context.getAmple().mutateTablet(extent);
    logsToRemove.forEach(tabletMutator::deleteWal);
    logsToAdd.forEach(tabletMutator::putWal);

    filesToRemove.forEach(tabletMutator::deleteFile);
    filesToAdd.forEach(tabletMutator::putFile);

    if (newDir != null)
      tabletMutator.putDir(newDir);

    tabletMutator.putZooLock(zooLock);

    tabletMutator.mutate();
  }

  public static SortedMap<FileRef,DataFileValue> getDataFileSizes(KeyExtent extent,
      ServerContext context) {
    TreeMap<FileRef,DataFileValue> sizes = new TreeMap<>();

    try (Scanner mdScanner = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY)) {
      mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      Text row = extent.getMetadataEntry();

      Key endKey = new Key(row, DataFileColumnFamily.NAME, new Text(""));
      endKey = endKey.followingKey(PartialKey.ROW_COLFAM);

      mdScanner.setRange(new Range(new Key(row), endKey));
      for (Entry<Key,Value> entry : mdScanner) {

        if (!entry.getKey().getRow().equals(row))
          break;
        DataFileValue dfv = new DataFileValue(entry.getValue().get());
        sizes.put(new FileRef(context.getVolumeManager(), entry.getKey()), dfv);
      }

      return sizes;
    }
  }

  public static void rollBackSplit(Text metadataEntry, Text oldPrevEndRow, ServerContext context,
      ZooLock zooLock) {
    KeyExtent ke = new KeyExtent(metadataEntry, oldPrevEndRow);
    Mutation m = ke.getPrevRowUpdateMutation();
    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.putDelete(m);
    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.putDelete(m);
    update(context, zooLock, m, new KeyExtent(metadataEntry, (Text) null));
  }

  public static void splitTablet(KeyExtent extent, Text oldPrevEndRow, double splitRatio,
      ServerContext context, ZooLock zooLock) {
    Mutation m = extent.getPrevRowUpdateMutation(); //

    TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m,
        new Value(Double.toString(splitRatio).getBytes(UTF_8)));

    TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m,
        KeyExtent.encodePrevEndRow(oldPrevEndRow));
    ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);
    update(context, zooLock, m, extent);
  }

  public static void finishSplit(Text metadataEntry, Map<FileRef,DataFileValue> datafileSizes,
      List<FileRef> highDatafilesToRemove, final ServerContext context, ZooLock zooLock) {
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

  public static void finishSplit(KeyExtent extent, Map<FileRef,DataFileValue> datafileSizes,
      List<FileRef> highDatafilesToRemove, ServerContext context, ZooLock zooLock) {
    finishSplit(extent.getMetadataEntry(), datafileSizes, highDatafilesToRemove, context, zooLock);
  }

  public static void addDeleteEntries(KeyExtent extent, Set<FileRef> datafilesToDelete,
      ServerContext context) {

    TableId tableId = extent.getTableId();

    // TODO could use batch writer,would need to handle failure and retry like update does -
    // ACCUMULO-1294
    for (FileRef pathToRemove : datafilesToDelete) {
      update(context,
          ServerAmpleImpl.createDeleteMutation(context, tableId, pathToRemove.path().toString()),
          extent);
    }
  }

  public static void addDeleteEntry(ServerContext context, TableId tableId, String path) {
    update(context, ServerAmpleImpl.createDeleteMutation(context, tableId, path),
        new KeyExtent(tableId, null, null));
  }

  public static void removeScanFiles(KeyExtent extent, Set<FileRef> scanFiles,
      ServerContext context, ZooLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    scanFiles.forEach(tablet::deleteScan);
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  public static void splitDatafiles(Text midRow, double splitRatio,
      Map<FileRef,FileUtil.FileInfo> firstAndLastRows, SortedMap<FileRef,DataFileValue> datafiles,
      SortedMap<FileRef,DataFileValue> lowDatafileSizes,
      SortedMap<FileRef,DataFileValue> highDatafileSizes, List<FileRef> highDatafilesToRemove) {

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
        highDatafileSizes.put(entry.getKey(),
            new DataFileValue(highSize, highEntries, entry.getValue().getTime()));
      } else if (rowsKnown && lastRow.compareTo(midRow) <= 0) {
        // only in low
        long lowSize = entry.getValue().getSize();
        long lowEntries = entry.getValue().getNumEntries();
        lowDatafileSizes.put(entry.getKey(),
            new DataFileValue(lowSize, lowEntries, entry.getValue().getTime()));

        highDatafilesToRemove.add(entry.getKey());
      } else {
        long lowSize = (long) Math.floor((entry.getValue().getSize() * splitRatio));
        long lowEntries = (long) Math.floor((entry.getValue().getNumEntries() * splitRatio));
        lowDatafileSizes.put(entry.getKey(),
            new DataFileValue(lowSize, lowEntries, entry.getValue().getTime()));

        long highSize = (long) Math.ceil((entry.getValue().getSize() * (1.0 - splitRatio)));
        long highEntries =
            (long) Math.ceil((entry.getValue().getNumEntries() * (1.0 - splitRatio)));
        highDatafileSizes.put(entry.getKey(),
            new DataFileValue(highSize, highEntries, entry.getValue().getTime()));
      }
    }
  }

  public static void deleteTable(TableId tableId, boolean insertDeletes, ServerContext context,
      ZooLock lock) throws AccumuloException {
    try (Scanner ms = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY);
        BatchWriter bw = new BatchWriterImpl(context, MetadataTable.ID,
            new BatchWriterConfig().setMaxMemory(1000000)
                .setMaxLatency(120000L, TimeUnit.MILLISECONDS).setMaxWriteThreads(2))) {

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
            FileRef ref = new FileRef(context.getVolumeManager(), key);
            bw.addMutation(
                ServerAmpleImpl.createDeleteMutation(context, tableId, ref.meta().toString()));
          }

          if (TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
            bw.addMutation(
                ServerAmpleImpl.createDeleteMutation(context, tableId, cell.getValue().toString()));
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
            putLockID(context, lock, m);
        }

        if (key.getRow().compareTo(m.getRow(), 0, m.getRow().length) != 0) {
          bw.addMutation(m);
          m = new Mutation(key.getRow());
          if (lock != null)
            putLockID(context, lock, m);
        }
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
      }

      if (m != null)
        bw.addMutation(m);
    }
  }

  public static Pair<List<LogEntry>,SortedMap<FileRef,DataFileValue>>
      getFileAndLogEntries(ServerContext context, KeyExtent extent)
          throws KeeperException, InterruptedException, IOException {
    ArrayList<LogEntry> result = new ArrayList<>();
    TreeMap<FileRef,DataFileValue> sizes = new TreeMap<>();

    VolumeManager fs = context.getVolumeManager();

    TabletMetadata tablet = context.getAmple().readTablet(extent, FILES, LOGS, PREV_ROW, DIR);

    if (!tablet.getExtent().equals(extent))
      throw new RuntimeException("Unexpected extent " + tablet.getExtent() + " expected " + extent);

    result.addAll(tablet.getLogs());

    if (extent.isRootTablet()) {
      Preconditions.checkState(tablet.getFiles().isEmpty(),
          "Saw unexpected files in root tablet metadata %s", tablet.getFiles());

      FileStatus[] files = fs.listStatus(new Path(tablet.getDir()));
      for (FileStatus fileStatus : files) {
        if (fileStatus.getPath().toString().endsWith("_tmp")) {
          continue;
        }
        DataFileValue dfv = new DataFileValue(0, 0);
        sizes.put(new FileRef(fileStatus.getPath().toString(), fileStatus.getPath()), dfv);
      }
    } else {
      tablet.getFilesMap().forEach((k, v) -> {
        sizes.put(new FileRef(k, fs.getFullPath(tablet.getTableId(), k)), v);
      });
    }

    return new Pair<>(result, sizes);
  }

  public static void removeUnusedWALEntries(ServerContext context, KeyExtent extent,
      final List<LogEntry> entries, ZooLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    entries.forEach(tablet::deleteWal);
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  private static void getFiles(Set<String> files, Collection<String> tabletFiles,
      TableId srcTableId) {
    for (String file : tabletFiles) {
      if (srcTableId != null && !file.startsWith("../") && !file.contains(":")) {
        file = "../" + srcTableId + file;
      }
      files.add(file);
    }
  }

  private static Mutation createCloneMutation(TableId srcTableId, TableId tableId,
      Map<Key,Value> tablet) {

    KeyExtent ke = new KeyExtent(tablet.keySet().iterator().next().getRow(), (Text) null);
    Mutation m = new Mutation(TabletsSection.getRow(tableId, ke.getEndRow()));

    for (Entry<Key,Value> entry : tablet.entrySet()) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        String cf = entry.getKey().getColumnQualifier().toString();
        if (!cf.startsWith("../") && !cf.contains(":"))
          cf = "../" + srcTableId + entry.getKey().getColumnQualifier();
        m.put(entry.getKey().getColumnFamily(), new Text(cf), entry.getValue());
      } else if (entry.getKey().getColumnFamily()
          .equals(TabletsSection.CurrentLocationColumnFamily.NAME)) {
        m.put(TabletsSection.LastLocationColumnFamily.NAME, entry.getKey().getColumnQualifier(),
            entry.getValue());
      } else if (entry.getKey().getColumnFamily()
          .equals(TabletsSection.LastLocationColumnFamily.NAME)) {
        // skip
      } else {
        m.put(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(),
            entry.getValue());
      }
    }
    return m;
  }

  private static Iterable<TabletMetadata> createCloneScanner(String testTableName, TableId tableId,
      AccumuloClient client) throws TableNotFoundException {

    String tableName;
    Range range;

    if (testTableName != null) {
      tableName = testTableName;
      range = TabletsSection.getRange(tableId);
    } else if (tableId.equals(MetadataTable.ID)) {
      tableName = RootTable.NAME;
      range = TabletsSection.getRange();
    } else {
      tableName = MetadataTable.NAME;
      range = TabletsSection.getRange(tableId);
    }

    return TabletsMetadata.builder().scanTable(tableName).overRange(range).checkConsistency()
        .saveKeyValues().fetch(FILES, LOCATION, LAST, CLONED, PREV_ROW, TIME).build(client);
  }

  @VisibleForTesting
  public static void initializeClone(String testTableName, TableId srcTableId, TableId tableId,
      AccumuloClient client, BatchWriter bw)
      throws TableNotFoundException, MutationsRejectedException {

    Iterator<TabletMetadata> ti = createCloneScanner(testTableName, srcTableId, client).iterator();

    if (!ti.hasNext())
      throw new RuntimeException(" table deleted during clone?  srcTableId = " + srcTableId);

    while (ti.hasNext())
      bw.addMutation(createCloneMutation(srcTableId, tableId, ti.next().getKeyValues()));

    bw.flush();
  }

  private static int compareEndRows(Text endRow1, Text endRow2) {
    return new KeyExtent(TableId.of("0"), endRow1, null)
        .compareTo(new KeyExtent(TableId.of("0"), endRow2, null));
  }

  @VisibleForTesting
  public static int checkClone(String testTableName, TableId srcTableId, TableId tableId,
      AccumuloClient client, BatchWriter bw)
      throws TableNotFoundException, MutationsRejectedException {

    Iterator<TabletMetadata> srcIter =
        createCloneScanner(testTableName, srcTableId, client).iterator();
    Iterator<TabletMetadata> cloneIter =
        createCloneScanner(testTableName, tableId, client).iterator();

    if (!cloneIter.hasNext() || !srcIter.hasNext())
      throw new RuntimeException(
          " table deleted during clone?  srcTableId = " + srcTableId + " tableId=" + tableId);

    int rewrites = 0;

    while (cloneIter.hasNext()) {
      TabletMetadata cloneTablet = cloneIter.next();
      Text cloneEndRow = cloneTablet.getEndRow();
      HashSet<String> cloneFiles = new HashSet<>();

      boolean cloneSuccessful = cloneTablet.getCloned() != null;

      if (!cloneSuccessful)
        getFiles(cloneFiles, cloneTablet.getFiles(), null);

      List<TabletMetadata> srcTablets = new ArrayList<>();
      TabletMetadata srcTablet = srcIter.next();
      srcTablets.add(srcTablet);

      Text srcEndRow = srcTablet.getEndRow();
      int cmp = compareEndRows(cloneEndRow, srcEndRow);
      if (cmp < 0)
        throw new TabletDeletedException(
            "Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);

      HashSet<String> srcFiles = new HashSet<>();
      if (!cloneSuccessful)
        getFiles(srcFiles, srcTablet.getFiles(), srcTableId);

      while (cmp > 0) {
        srcTablet = srcIter.next();
        srcTablets.add(srcTablet);
        srcEndRow = srcTablet.getEndRow();
        cmp = compareEndRows(cloneEndRow, srcEndRow);
        if (cmp < 0)
          throw new TabletDeletedException(
              "Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);

        if (!cloneSuccessful)
          getFiles(srcFiles, srcTablet.getFiles(), srcTableId);
      }

      if (cloneSuccessful)
        continue;

      if (!srcFiles.containsAll(cloneFiles)) {
        // delete existing cloned tablet entry
        Mutation m = new Mutation(cloneTablet.getExtent().getMetadataEntry());

        for (Entry<Key,Value> entry : cloneTablet.getKeyValues().entrySet()) {
          Key k = entry.getKey();
          m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), k.getTimestamp());
        }

        bw.addMutation(m);

        for (TabletMetadata st : srcTablets)
          bw.addMutation(createCloneMutation(srcTableId, tableId, st.getKeyValues()));

        rewrites++;
      } else {
        // write out marker that this tablet was successfully cloned
        Mutation m = new Mutation(cloneTablet.getExtent().getMetadataEntry());
        m.put(ClonedColumnFamily.NAME, new Text(""), new Value("OK".getBytes(UTF_8)));
        bw.addMutation(m);
      }
    }

    bw.flush();
    return rewrites;
  }

  public static void cloneTable(ServerContext context, TableId srcTableId, TableId tableId,
      VolumeManager volumeManager) throws Exception {

    try (BatchWriter bw = context.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig())) {

      while (true) {

        try {
          initializeClone(null, srcTableId, tableId, context, bw);

          // the following loop looks changes in the file that occurred during the copy.. if files
          // were dereferenced then they could have been GCed

          while (true) {
            int rewrites = checkClone(null, srcTableId, tableId, context, bw);

            if (rewrites == 0)
              break;
          }

          bw.flush();
          break;

        } catch (TabletDeletedException tde) {
          // tablets were merged in the src table
          bw.flush();

          // delete what we have cloned and try again
          deleteTable(tableId, false, context, null);

          log.debug("Tablets merged in table {} while attempting to clone, trying again",
              srcTableId);

          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      }

      // delete the clone markers and create directory entries
      Scanner mscanner = context.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      mscanner.setRange(new KeyExtent(tableId, null, null).toMetadataRange());
      mscanner.fetchColumnFamily(ClonedColumnFamily.NAME);

      int dirCount = 0;

      for (Entry<Key,Value> entry : mscanner) {
        Key k = entry.getKey();
        Mutation m = new Mutation(k.getRow());
        m.putDelete(k.getColumnFamily(), k.getColumnQualifier());
        VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironmentImpl(tableId,
            new KeyExtent(k.getRow(), (Text) null).getEndRow(), context);
        String dir = volumeManager.choose(chooserEnv, ServerConstants.getBaseUris(context))
            + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableId + Path.SEPARATOR + new String(
                FastFormat.toZeroPaddedString(dirCount++, 8, 16, Constants.CLONE_PREFIX_BYTES));
        TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(dir.getBytes(UTF_8)));

        bw.addMutation(m);
      }
    }
  }

  public static void chopped(ServerContext context, KeyExtent extent, ZooLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putChopped();
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  public static void removeBulkLoadEntries(AccumuloClient client, TableId tableId, long tid)
      throws Exception {
    try (
        Scanner mscanner =
            new IsolatedScanner(client.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
        BatchWriter bw = client.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig())) {
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

  public static List<FileRef> getBulkFilesLoaded(ServerContext context, AccumuloClient client,
      KeyExtent extent, long tid) {
    List<FileRef> result = new ArrayList<>();
    try (Scanner mscanner = new IsolatedScanner(client.createScanner(
        extent.isMeta() ? RootTable.NAME : MetadataTable.NAME, Authorizations.EMPTY))) {
      VolumeManager fs = context.getVolumeManager();
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

  public static Map<Long,? extends Collection<FileRef>> getBulkFilesLoaded(ServerContext context,
      KeyExtent extent) {
    Text metadataRow = extent.getMetadataEntry();
    Map<Long,List<FileRef>> result = new HashMap<>();

    VolumeManager fs = context.getVolumeManager();
    try (Scanner scanner = new ScannerImpl(context,
        extent.isMeta() ? RootTable.ID : MetadataTable.ID, Authorizations.EMPTY)) {
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

  public static void addBulkLoadInProgressFlag(ServerContext context, String path) {

    Mutation m = new Mutation(MetadataSchema.BlipSection.getRowPrefix() + path);
    m.put(EMPTY_TEXT, EMPTY_TEXT, new Value(new byte[] {}));

    // new KeyExtent is only added to force update to write to the metadata table, not the root
    // table
    // because bulk loads aren't supported to the metadata table
    update(context, m, new KeyExtent(TableId.of("anythingNotMetadata"), null, null));
  }

  public static void removeBulkLoadInProgressFlag(ServerContext context, String path) {

    Mutation m = new Mutation(MetadataSchema.BlipSection.getRowPrefix() + path);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);

    // new KeyExtent is only added to force update to write to the metadata table, not the root
    // table
    // because bulk loads aren't supported to the metadata table
    update(context, m, new KeyExtent(TableId.of("anythingNotMetadata"), null, null));
  }

  public static SortedMap<Text,SortedMap<ColumnFQ,Value>>
      getTabletEntries(SortedMap<Key,Value> tabletKeyValues, List<ColumnFQ> columns) {
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

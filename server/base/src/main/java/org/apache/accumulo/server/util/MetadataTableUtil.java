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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.CLONED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.BatchWriterImpl;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.clientImpl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.BlipSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

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

  public static void putLockID(ServerContext context, ServiceLock zooLock, Mutation m) {
    ServerColumnFamily.LOCK_COLUMN.put(m,
        new Value(zooLock.getLockID().serialize(context.getZooKeeperRoot() + "/")));
  }

  private static void update(ServerContext context, Mutation m, KeyExtent extent) {
    update(context, null, m, extent);
  }

  public static void update(ServerContext context, ServiceLock zooLock, Mutation m,
      KeyExtent extent) {
    Writer t = extent.isMeta() ? getRootTable(context) : getMetadataTable(context);
    update(context, t, zooLock, m, extent);
  }

  public static void update(ServerContext context, Writer t, ServiceLock zooLock, Mutation m,
      KeyExtent extent) {
    if (zooLock != null) {
      putLockID(context, zooLock, m);
    }
    while (true) {
      try {
        t.update(m);
        return;
      } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
        logUpdateFailure(m, extent, e);
      } catch (ConstraintViolationException e) {
        logUpdateFailure(m, extent, e);
        // retrying when a CVE occurs is probably futile and can cause problems, see ACCUMULO-3096
        throw new RuntimeException(e);
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  private static void logUpdateFailure(Mutation m, KeyExtent extent, Exception e) {
    log.error("Failed to write metadata updates for extent {} {}", extent, m.prettyPrint(), e);
  }

  public static void updateTabletFlushID(KeyExtent extent, long flushID, ServerContext context,
      ServiceLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putFlushId(flushID);
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  public static void updateTabletCompactID(KeyExtent extent, long compactID, ServerContext context,
      ServiceLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putCompactionId(compactID);
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  public static Map<StoredTabletFile,DataFileValue> updateTabletDataFile(long tid, KeyExtent extent,
      Map<TabletFile,DataFileValue> estSizes, MetadataTime time, ServerContext context,
      ServiceLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putTime(time);

    Map<StoredTabletFile,DataFileValue> newFiles = new HashMap<>(estSizes.size());
    estSizes.forEach((tf, dfv) -> {
      tablet.putFile(tf, dfv);
      tablet.putBulkFile(tf, tid);
      newFiles.put(tf.insert(), dfv);
    });
    tablet.putZooLock(zooLock);
    tablet.mutate();
    return newFiles;
  }

  public static void updateTabletDir(KeyExtent extent, String newDir, ServerContext context,
      ServiceLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putDirName(newDir);
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  public static void addTablet(KeyExtent extent, String path, ServerContext context,
      TimeType timeType, ServiceLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    tablet.putPrevEndRow(extent.prevEndRow());
    tablet.putDirName(path);
    tablet.putTime(new MetadataTime(0, timeType));
    tablet.putZooLock(zooLock);
    tablet.mutate();

  }

  public static void updateTabletVolumes(KeyExtent extent, List<LogEntry> logsToRemove,
      List<LogEntry> logsToAdd, List<StoredTabletFile> filesToRemove,
      SortedMap<TabletFile,DataFileValue> filesToAdd, ServiceLock zooLock, ServerContext context) {

    TabletMutator tabletMutator = context.getAmple().mutateTablet(extent);
    logsToRemove.forEach(tabletMutator::deleteWal);
    logsToAdd.forEach(tabletMutator::putWal);

    filesToRemove.forEach(tabletMutator::deleteFile);
    filesToAdd.forEach(tabletMutator::putFile);

    tabletMutator.putZooLock(zooLock);

    tabletMutator.mutate();
  }

  public static void rollBackSplit(Text metadataEntry, Text oldPrevEndRow, ServerContext context,
      ServiceLock zooLock) {
    KeyExtent ke = KeyExtent.fromMetaRow(metadataEntry, oldPrevEndRow);
    Mutation m = TabletColumnFamily.createPrevRowMutation(ke);
    TabletColumnFamily.SPLIT_RATIO_COLUMN.putDelete(m);
    TabletColumnFamily.OLD_PREV_ROW_COLUMN.putDelete(m);
    update(context, zooLock, m, KeyExtent.fromMetaRow(metadataEntry));
  }

  public static void splitTablet(KeyExtent extent, Text oldPrevEndRow, double splitRatio,
      ServerContext context, ServiceLock zooLock, Set<ExternalCompactionId> ecids) {
    Mutation m = TabletColumnFamily.createPrevRowMutation(extent);

    TabletColumnFamily.SPLIT_RATIO_COLUMN.put(m, new Value(Double.toString(splitRatio)));

    TabletColumnFamily.OLD_PREV_ROW_COLUMN.put(m,
        TabletColumnFamily.encodePrevEndRow(oldPrevEndRow));
    ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);

    ecids.forEach(ecid -> m.putDelete(ExternalCompactionColumnFamily.STR_NAME, ecid.canonical()));

    update(context, zooLock, m, extent);
  }

  public static void finishSplit(Text metadataEntry,
      Map<StoredTabletFile,DataFileValue> datafileSizes,
      List<StoredTabletFile> highDatafilesToRemove, final ServerContext context,
      ServiceLock zooLock) {
    Mutation m = new Mutation(metadataEntry);
    TabletColumnFamily.SPLIT_RATIO_COLUMN.putDelete(m);
    TabletColumnFamily.OLD_PREV_ROW_COLUMN.putDelete(m);
    ChoppedColumnFamily.CHOPPED_COLUMN.putDelete(m);

    for (Entry<StoredTabletFile,DataFileValue> entry : datafileSizes.entrySet()) {
      m.put(DataFileColumnFamily.NAME, entry.getKey().getMetaInsertText(),
          new Value(entry.getValue().encode()));
    }

    for (StoredTabletFile pathToRemove : highDatafilesToRemove) {
      m.putDelete(DataFileColumnFamily.NAME, pathToRemove.getMetaUpdateDeleteText());
    }

    update(context, zooLock, m, KeyExtent.fromMetaRow(metadataEntry));
  }

  public static void finishSplit(KeyExtent extent,
      Map<StoredTabletFile,DataFileValue> datafileSizes,
      List<StoredTabletFile> highDatafilesToRemove, ServerContext context, ServiceLock zooLock) {
    finishSplit(extent.toMetaRow(), datafileSizes, highDatafilesToRemove, context, zooLock);
  }

  public static void removeScanFiles(KeyExtent extent, Set<StoredTabletFile> scanFiles,
      ServerContext context, ServiceLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    scanFiles.forEach(tablet::deleteScan);
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  public static void splitDatafiles(Text midRow, double splitRatio,
      Map<TabletFile,FileUtil.FileInfo> firstAndLastRows,
      SortedMap<StoredTabletFile,DataFileValue> datafiles,
      SortedMap<StoredTabletFile,DataFileValue> lowDatafileSizes,
      SortedMap<StoredTabletFile,DataFileValue> highDatafileSizes,
      List<StoredTabletFile> highDatafilesToRemove) {

    for (Entry<StoredTabletFile,DataFileValue> entry : datafiles.entrySet()) {

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
      ServiceLock lock) throws AccumuloException {
    try (Scanner ms = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY);
        BatchWriter bw = new BatchWriterImpl(context, MetadataTable.ID,
            new BatchWriterConfig().setMaxMemory(1000000)
                .setMaxLatency(120000L, TimeUnit.MILLISECONDS).setMaxWriteThreads(2))) {

      // scan metadata for our table and delete everything we find
      Mutation m = null;
      Ample ample = context.getAmple();
      ms.setRange(new KeyExtent(tableId, null, null).toMetaRange());

      // insert deletes before deleting data from metadata... this makes the code fault tolerant
      if (insertDeletes) {

        ms.fetchColumnFamily(DataFileColumnFamily.NAME);
        ServerColumnFamily.DIRECTORY_COLUMN.fetch(ms);

        for (Entry<Key,Value> cell : ms) {
          Key key = cell.getKey();

          if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            StoredTabletFile stf = new StoredTabletFile(key.getColumnQualifierData().toString());
            bw.addMutation(
                ample.createDeleteMutation(new ReferenceFile(tableId, stf.getMetaUpdateDelete())));
          }

          if (ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
            var uri = new AllVolumesDirectory(tableId, cell.getValue().toString());
            bw.addMutation(ample.createDeleteMutation(uri));
          }
        }

        bw.flush();

        ms.clearColumns();
      }

      for (Entry<Key,Value> cell : ms) {
        Key key = cell.getKey();

        if (m == null) {
          m = new Mutation(key.getRow());
          if (lock != null) {
            putLockID(context, lock, m);
          }
        }

        if (key.getRow().compareTo(m.getRow(), 0, m.getRow().length) != 0) {
          bw.addMutation(m);
          m = new Mutation(key.getRow());
          if (lock != null) {
            putLockID(context, lock, m);
          }
        }
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
      }

      if (m != null) {
        bw.addMutation(m);
      }
    }
  }

  public static Pair<List<LogEntry>,SortedMap<StoredTabletFile,DataFileValue>>
      getFileAndLogEntries(ServerContext context, KeyExtent extent) throws IOException {
    ArrayList<LogEntry> result = new ArrayList<>();
    TreeMap<StoredTabletFile,DataFileValue> sizes = new TreeMap<>();

    TabletMetadata tablet = context.getAmple().readTablet(extent, FILES, LOGS, PREV_ROW, DIR);

    if (tablet == null) {
      throw new RuntimeException("Tablet " + extent + " not found in metadata");
    }

    result.addAll(tablet.getLogs());

    tablet.getFilesMap().forEach(sizes::put);

    return new Pair<>(result, sizes);
  }

  public static void removeUnusedWALEntries(ServerContext context, KeyExtent extent,
      final List<LogEntry> entries, ServiceLock zooLock) {
    TabletMutator tablet = context.getAmple().mutateTablet(extent);
    entries.forEach(tablet::deleteWal);
    tablet.putZooLock(zooLock);
    tablet.mutate();
  }

  private static Mutation createCloneMutation(TableId srcTableId, TableId tableId,
      Map<Key,Value> tablet) {

    KeyExtent ke = KeyExtent.fromMetaRow(tablet.keySet().iterator().next().getRow());
    Mutation m = new Mutation(TabletsSection.encodeRow(tableId, ke.endRow()));

    for (Entry<Key,Value> entry : tablet.entrySet()) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        String cf = entry.getKey().getColumnQualifier().toString();
        if (!cf.startsWith("../") && !cf.contains(":")) {
          cf = "../" + srcTableId + entry.getKey().getColumnQualifier();
        }
        m.put(entry.getKey().getColumnFamily(), new Text(cf), entry.getValue());
      } else if (entry.getKey().getColumnFamily().equals(CurrentLocationColumnFamily.NAME)) {
        m.put(LastLocationColumnFamily.NAME, entry.getKey().getColumnQualifier(), entry.getValue());
      } else if (entry.getKey().getColumnFamily().equals(LastLocationColumnFamily.NAME)) {
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

    return TabletsMetadata.builder(client).scanTable(tableName).overRange(range).checkConsistency()
        .saveKeyValues().fetch(FILES, LOCATION, LAST, CLONED, PREV_ROW, TIME).build();
  }

  @VisibleForTesting
  public static void initializeClone(String testTableName, TableId srcTableId, TableId tableId,
      AccumuloClient client, BatchWriter bw)
      throws TableNotFoundException, MutationsRejectedException {

    Iterator<TabletMetadata> ti = createCloneScanner(testTableName, srcTableId, client).iterator();

    if (!ti.hasNext()) {
      throw new RuntimeException(" table deleted during clone?  srcTableId = " + srcTableId);
    }

    while (ti.hasNext()) {
      bw.addMutation(createCloneMutation(srcTableId, tableId, ti.next().getKeyValues()));
    }

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

    if (!cloneIter.hasNext() || !srcIter.hasNext()) {
      throw new RuntimeException(
          " table deleted during clone?  srcTableId = " + srcTableId + " tableId=" + tableId);
    }

    int rewrites = 0;

    while (cloneIter.hasNext()) {
      TabletMetadata cloneTablet = cloneIter.next();
      Text cloneEndRow = cloneTablet.getEndRow();
      HashSet<TabletFile> cloneFiles = new HashSet<>();

      boolean cloneSuccessful = cloneTablet.getCloned() != null;

      if (!cloneSuccessful) {
        cloneFiles.addAll(cloneTablet.getFiles());
      }

      List<TabletMetadata> srcTablets = new ArrayList<>();
      TabletMetadata srcTablet = srcIter.next();
      srcTablets.add(srcTablet);

      Text srcEndRow = srcTablet.getEndRow();
      int cmp = compareEndRows(cloneEndRow, srcEndRow);
      if (cmp < 0) {
        throw new TabletDeletedException(
            "Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);
      }

      HashSet<TabletFile> srcFiles = new HashSet<>();
      if (!cloneSuccessful) {
        srcFiles.addAll(srcTablet.getFiles());
      }

      while (cmp > 0) {
        srcTablet = srcIter.next();
        srcTablets.add(srcTablet);
        srcEndRow = srcTablet.getEndRow();
        cmp = compareEndRows(cloneEndRow, srcEndRow);
        if (cmp < 0) {
          throw new TabletDeletedException(
              "Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);
        }

        if (!cloneSuccessful) {
          srcFiles.addAll(srcTablet.getFiles());
        }
      }

      if (cloneSuccessful) {
        continue;
      }

      if (srcFiles.containsAll(cloneFiles)) {
        // write out marker that this tablet was successfully cloned
        Mutation m = new Mutation(cloneTablet.getExtent().toMetaRow());
        m.put(ClonedColumnFamily.NAME, new Text(""), new Value("OK"));
        bw.addMutation(m);
      } else {
        // delete existing cloned tablet entry
        Mutation m = new Mutation(cloneTablet.getExtent().toMetaRow());

        for (Entry<Key,Value> entry : cloneTablet.getKeyValues().entrySet()) {
          Key k = entry.getKey();
          m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), k.getTimestamp());
        }

        bw.addMutation(m);

        for (TabletMetadata st : srcTablets) {
          bw.addMutation(createCloneMutation(srcTableId, tableId, st.getKeyValues()));
        }

        rewrites++;
      }
    }

    bw.flush();
    return rewrites;
  }

  public static void cloneTable(ServerContext context, TableId srcTableId, TableId tableId)
      throws Exception {

    try (BatchWriter bw = context.createBatchWriter(MetadataTable.NAME)) {

      while (true) {

        try {
          initializeClone(null, srcTableId, tableId, context, bw);

          // the following loop looks changes in the file that occurred during the copy.. if files
          // were dereferenced then they could have been GCed

          while (true) {
            int rewrites = checkClone(null, srcTableId, tableId, context, bw);

            if (rewrites == 0) {
              break;
            }
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
      mscanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());
      mscanner.fetchColumnFamily(ClonedColumnFamily.NAME);

      int dirCount = 0;

      for (Entry<Key,Value> entry : mscanner) {
        Key k = entry.getKey();
        Mutation m = new Mutation(k.getRow());
        m.putDelete(k.getColumnFamily(), k.getColumnQualifier());
        byte[] dirName =
            FastFormat.toZeroPaddedString(dirCount++, 8, 16, Constants.CLONE_PREFIX_BYTES);
        ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(dirName));

        bw.addMutation(m);
      }
    }
  }

  public static void chopped(ServerContext context, KeyExtent extent, ServiceLock zooLock) {
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
        BatchWriter bw = client.createBatchWriter(MetadataTable.NAME)) {
      mscanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());
      mscanner.fetchColumnFamily(BulkFileColumnFamily.NAME);

      for (Entry<Key,Value> entry : mscanner) {
        log.trace("Looking at entry {} with tid {}", entry, tid);
        long entryTid = BulkFileColumnFamily.getBulkLoadTid(entry.getValue());
        if (tid == entryTid) {
          log.trace("deleting entry {}", entry);
          Key key = entry.getKey();
          Mutation m = new Mutation(key.getRow());
          m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
          bw.addMutation(m);
        }
      }
    }
  }

  public static void addBulkLoadInProgressFlag(ServerContext context, String path, long fateTxid) {

    Mutation m = new Mutation(BlipSection.getRowPrefix() + path);
    m.put(EMPTY_TEXT, EMPTY_TEXT, new Value(FateTxId.formatTid(fateTxid)));

    // new KeyExtent is only added to force update to write to the metadata table, not the root
    // table
    // because bulk loads aren't supported to the metadata table
    update(context, m, new KeyExtent(TableId.of("anythingNotMetadata"), null, null));
  }

  public static void removeBulkLoadInProgressFlag(ServerContext context, String path) {

    Mutation m = new Mutation(BlipSection.getRowPrefix() + path);
    m.putDelete(EMPTY_TEXT, EMPTY_TEXT);

    // new KeyExtent is only added to force update to write to the metadata table, not the root
    // table because bulk loads aren't supported to the metadata table
    update(context, m, new KeyExtent(TableId.of("anythingNotMetadata"), null, null));
  }

  public static SortedMap<Text,SortedMap<ColumnFQ,Value>>
      getTabletEntries(SortedMap<Key,Value> tabletKeyValues, List<ColumnFQ> columns) {
    TreeMap<Text,SortedMap<ColumnFQ,Value>> tabletEntries = new TreeMap<>();

    HashSet<ColumnFQ> colSet = columns == null ? null : new HashSet<>(columns);

    tabletKeyValues.forEach((key, val) -> {
      ColumnFQ currentKey = new ColumnFQ(key);
      if (columns == null || colSet.contains(currentKey)) {
        tabletEntries.computeIfAbsent(key.getRow(), k -> new TreeMap<>()).put(currentKey, val);
      }
    });

    return tabletEntries;
  }
}

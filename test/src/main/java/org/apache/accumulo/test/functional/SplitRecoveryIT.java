/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.clientImpl.Writer;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.util.MasterMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SplitRecoveryIT extends ConfigurableMacBase {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  private KeyExtent nke(String table, String endRow, String prevEndRow) {
    return new KeyExtent(TableId.of(table), endRow == null ? null : new Text(endRow),
        prevEndRow == null ? null : new Text(prevEndRow));
  }

  private void run(ServerContext c) throws Exception {
    String zPath = c.getZooKeeperRoot() + "/testLock";
    ZooReaderWriter zoo = c.getZooReaderWriter();
    zoo.putPersistentData(zPath, new byte[0], NodeExistsPolicy.OVERWRITE);
    ZooLock zl = new ZooLock(zoo, zPath);
    boolean gotLock = zl.tryLock(new LockWatcher() {

      @SuppressFBWarnings(value = "DM_EXIT",
          justification = "System.exit() is a bad idea here, but okay for now, since it's a test")
      @Override
      public void lostLock(LockLossReason reason) {
        System.exit(-1);

      }

      @SuppressFBWarnings(value = "DM_EXIT",
          justification = "System.exit() is a bad idea here, but okay for now, since it's a test")
      @Override
      public void unableToMonitorLockNode(Throwable e) {
        System.exit(-1);
      }
    }, "foo".getBytes(UTF_8));

    if (!gotLock) {
      System.err.println("Failed to get lock " + zPath);
    }

    // run test for a table with one tablet
    runSplitRecoveryTest(c, 0, "sp", 0, zl, nke("foo0", null, null));
    runSplitRecoveryTest(c, 1, "sp", 0, zl, nke("foo1", null, null));

    // run test for tables with two tablets, run test on first and last tablet
    runSplitRecoveryTest(c, 0, "k", 0, zl, nke("foo2", "m", null), nke("foo2", null, "m"));
    runSplitRecoveryTest(c, 1, "k", 0, zl, nke("foo3", "m", null), nke("foo3", null, "m"));
    runSplitRecoveryTest(c, 0, "o", 1, zl, nke("foo4", "m", null), nke("foo4", null, "m"));
    runSplitRecoveryTest(c, 1, "o", 1, zl, nke("foo5", "m", null), nke("foo5", null, "m"));

    // run test for table w/ three tablets, run test on middle tablet
    runSplitRecoveryTest(c, 0, "o", 1, zl, nke("foo6", "m", null), nke("foo6", "r", "m"),
        nke("foo6", null, "r"));
    runSplitRecoveryTest(c, 1, "o", 1, zl, nke("foo7", "m", null), nke("foo7", "r", "m"),
        nke("foo7", null, "r"));

    // run test for table w/ three tablets, run test on first
    runSplitRecoveryTest(c, 0, "g", 0, zl, nke("foo8", "m", null), nke("foo8", "r", "m"),
        nke("foo8", null, "r"));
    runSplitRecoveryTest(c, 1, "g", 0, zl, nke("foo9", "m", null), nke("foo9", "r", "m"),
        nke("foo9", null, "r"));

    // run test for table w/ three tablets, run test on last tablet
    runSplitRecoveryTest(c, 0, "w", 2, zl, nke("fooa", "m", null), nke("fooa", "r", "m"),
        nke("fooa", null, "r"));
    runSplitRecoveryTest(c, 1, "w", 2, zl, nke("foob", "m", null), nke("foob", "r", "m"),
        nke("foob", null, "r"));
  }

  private void runSplitRecoveryTest(ServerContext context, int failPoint, String mr,
      int extentToSplit, ZooLock zl, KeyExtent... extents) throws Exception {

    Text midRow = new Text(mr);

    SortedMap<StoredTabletFile,DataFileValue> splitMapFiles = null;

    for (int i = 0; i < extents.length; i++) {
      KeyExtent extent = extents[i];

      String dirName = "dir_" + i;
      String tdir = ServerConstants.getTablesDirs(context).iterator().next() + "/"
          + extent.getTableId() + "/" + dirName;
      MetadataTableUtil.addTablet(extent, dirName, context, TimeType.LOGICAL, zl);
      SortedMap<TabletFile,DataFileValue> mapFiles = new TreeMap<>();
      mapFiles.put(new TabletFile(new Path(tdir + "/" + RFile.EXTENSION + "_000_000")),
          new DataFileValue(1000017 + i, 10000 + i));

      int tid = 0;
      TransactionWatcher.ZooArbitrator.start(context, Constants.BULK_ARBITRATOR_TYPE, tid);
      SortedMap<StoredTabletFile,DataFileValue> storedFiles =
          new TreeMap<>(MetadataTableUtil.updateTabletDataFile(tid, extent, mapFiles,
              new MetadataTime(0, TimeType.LOGICAL), context, zl));
      if (i == extentToSplit) {
        splitMapFiles = storedFiles;
      }
    }

    KeyExtent extent = extents[extentToSplit];

    KeyExtent high = new KeyExtent(extent.getTableId(), extent.getEndRow(), midRow);
    KeyExtent low = new KeyExtent(extent.getTableId(), midRow, extent.getPrevEndRow());

    splitPartiallyAndRecover(context, extent, high, low, .4, splitMapFiles, midRow,
        "localhost:1234", failPoint, zl);
  }

  private static Map<Long,List<TabletFile>> getBulkFilesLoaded(ServerContext context,
      KeyExtent extent) {
    Map<Long,List<TabletFile>> bulkFiles = new HashMap<>();

    context.getAmple().readTablet(extent).getLoaded()
        .forEach((path, txid) -> bulkFiles.computeIfAbsent(txid, k -> new ArrayList<>()).add(path));

    return bulkFiles;
  }

  private void splitPartiallyAndRecover(ServerContext context, KeyExtent extent, KeyExtent high,
      KeyExtent low, double splitRatio, SortedMap<StoredTabletFile,DataFileValue> mapFiles,
      Text midRow, String location, int steps, ZooLock zl) throws Exception {

    SortedMap<StoredTabletFile,DataFileValue> lowDatafileSizes = new TreeMap<>();
    SortedMap<StoredTabletFile,DataFileValue> highDatafileSizes = new TreeMap<>();
    List<StoredTabletFile> highDatafilesToRemove = new ArrayList<>();

    MetadataTableUtil.splitDatafiles(midRow, splitRatio, new HashMap<>(), mapFiles,
        lowDatafileSizes, highDatafileSizes, highDatafilesToRemove);

    MetadataTableUtil.splitTablet(high, extent.getPrevEndRow(), splitRatio, context, zl);
    TServerInstance instance = new TServerInstance(location, zl.getSessionId());
    Writer writer = MetadataTableUtil.getMetadataTable(context);
    Assignment assignment = new Assignment(high, instance);
    Mutation m = new Mutation(assignment.tablet.getMetadataEntry());
    assignment.server.putFutureLocation(m);
    writer.update(m);

    if (steps >= 1) {
      Map<Long,List<TabletFile>> bulkFiles = getBulkFilesLoaded(context, extent);

      MasterMetadataUtil.addNewTablet(context, low, "lowDir", instance, lowDatafileSizes, bulkFiles,
          new MetadataTime(0, TimeType.LOGICAL), -1L, -1L, zl);
    }
    if (steps >= 2) {
      MetadataTableUtil.finishSplit(high, highDatafileSizes, highDatafilesToRemove, context, zl);
    }

    TabletMetadata meta = context.getAmple().readTablet(high);
    KeyExtent fixedExtent = MasterMetadataUtil.fixSplit(context, meta, zl);

    if (steps < 2)
      assertEquals(splitRatio, meta.getSplitRatio(), 0.0);

    if (steps >= 1) {
      assertEquals(high, fixedExtent);
      ensureTabletHasNoUnexpectedMetadataEntries(context, low, lowDatafileSizes);
      ensureTabletHasNoUnexpectedMetadataEntries(context, high, highDatafileSizes);

      Map<Long,? extends Collection<TabletFile>> lowBulkFiles = getBulkFilesLoaded(context, low);
      Map<Long,? extends Collection<TabletFile>> highBulkFiles = getBulkFilesLoaded(context, high);

      if (!lowBulkFiles.equals(highBulkFiles)) {
        throw new Exception(" " + lowBulkFiles + " != " + highBulkFiles + " " + low + " " + high);
      }

      if (lowBulkFiles.isEmpty()) {
        throw new Exception(" no bulk files " + low);
      }
    } else {
      assertEquals(extent, fixedExtent);
      ensureTabletHasNoUnexpectedMetadataEntries(context, extent, mapFiles);
    }
  }

  private void ensureTabletHasNoUnexpectedMetadataEntries(ServerContext context, KeyExtent extent,
      SortedMap<StoredTabletFile,DataFileValue> expectedMapFiles) throws Exception {
    try (Scanner scanner = new ScannerImpl(context, MetadataTable.ID, Authorizations.EMPTY)) {
      scanner.setRange(extent.toMetadataRange());

      HashSet<ColumnFQ> expectedColumns = new HashSet<>();
      expectedColumns.add(TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN);
      expectedColumns.add(TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN);
      expectedColumns.add(TabletsSection.ServerColumnFamily.TIME_COLUMN);
      expectedColumns.add(TabletsSection.ServerColumnFamily.LOCK_COLUMN);

      HashSet<Text> expectedColumnFamilies = new HashSet<>();
      expectedColumnFamilies.add(DataFileColumnFamily.NAME);
      expectedColumnFamilies.add(TabletsSection.FutureLocationColumnFamily.NAME);
      expectedColumnFamilies.add(TabletsSection.CurrentLocationColumnFamily.NAME);
      expectedColumnFamilies.add(TabletsSection.LastLocationColumnFamily.NAME);
      expectedColumnFamilies.add(TabletsSection.BulkFileColumnFamily.NAME);

      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      boolean sawPer = false;

      while (iter.hasNext()) {
        Entry<Key,Value> entry = iter.next();
        Key key = entry.getKey();

        if (!key.getRow().equals(extent.getMetadataEntry())) {
          throw new Exception(
              "Tablet " + extent + " contained unexpected " + MetadataTable.NAME + " entry " + key);
        }

        if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
          sawPer = true;
          if (!new KeyExtent(key.getRow(), entry.getValue()).equals(extent)) {
            throw new Exception("Unexpected prev end row " + entry);
          }
        }

        if (expectedColumnFamilies.contains(key.getColumnFamily())) {
          continue;
        }

        if (expectedColumns.remove(new ColumnFQ(key))) {
          continue;
        }

        throw new Exception(
            "Tablet " + extent + " contained unexpected " + MetadataTable.NAME + " entry " + key);
      }

      if (expectedColumns.size() > 1 || (expectedColumns.size() == 1)) {
        throw new Exception("Not all expected columns seen " + extent + " " + expectedColumns);
      }

      assertTrue(sawPer);

      SortedMap<StoredTabletFile,DataFileValue> fixedMapFiles =
          MetadataTableUtil.getFileAndLogEntries(context, extent).getSecond();
      verifySame(expectedMapFiles, fixedMapFiles);
    }
  }

  private void verifySame(SortedMap<StoredTabletFile,DataFileValue> datafileSizes,
      SortedMap<StoredTabletFile,DataFileValue> fixedDatafileSizes) throws Exception {

    if (!datafileSizes.keySet().containsAll(fixedDatafileSizes.keySet())
        || !fixedDatafileSizes.keySet().containsAll(datafileSizes.keySet())) {
      throw new Exception("Key sets not the same " + datafileSizes.keySet() + " !=  "
          + fixedDatafileSizes.keySet());
    }

    for (Entry<StoredTabletFile,DataFileValue> entry : datafileSizes.entrySet()) {
      DataFileValue dfv = entry.getValue();
      DataFileValue otherDfv = fixedDatafileSizes.get(entry.getKey());

      if (!dfv.equals(otherDfv)) {
        throw new Exception(entry.getKey() + " dfv not equal  " + dfv + "  " + otherDfv);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    new SplitRecoveryIT().run(new ServerContext(SiteConfiguration.auto()));
  }

  @Test
  public void test() throws Exception {
    assertEquals(0, exec(SplitRecoveryIT.class).waitFor());
  }

}

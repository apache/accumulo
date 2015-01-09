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
package org.apache.accumulo.test.functional;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
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
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.accumulo.server.util.MasterMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class SplitRecoveryIT extends ConfigurableMacIT {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  private KeyExtent nke(String table, String endRow, String prevEndRow) {
    return new KeyExtent(new Text(table), endRow == null ? null : new Text(endRow), prevEndRow == null ? null : new Text(prevEndRow));
  }

  private void run() throws Exception {
    String zPath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + "/testLock";
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    zoo.putPersistentData(zPath, new byte[0], NodeExistsPolicy.OVERWRITE);
    ZooLock zl = new ZooLock(zPath);
    boolean gotLock = zl.tryLock(new LockWatcher() {

      @Override
      public void lostLock(LockLossReason reason) {
        System.exit(-1);

      }

      @Override
      public void unableToMonitorLockNode(Throwable e) {
        System.exit(-1);
      }
    }, "foo".getBytes(UTF_8));

    if (!gotLock) {
      System.err.println("Failed to get lock " + zPath);
    }

    // run test for a table with one tablet
    runSplitRecoveryTest(0, "sp", 0, zl, nke("foo0", null, null));
    runSplitRecoveryTest(1, "sp", 0, zl, nke("foo1", null, null));

    // run test for tables with two tablets, run test on first and last tablet
    runSplitRecoveryTest(0, "k", 0, zl, nke("foo2", "m", null), nke("foo2", null, "m"));
    runSplitRecoveryTest(1, "k", 0, zl, nke("foo3", "m", null), nke("foo3", null, "m"));
    runSplitRecoveryTest(0, "o", 1, zl, nke("foo4", "m", null), nke("foo4", null, "m"));
    runSplitRecoveryTest(1, "o", 1, zl, nke("foo5", "m", null), nke("foo5", null, "m"));

    // run test for table w/ three tablets, run test on middle tablet
    runSplitRecoveryTest(0, "o", 1, zl, nke("foo6", "m", null), nke("foo6", "r", "m"), nke("foo6", null, "r"));
    runSplitRecoveryTest(1, "o", 1, zl, nke("foo7", "m", null), nke("foo7", "r", "m"), nke("foo7", null, "r"));

    // run test for table w/ three tablets, run test on first
    runSplitRecoveryTest(0, "g", 0, zl, nke("foo8", "m", null), nke("foo8", "r", "m"), nke("foo8", null, "r"));
    runSplitRecoveryTest(1, "g", 0, zl, nke("foo9", "m", null), nke("foo9", "r", "m"), nke("foo9", null, "r"));

    // run test for table w/ three tablets, run test on last tablet
    runSplitRecoveryTest(0, "w", 2, zl, nke("fooa", "m", null), nke("fooa", "r", "m"), nke("fooa", null, "r"));
    runSplitRecoveryTest(1, "w", 2, zl, nke("foob", "m", null), nke("foob", "r", "m"), nke("foob", null, "r"));
  }

  private void runSplitRecoveryTest(int failPoint, String mr, int extentToSplit, ZooLock zl, KeyExtent... extents) throws Exception {

    Text midRow = new Text(mr);

    SortedMap<FileRef,DataFileValue> splitMapFiles = null;

    for (int i = 0; i < extents.length; i++) {
      KeyExtent extent = extents[i];

      String tdir = ServerConstants.getTablesDirs()[0] + "/" + extent.getTableId().toString() + "/dir_" + i;
      MetadataTableUtil.addTablet(extent, tdir, SystemCredentials.get(), TabletTime.LOGICAL_TIME_ID, zl);
      SortedMap<FileRef,DataFileValue> mapFiles = new TreeMap<FileRef,DataFileValue>();
      mapFiles.put(new FileRef(tdir + "/" + RFile.EXTENSION + "_000_000"), new DataFileValue(1000017 + i, 10000 + i));

      if (i == extentToSplit) {
        splitMapFiles = mapFiles;
      }
      int tid = 0;
      TransactionWatcher.ZooArbitrator.start(Constants.BULK_ARBITRATOR_TYPE, tid);
      MetadataTableUtil.updateTabletDataFile(tid, extent, mapFiles, "L0", SystemCredentials.get(), zl);
    }

    KeyExtent extent = extents[extentToSplit];

    KeyExtent high = new KeyExtent(extent.getTableId(), extent.getEndRow(), midRow);
    KeyExtent low = new KeyExtent(extent.getTableId(), midRow, extent.getPrevEndRow());

    splitPartiallyAndRecover(extent, high, low, .4, splitMapFiles, midRow, "localhost:1234", failPoint, zl);
  }

  private void splitPartiallyAndRecover(KeyExtent extent, KeyExtent high, KeyExtent low, double splitRatio, SortedMap<FileRef,DataFileValue> mapFiles,
      Text midRow, String location, int steps, ZooLock zl) throws Exception {

    SortedMap<FileRef,DataFileValue> lowDatafileSizes = new TreeMap<FileRef,DataFileValue>();
    SortedMap<FileRef,DataFileValue> highDatafileSizes = new TreeMap<FileRef,DataFileValue>();
    List<FileRef> highDatafilesToRemove = new ArrayList<FileRef>();

    MetadataTableUtil.splitDatafiles(extent.getTableId(), midRow, splitRatio, new HashMap<FileRef,FileUtil.FileInfo>(), mapFiles, lowDatafileSizes,
        highDatafileSizes, highDatafilesToRemove);

    MetadataTableUtil.splitTablet(high, extent.getPrevEndRow(), splitRatio, SystemCredentials.get(), zl);
    TServerInstance instance = new TServerInstance(location, zl.getSessionId());
    Writer writer = new Writer(HdfsZooInstance.getInstance(), SystemCredentials.get(), MetadataTable.ID);
    Assignment assignment = new Assignment(high, instance);
    Mutation m = new Mutation(assignment.tablet.getMetadataEntry());
    m.put(TabletsSection.FutureLocationColumnFamily.NAME, assignment.server.asColumnQualifier(), assignment.server.asMutationValue());
    writer.update(m);

    if (steps >= 1) {
      Map<FileRef,Long> bulkFiles = MetadataTableUtil.getBulkFilesLoaded(SystemCredentials.get(), extent);
      MasterMetadataUtil.addNewTablet(low, "/lowDir", instance, lowDatafileSizes, bulkFiles, SystemCredentials.get(), TabletTime.LOGICAL_TIME_ID + "0", -1l,
          -1l, zl);
    }
    if (steps >= 2)
      MetadataTableUtil.finishSplit(high, highDatafileSizes, highDatafilesToRemove, SystemCredentials.get(), zl);

    TabletServer.verifyTabletInformation(high, instance, null, "127.0.0.1:0", zl);

    if (steps >= 1) {
      ensureTabletHasNoUnexpectedMetadataEntries(low, lowDatafileSizes);
      ensureTabletHasNoUnexpectedMetadataEntries(high, highDatafileSizes);

      Map<FileRef,Long> lowBulkFiles = MetadataTableUtil.getBulkFilesLoaded(SystemCredentials.get(), low);
      Map<FileRef,Long> highBulkFiles = MetadataTableUtil.getBulkFilesLoaded(SystemCredentials.get(), high);

      if (!lowBulkFiles.equals(highBulkFiles)) {
        throw new Exception(" " + lowBulkFiles + " != " + highBulkFiles + " " + low + " " + high);
      }

      if (lowBulkFiles.size() == 0) {
        throw new Exception(" no bulk files " + low);
      }
    } else {
      ensureTabletHasNoUnexpectedMetadataEntries(extent, mapFiles);
    }
  }

  private void ensureTabletHasNoUnexpectedMetadataEntries(KeyExtent extent, SortedMap<FileRef,DataFileValue> expectedMapFiles) throws Exception {
    Scanner scanner = new ScannerImpl(HdfsZooInstance.getInstance(), SystemCredentials.get(), MetadataTable.ID, Authorizations.EMPTY);
    scanner.setRange(extent.toMetadataRange());

    HashSet<ColumnFQ> expectedColumns = new HashSet<ColumnFQ>();
    expectedColumns.add(TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN);
    expectedColumns.add(TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN);
    expectedColumns.add(TabletsSection.ServerColumnFamily.TIME_COLUMN);
    expectedColumns.add(TabletsSection.ServerColumnFamily.LOCK_COLUMN);

    HashSet<Text> expectedColumnFamilies = new HashSet<Text>();
    expectedColumnFamilies.add(DataFileColumnFamily.NAME);
    expectedColumnFamilies.add(TabletsSection.FutureLocationColumnFamily.NAME);
    expectedColumnFamilies.add(TabletsSection.CurrentLocationColumnFamily.NAME);
    expectedColumnFamilies.add(TabletsSection.LastLocationColumnFamily.NAME);
    expectedColumnFamilies.add(TabletsSection.BulkFileColumnFamily.NAME);

    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    while (iter.hasNext()) {
      Key key = iter.next().getKey();

      if (!key.getRow().equals(extent.getMetadataEntry())) {
        throw new Exception("Tablet " + extent + " contained unexpected " + MetadataTable.NAME + " entry " + key);
      }

      if (expectedColumnFamilies.contains(key.getColumnFamily())) {
        continue;
      }

      if (expectedColumns.remove(new ColumnFQ(key))) {
        continue;
      }

      throw new Exception("Tablet " + extent + " contained unexpected " + MetadataTable.NAME + " entry " + key);
    }
    System.out.println("expectedColumns " + expectedColumns);
    if (expectedColumns.size() > 1 || (expectedColumns.size() == 1)) {
      throw new Exception("Not all expected columns seen " + extent + " " + expectedColumns);
    }

    SortedMap<FileRef,DataFileValue> fixedMapFiles = MetadataTableUtil.getDataFileSizes(extent, SystemCredentials.get());
    verifySame(expectedMapFiles, fixedMapFiles);
  }

  private void verifySame(SortedMap<FileRef,DataFileValue> datafileSizes, SortedMap<FileRef,DataFileValue> fixedDatafileSizes) throws Exception {

    if (!datafileSizes.keySet().containsAll(fixedDatafileSizes.keySet()) || !fixedDatafileSizes.keySet().containsAll(datafileSizes.keySet())) {
      throw new Exception("Key sets not the same " + datafileSizes.keySet() + " !=  " + fixedDatafileSizes.keySet());
    }

    for (Entry<FileRef,DataFileValue> entry : datafileSizes.entrySet()) {
      DataFileValue dfv = entry.getValue();
      DataFileValue otherDfv = fixedDatafileSizes.get(entry.getKey());

      if (!dfv.equals(otherDfv)) {
        throw new Exception(entry.getKey() + " dfv not equal  " + dfv + "  " + otherDfv);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    new SplitRecoveryIT().run();
  }

  @Test
  public void test() throws Exception {
    assertEquals(0, exec(SplitRecoveryIT.class).waitFor());
  }

}

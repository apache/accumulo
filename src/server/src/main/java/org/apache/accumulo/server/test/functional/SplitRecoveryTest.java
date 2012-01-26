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
package org.apache.accumulo.server.test.functional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.Writer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.MetadataTable.DataFileValue;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.tabletserver.TabletServer;
import org.apache.accumulo.server.tabletserver.TabletTime;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.server.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.io.Text;

public class SplitRecoveryTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {
    
  }
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.emptyList();
  }
  
  private KeyExtent nke(String table, String endRow, String prevEndRow) {
    return new KeyExtent(new Text(table), endRow == null ? null : new Text(endRow), prevEndRow == null ? null : new Text(prevEndRow));
  }
  
  @Override
  public void run() throws Exception {
    String zPath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + "/testLock";
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    zoo.putPersistentData(zPath, "".getBytes(), NodeExistsPolicy.OVERWRITE);
    ZooLock zl = new ZooLock(zPath);
    boolean gotLock = zl.tryLock(new LockWatcher() {
      
      @Override
      public void lostLock(LockLossReason reason) {
        System.exit(-1);
        
      }
    }, "foo".getBytes());
    
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
    
    SortedMap<String,DataFileValue> splitMapFiles = null;
    
    for (int i = 0; i < extents.length; i++) {
      KeyExtent extent = extents[i];
      
      String tdir = "/dir_" + i;
      MetadataTable.addTablet(extent, tdir, SecurityConstants.getSystemCredentials(), TabletTime.LOGICAL_TIME_ID, zl);
      SortedMap<String,DataFileValue> mapFiles = new TreeMap<String,DataFileValue>();
      mapFiles.put(tdir + "/" + RFile.EXTENSION + "_000_000", new DataFileValue(1000017 + i, 10000 + i));
      
      if (i == extentToSplit) {
        splitMapFiles = mapFiles;
      }
      
      MetadataTable.updateTabletDataFile(0, extent, mapFiles, "L0", SecurityConstants.getSystemCredentials(), zl);
    }
    
    KeyExtent extent = extents[extentToSplit];
    
    KeyExtent high = new KeyExtent(extent.getTableId(), extent.getEndRow(), midRow);
    KeyExtent low = new KeyExtent(extent.getTableId(), midRow, extent.getPrevEndRow());
    
    splitPartiallyAndRecover(extent, high, low, .4, splitMapFiles, midRow, "localhost:1234", failPoint, zl);
  }
  
  private void splitPartiallyAndRecover(KeyExtent extent, KeyExtent high, KeyExtent low, double splitRatio, SortedMap<String,DataFileValue> mapFiles,
      Text midRow, String location, int steps, ZooLock zl) throws Exception {
    
    SortedMap<String,DataFileValue> lowDatafileSizes = new TreeMap<String,DataFileValue>();
    SortedMap<String,DataFileValue> highDatafileSizes = new TreeMap<String,DataFileValue>();
    List<String> highDatafilesToRemove = new ArrayList<String>();
    
    MetadataTable.splitDatafiles(extent.getTableId(), midRow, splitRatio, new HashMap<String,FileUtil.FileInfo>(), mapFiles, lowDatafileSizes,
        highDatafileSizes, highDatafilesToRemove);
    
    MetadataTable.splitTablet(high, extent.getPrevEndRow(), splitRatio, SecurityConstants.getSystemCredentials(), zl);
    TServerInstance instance = new TServerInstance(location, zl.getSessionId());
    Writer writer = new Writer(HdfsZooInstance.getInstance(), SecurityConstants.getSystemCredentials(), Constants.METADATA_TABLE_ID);
    Assignment assignment = new Assignment(high, instance);
    Mutation m = new Mutation(assignment.tablet.getMetadataEntry());
    m.put(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, assignment.server.asColumnQualifier(), assignment.server.asMutationValue());
    writer.update(m);
    
    if (steps >= 1) {
      Map<String,Long> bulkFiles = MetadataTable.getBulkFilesLoaded(SecurityConstants.getSystemCredentials(), extent);
      MetadataTable.addNewTablet(low, "/lowDir", instance, lowDatafileSizes, bulkFiles, SecurityConstants.getSystemCredentials(), TabletTime.LOGICAL_TIME_ID
          + "0", -1l, -1l, zl);
    }
    if (steps >= 2)
      MetadataTable.finishSplit(high, highDatafileSizes, highDatafilesToRemove, SecurityConstants.getSystemCredentials(), zl);
    
    SortedMap<KeyExtent,Text> vtiRet = TabletServer.verifyTabletInformation(extent, instance, null, "127.0.0.1:0", zl);
    
    if (vtiRet.size() != 2) {
      throw new Exception("verifyTabletInformation did not return two tablets, " + vtiRet.size());
    }
    
    if (!vtiRet.containsKey(high) || !vtiRet.containsKey(low)) {
      throw new Exception("verifyTabletInformation did not return correct tablets, " + vtiRet.keySet());
    }
    
    ensureTabletHasNoUnexpectedMetadataEntries(low, lowDatafileSizes);
    ensureTabletHasNoUnexpectedMetadataEntries(high, highDatafileSizes);
    
    Map<String,Long> lowBulkFiles = MetadataTable.getBulkFilesLoaded(SecurityConstants.getSystemCredentials(), low);
    Map<String,Long> highBulkFiles = MetadataTable.getBulkFilesLoaded(SecurityConstants.getSystemCredentials(), high);
    
    if (!lowBulkFiles.equals(highBulkFiles)) {
      throw new Exception(" " + lowBulkFiles + " != " + highBulkFiles + " " + low + " " + high);
    }
    
    if (lowBulkFiles.size() == 0) {
      throw new Exception(" no bulk files " + low);
    }
  }
  
  private void ensureTabletHasNoUnexpectedMetadataEntries(KeyExtent extent, SortedMap<String,DataFileValue> expectedMapFiles) throws Exception {
    SortedMap<Key,Value> tkv = new TreeMap<Key,Value>();
    MetadataTable.getTabletAndPrevTabletKeyValues(tkv, extent, null, SecurityConstants.getSystemCredentials());
    
    HashSet<ColumnFQ> expectedColumns = new HashSet<ColumnFQ>();
    expectedColumns.add(Constants.METADATA_DIRECTORY_COLUMN);
    expectedColumns.add(Constants.METADATA_PREV_ROW_COLUMN);
    expectedColumns.add(Constants.METADATA_TIME_COLUMN);
    expectedColumns.add(Constants.METADATA_LOCK_COLUMN);
    
    HashSet<Text> expectedColumnFamilies = new HashSet<Text>();
    expectedColumnFamilies.add(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
    expectedColumnFamilies.add(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY);
    expectedColumnFamilies.add(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
    expectedColumnFamilies.add(Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY);
    expectedColumnFamilies.add(Constants.METADATA_BULKFILE_COLUMN_FAMILY);
    
    Iterator<Key> iter = tkv.keySet().iterator();
    while (iter.hasNext()) {
      Key key = iter.next();
      
      if (!key.getRow().equals(extent.getMetadataEntry())) {
        continue;
      }
      
      if (expectedColumnFamilies.contains(key.getColumnFamily())) {
        continue;
      }
      
      if (expectedColumns.remove(new ColumnFQ(key))) {
        continue;
      }
      
      throw new Exception("Tablet " + extent + " contained unexpected " + Constants.METADATA_TABLE_NAME + " entry " + key);
    }
    System.out.println("expectedColumns " + expectedColumns);
    if (expectedColumns.size() > 1 || (expectedColumns.size() == 1)) {
      throw new Exception("Not all expected columns seen " + extent + " " + expectedColumns);
    }
    
    SortedMap<String,DataFileValue> fixedMapFiles = MetadataTable.getDataFileSizes(extent, SecurityConstants.getSystemCredentials());
    verifySame(expectedMapFiles, fixedMapFiles);
  }
  
  private void verifySame(SortedMap<String,DataFileValue> datafileSizes, SortedMap<String,DataFileValue> fixedDatafileSizes) throws Exception {
    
    if (!datafileSizes.keySet().containsAll(fixedDatafileSizes.keySet()) || !fixedDatafileSizes.keySet().containsAll(datafileSizes.keySet())) {
      throw new Exception("Key sets not the same " + datafileSizes.keySet() + " !=  " + fixedDatafileSizes.keySet());
    }
    
    for (Entry<String,DataFileValue> entry : datafileSizes.entrySet()) {
      DataFileValue dfv = entry.getValue();
      DataFileValue otherDfv = fixedDatafileSizes.get(entry.getKey());
      
      if (!dfv.equals(otherDfv)) {
        throw new Exception(entry.getKey() + " dfv not equal  " + dfv + "  " + otherDfv);
      }
    }
  }
  
}

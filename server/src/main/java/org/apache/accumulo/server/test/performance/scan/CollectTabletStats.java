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
package org.apache.accumulo.server.test.performance.scan;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class CollectTabletStats {
  public static void main(String[] args) throws Exception {
    
    int iterations = 3;
    int numThreads = 1;
    boolean selectLocalTablets = true;
    String columnsTmp[] = new String[] {};
    
    int index = 0;
    String processedArgs[] = new String[8];
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-i"))
        iterations = Integer.parseInt(args[++i]);
      else if (args[i].equals("-t"))
        numThreads = Integer.parseInt(args[++i]);
      else if (args[i].equals("-l"))
        selectLocalTablets = true;
      else if (args[i].equals("-f"))
        selectLocalTablets = false;
      else if (args[i].equals("-c"))
        columnsTmp = args[++i].split(",");
      else
        processedArgs[index++] = args[i];
    }
    
    final String columns[] = columnsTmp;
    
    if (index != 7) {
      System.err.println("USAGE : " + CollectTabletStats.class
          + " [-i <iterations>] [-t <num threads>] [-l|-f] [-c <column fams>] <instance> <zookeepers> <user> <pass> <table> <auths> <batch size>");
      return;
    }
    final FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());

    String instance = processedArgs[0];
    String zookeepers = processedArgs[1];
    String user = processedArgs[2];
    String pass = processedArgs[3];
    final String tableName = processedArgs[4];
    final String auths[] = processedArgs[5].split(",");
    final int batchSize = Integer.parseInt(processedArgs[6]);
    ZooKeeperInstance zki = new ZooKeeperInstance(instance, zookeepers);
    final ServerConfiguration sconf = new ServerConfiguration(zki);
    
    String tableId = Tables.getNameToIdMap(zki).get(tableName);
    
    Map<KeyExtent,String> locations = new HashMap<KeyExtent,String>();
    List<KeyExtent> candidates = findTablets(selectLocalTablets, user, pass, tableName, zki, locations);
    
    if (candidates.size() < numThreads) {
      System.err.println("ERROR : Unable to find " + numThreads + " " + (selectLocalTablets ? "local" : "far") + " tablets");
      System.exit(-1);
    }
    
    List<KeyExtent> tabletsToTest = selectRandomTablets(numThreads, candidates);
    
    Map<KeyExtent,List<String>> tabletFiles = new HashMap<KeyExtent,List<String>>();
    
    for (KeyExtent ke : tabletsToTest) {
      List<String> files = getTabletFiles(user, pass, zki, tableId, ke);
      tabletFiles.put(ke, files);
    }
    
    System.out.println();
    System.out.println("run location      : " + InetAddress.getLocalHost().getHostName() + "/" + InetAddress.getLocalHost().getHostAddress());
    System.out.println("num threads       : " + numThreads);
    System.out.println("table             : " + tableName);
    System.out.println("table id          : " + tableId);
    
    for (KeyExtent ke : tabletsToTest) {
      System.out.println("\t *** Information about tablet " + ke.getUUID() + " *** ");
      System.out.println("\t\t# files in tablet : " + tabletFiles.get(ke).size());
      System.out.println("\t\ttablet location   : " + locations.get(ke));
      reportHdfsBlockLocations(tabletFiles.get(ke));
    }
    
    System.out.println("\n*** RUNNING TEST ***\n");
    
    ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
    
    for (int i = 0; i < iterations; i++) {
      
      ArrayList<Test> tests = new ArrayList<Test>();
      
      for (final KeyExtent ke : tabletsToTest) {
        final List<String> files = tabletFiles.get(ke);
        Test test = new Test(ke) {
          public int runTest() throws Exception {
            return readFiles(fs, sconf.getConfiguration(), files, ke, columns);
          }
          
        };
        
        tests.add(test);
      }
      
      runTest("read files", tests, numThreads, threadPool);
    }
    
    for (int i = 0; i < iterations; i++) {
      
      ArrayList<Test> tests = new ArrayList<Test>();
      
      for (final KeyExtent ke : tabletsToTest) {
        final List<String> files = tabletFiles.get(ke);
        Test test = new Test(ke) {
          public int runTest() throws Exception {
            return readFilesUsingIterStack(fs, sconf, files, auths, ke, columns, false);
          }
        };
        
        tests.add(test);
      }
      
      runTest("read tablet files w/ system iter stack", tests, numThreads, threadPool);
    }
    
    for (int i = 0; i < iterations; i++) {
      ArrayList<Test> tests = new ArrayList<Test>();
      
      for (final KeyExtent ke : tabletsToTest) {
        final List<String> files = tabletFiles.get(ke);
        Test test = new Test(ke) {
          public int runTest() throws Exception {
            return readFilesUsingIterStack(fs, sconf, files, auths, ke, columns, true);
          }
        };
        
        tests.add(test);
      }
      
      runTest("read tablet files w/ table iter stack", tests, numThreads, threadPool);
    }
    
    for (int i = 0; i < iterations; i++) {
      
      ArrayList<Test> tests = new ArrayList<Test>();
      
      final Connector conn = zki.getConnector(user, pass.getBytes());
      
      for (final KeyExtent ke : tabletsToTest) {
        Test test = new Test(ke) {
          public int runTest() throws Exception {
            return scanTablet(conn, tableName, auths, batchSize, ke.getPrevEndRow(), ke.getEndRow(), columns);
          }
        };
        
        tests.add(test);
      }
      
      runTest("read tablet data through accumulo", tests, numThreads, threadPool);
    }
    
    for (final KeyExtent ke : tabletsToTest) {
      final Connector conn = zki.getConnector(user, pass.getBytes());
      
      threadPool.submit(new Runnable() {
        public void run() {
          try {
            calcTabletStats(conn, tableName, auths, batchSize, ke, columns);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
    
    threadPool.shutdown();
  }
  
  private static abstract class Test implements Runnable {
    
    private int count;
    private long t1;
    private long t2;
    private CountDownLatch startCdl, finishCdl;
    private KeyExtent ke;
    
    Test(KeyExtent ke) {
      this.ke = ke;
    }
    
    public abstract int runTest() throws Exception;
    
    void setSignals(CountDownLatch scdl, CountDownLatch fcdl) {
      this.startCdl = scdl;
      this.finishCdl = fcdl;
    }
    
    public void run() {
      
      try {
        startCdl.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      t1 = System.currentTimeMillis();
      
      try {
        count = runTest();
      } catch (Exception e) {
        e.printStackTrace();
      }
      
      t2 = System.currentTimeMillis();
      
      double time = (t2 - t1) / 1000.0;
      
      System.out.printf("\t\ttablet: " + ke.getUUID() + "  thread: " + Thread.currentThread().getId()
          + " count: %,d cells  time: %6.2f  rate: %,6.2f cells/sec\n", count, time, count / time);
      
      finishCdl.countDown();
    }
    
    int getCount() {
      return count;
    }
    
    long getStartTime() {
      return t1;
    }
    
    long getFinishTime() {
      return t2;
    }
    
  }
  
  private static void runTest(String desc, List<Test> tests, int numThreads, ExecutorService threadPool) throws Exception {
    
    System.out.println("\tRunning test : " + desc);
    
    CountDownLatch startSignal = new CountDownLatch(1);
    CountDownLatch finishedSignal = new CountDownLatch(numThreads);
    
    for (Test test : tests) {
      threadPool.submit(test);
      test.setSignals(startSignal, finishedSignal);
    }
    
    startSignal.countDown();
    
    finishedSignal.await();
    
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    long count = 0;
    
    for (Test test : tests) {
      minTime = Math.min(test.getStartTime(), minTime);
      maxTime = Math.max(test.getFinishTime(), maxTime);
      count += test.getCount();
    }
    
    double time = (maxTime - minTime) / 1000.0;
    System.out.printf("\tAggregate stats  count: %,d cells  time: %6.2f  rate: %,6.2f cells/sec\n", count, time, count / time);
    System.out.println();
    
    // run the gc between test so that object created during previous test are not
    // collected in following test
    System.gc();
    System.gc();
    System.gc();
    
  }
  
  private static List<KeyExtent> findTablets(boolean selectLocalTablets, String user, String pass, String table, ZooKeeperInstance zki,
      Map<KeyExtent,String> locations) throws Exception {
    SortedSet<KeyExtent> tablets = new TreeSet<KeyExtent>();
    
    MetadataTable.getEntries(zki, new AuthInfo(user, ByteBuffer.wrap(pass.getBytes()), zki.getInstanceID()), table, false, locations, tablets);
    
    InetAddress localaddress = InetAddress.getLocalHost();
    
    List<KeyExtent> candidates = new ArrayList<KeyExtent>();
    
    for (Entry<KeyExtent,String> entry : locations.entrySet()) {
      boolean isLocal = AddressUtil.parseAddress(entry.getValue(), 4).getAddress().equals(localaddress);
      
      if (selectLocalTablets && isLocal) {
        candidates.add(entry.getKey());
      } else if (!selectLocalTablets && !isLocal) {
        candidates.add(entry.getKey());
      }
    }
    return candidates;
  }
  
  private static List<KeyExtent> selectRandomTablets(int numThreads, List<KeyExtent> candidates) {
    List<KeyExtent> tabletsToTest = new ArrayList<KeyExtent>();
    
    Random rand = new Random();
    for (int i = 0; i < numThreads; i++) {
      int rindex = rand.nextInt(candidates.size());
      tabletsToTest.add(candidates.get(rindex));
      Collections.swap(candidates, rindex, candidates.size() - 1);
      candidates = candidates.subList(0, candidates.size() - 1);
    }
    return tabletsToTest;
  }
  
  private static List<String> getTabletFiles(String user, String pass, ZooKeeperInstance zki, String tableId, KeyExtent ke) {
    List<String> files = new ArrayList<String>();
    
    SortedMap<Key,Value> tkv = new TreeMap<Key,Value>();
    MetadataTable.getTabletAndPrevTabletKeyValues(zki, tkv, ke, null, new AuthInfo(user, ByteBuffer.wrap(pass.getBytes()), zki.getInstanceID()));
    
    Set<Entry<Key,Value>> es = tkv.entrySet();
    for (Entry<Key,Value> entry : es) {
      if (entry.getKey().compareRow(ke.getMetadataEntry()) == 0) {
        if (entry.getKey().compareColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY) == 0) {
          files.add(ServerConstants.getTablesDir() + "/" + tableId + entry.getKey().getColumnQualifier());
        }
      }
    }
    return files;
  }
  
  private static void reportHdfsBlockLocations(List<String> files) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    
    System.out.println("\t\tFile block report : ");
    for (String file : files) {
      FileStatus status = fs.getFileStatus(new Path(file));
      
      if (status.isDir()) {
        // assume it is a map file
        status = fs.getFileStatus(new Path(file + "/data"));
      }
      
      BlockLocation[] locs = fs.getFileBlockLocations(status, 0, status.getLen());
      
      System.out.println("\t\t\tBlocks for : " + file);
      
      for (BlockLocation blockLocation : locs) {
        System.out.printf("\t\t\t\t offset : %,13d  hosts :", blockLocation.getOffset());
        for (String host : blockLocation.getHosts()) {
          System.out.print(" " + host);
        }
        System.out.println();
      }
    }
    
    System.out.println();
    
  }
  
  private static SortedKeyValueIterator<Key,Value> createScanIterator(KeyExtent ke, Collection<SortedKeyValueIterator<Key,Value>> mapfiles,
      Authorizations authorizations, byte[] defaultLabels, HashSet<Column> columnSet, List<IterInfo> ssiList, Map<String,Map<String,String>> ssio,
      boolean useTableIterators, TableConfiguration conf) throws IOException {
    
    SortedMapIterator smi = new SortedMapIterator(new TreeMap<Key,Value>());
    
    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<SortedKeyValueIterator<Key,Value>>(mapfiles.size() + 1);
    
    iters.addAll(mapfiles);
    iters.add(smi);
    
    MultiIterator multiIter = new MultiIterator(iters, ke);
    DeletingIterator delIter = new DeletingIterator(multiIter, false);
    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);
    ColumnQualifierFilter colFilter = new ColumnQualifierFilter(cfsi, columnSet);
    VisibilityFilter visFilter = new VisibilityFilter(colFilter, authorizations, defaultLabels);
    
    if (useTableIterators)
      return IteratorUtil.loadIterators(IteratorScope.scan, visFilter, ke, conf, ssiList, ssio, null);
    return visFilter;
  }
  
  private static int readFiles(FileSystem fs, AccumuloConfiguration aconf, List<String> files, KeyExtent ke, String[] columns) throws Exception {
    
    int count = 0;
    
    HashSet<ByteSequence> columnSet = createColumnBSS(columns);
    
    for (String file : files) {
      FileSKVIterator reader = FileOperations.getInstance().openReader(file, false, fs, fs.getConf(), aconf);
      Range range = new Range(ke.getPrevEndRow(), false, ke.getEndRow(), true);
      reader.seek(range, columnSet, columnSet.size() == 0 ? false : true);
      while (reader.hasTop() && !range.afterEndKey(reader.getTopKey())) {
        count++;
        reader.next();
      }
      reader.close();
    }
    
    return count;
  }
  
  private static HashSet<ByteSequence> createColumnBSS(String[] columns) {
    HashSet<ByteSequence> columnSet = new HashSet<ByteSequence>();
    for (String c : columns) {
      columnSet.add(new ArrayByteSequence(c));
    }
    return columnSet;
  }
  
  private static int readFilesUsingIterStack(FileSystem fs, ServerConfiguration aconf, List<String> files, String auths[], KeyExtent ke, String[] columns,
      boolean useTableIterators)
      throws Exception {
    
    SortedKeyValueIterator<Key,Value> reader;
    
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<SortedKeyValueIterator<Key,Value>>(files.size());
    
    for (String file : files) {
      readers.add(FileOperations.getInstance().openReader(file, false, fs, fs.getConf(), aconf.getConfiguration()));
    }
    
    List<IterInfo> emptyIterinfo = Collections.emptyList();
    Map<String,Map<String,String>> emptySsio = Collections.emptyMap();
    TableConfiguration tconf = aconf.getTableConfiguration(ke.getTableId().toString());
    reader = createScanIterator(ke, readers, new Authorizations(auths), new byte[] {}, new HashSet<Column>(), emptyIterinfo, emptySsio, useTableIterators, tconf);
    
    HashSet<ByteSequence> columnSet = createColumnBSS(columns);
    
    reader.seek(new Range(ke.getPrevEndRow(), false, ke.getEndRow(), true), columnSet, columnSet.size() == 0 ? false : true);
    
    int count = 0;
    
    while (reader.hasTop()) {
      count++;
      reader.next();
    }
    
    return count;
    
  }
  
  private static int scanTablet(Connector conn, String table, String[] auths, int batchSize, Text prevEndRow, Text endRow, String[] columns) throws Exception {
    
    Scanner scanner = conn.createScanner(table, new Authorizations(auths));
    scanner.setBatchSize(batchSize);
    scanner.setRange(new Range(prevEndRow, false, endRow, true));
    
    for (String c : columns) {
      scanner.fetchColumnFamily(new Text(c));
    }
    
    int count = 0;
    
    for (Entry<Key,Value> entry : scanner) {
      if (entry != null)
        count++;
    }
    
    return count;
  }
  
  private static void calcTabletStats(Connector conn, String table, String[] auths, int batchSize, KeyExtent ke, String[] columns) throws Exception {
    
    // long t1 = System.currentTimeMillis();
    
    Scanner scanner = conn.createScanner(table, new Authorizations(auths));
    scanner.setBatchSize(batchSize);
    scanner.setRange(new Range(ke.getPrevEndRow(), false, ke.getEndRow(), true));
    
    for (String c : columns) {
      scanner.fetchColumnFamily(new Text(c));
    }
    
    Stat rowLen = new Stat();
    Stat cfLen = new Stat();
    Stat cqLen = new Stat();
    Stat cvLen = new Stat();
    Stat valLen = new Stat();
    Stat colsPerRow = new Stat();
    
    Text lastRow = null;
    int colsPerRowCount = 0;
    
    for (Entry<Key,Value> entry : scanner) {
      
      Key key = entry.getKey();
      Text row = key.getRow();
      
      if (lastRow == null) {
        lastRow = row;
      }
      
      if (!lastRow.equals(row)) {
        colsPerRow.addStat(colsPerRowCount);
        lastRow = row;
        colsPerRowCount = 0;
      }
      
      colsPerRowCount++;
      
      rowLen.addStat(row.getLength());
      cfLen.addStat(key.getColumnFamilyData().length());
      cqLen.addStat(key.getColumnQualifierData().length());
      cvLen.addStat(key.getColumnVisibilityData().length());
      valLen.addStat(entry.getValue().get().length);
    }
    
    synchronized (System.out) {
      System.out.println("");
      System.out.println("\tTablet " + ke.getUUID() + " statistics : ");
      printStat("Row length", rowLen);
      printStat("Column family length", cfLen);
      printStat("Column qualifier length", cqLen);
      printStat("Column visibility length", cvLen);
      printStat("Value length", valLen);
      printStat("Columns per row", colsPerRow);
      System.out.println("");
    }
    
  }
  
  private static void printStat(String desc, Stat s) {
    System.out.printf("\t\tDescription: [%30s]  average: %,6.2f  std dev: %,6.2f  min: %,d  max: %,d \n", desc, s.getAverage(), s.getStdDev(), s.getMin(),
        s.getMax());
    
  }
  
}

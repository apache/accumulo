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
package org.apache.accumulo.test.performance.scan;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
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
import org.apache.accumulo.core.metadata.MetadataServicer;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.server.cli.ClientOnRequiredTable;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.net.HostAndPort;

public class CollectTabletStats {
  private static final Logger log = LoggerFactory.getLogger(CollectTabletStats.class);

  static class CollectOptions extends ClientOnRequiredTable {
    @Parameter(names = "--iterations", description = "number of iterations")
    int iterations = 3;
    @Parameter(names = "--numThreads", description = "number of threads")
    int numThreads = 1;
    @Parameter(names = "-f", description = "select far tablets, default is to use local tablets")
    boolean selectFarTablets = false;
    @Parameter(names = "-c", description = "comma separated list of columns")
    String columns;
  }

  public static void main(String[] args) throws Exception {

    final CollectOptions opts = new CollectOptions();
    final ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(CollectTabletStats.class.getName(), args, scanOpts);

    String columnsTmp[] = new String[] {};
    if (opts.columns != null)
      columnsTmp = opts.columns.split(",");
    final String columns[] = columnsTmp;

    final VolumeManager fs = VolumeManagerImpl.get();

    Instance instance = opts.getInstance();
    final ServerConfigurationFactory sconf = new ServerConfigurationFactory(instance);
    Credentials creds = new Credentials(opts.getPrincipal(), opts.getToken());
    ClientContext context = new ClientContext(instance, creds, sconf.getSystemConfiguration());

    String tableId = Tables.getNameToIdMap(instance).get(opts.getTableName());
    if (tableId == null) {
      log.error("Unable to find table named " + opts.getTableName());
      System.exit(-1);
    }

    TreeMap<KeyExtent,String> tabletLocations = new TreeMap<>();
    List<KeyExtent> candidates = findTablets(context, !opts.selectFarTablets, opts.getTableName(), tabletLocations);

    if (candidates.size() < opts.numThreads) {
      System.err.println("ERROR : Unable to find " + opts.numThreads + " " + (opts.selectFarTablets ? "far" : "local") + " tablets");
      System.exit(-1);
    }

    List<KeyExtent> tabletsToTest = selectRandomTablets(opts.numThreads, candidates);

    Map<KeyExtent,List<FileRef>> tabletFiles = new HashMap<>();

    for (KeyExtent ke : tabletsToTest) {
      List<FileRef> files = getTabletFiles(context, tableId, ke);
      tabletFiles.put(ke, files);
    }

    System.out.println();
    System.out.println("run location      : " + InetAddress.getLocalHost().getHostName() + "/" + InetAddress.getLocalHost().getHostAddress());
    System.out.println("num threads       : " + opts.numThreads);
    System.out.println("table             : " + opts.getTableName());
    System.out.println("table id          : " + tableId);

    for (KeyExtent ke : tabletsToTest) {
      System.out.println("\t *** Information about tablet " + ke.getUUID() + " *** ");
      System.out.println("\t\t# files in tablet : " + tabletFiles.get(ke).size());
      System.out.println("\t\ttablet location   : " + tabletLocations.get(ke));
      reportHdfsBlockLocations(tabletFiles.get(ke));
    }

    System.out.println("%n*** RUNNING TEST ***%n");

    ExecutorService threadPool = Executors.newFixedThreadPool(opts.numThreads);

    for (int i = 0; i < opts.iterations; i++) {

      ArrayList<Test> tests = new ArrayList<>();

      for (final KeyExtent ke : tabletsToTest) {
        final List<FileRef> files = tabletFiles.get(ke);
        Test test = new Test(ke) {
          @Override
          public int runTest() throws Exception {
            return readFiles(fs, sconf.getSystemConfiguration(), files, ke, columns);
          }

        };

        tests.add(test);
      }

      runTest("read files", tests, opts.numThreads, threadPool);
    }

    for (int i = 0; i < opts.iterations; i++) {

      ArrayList<Test> tests = new ArrayList<>();

      for (final KeyExtent ke : tabletsToTest) {
        final List<FileRef> files = tabletFiles.get(ke);
        Test test = new Test(ke) {
          @Override
          public int runTest() throws Exception {
            return readFilesUsingIterStack(fs, sconf, files, opts.auths, ke, columns, false);
          }
        };

        tests.add(test);
      }

      runTest("read tablet files w/ system iter stack", tests, opts.numThreads, threadPool);
    }

    for (int i = 0; i < opts.iterations; i++) {
      ArrayList<Test> tests = new ArrayList<>();

      for (final KeyExtent ke : tabletsToTest) {
        final List<FileRef> files = tabletFiles.get(ke);
        Test test = new Test(ke) {
          @Override
          public int runTest() throws Exception {
            return readFilesUsingIterStack(fs, sconf, files, opts.auths, ke, columns, true);
          }
        };

        tests.add(test);
      }

      runTest("read tablet files w/ table iter stack", tests, opts.numThreads, threadPool);
    }

    for (int i = 0; i < opts.iterations; i++) {

      ArrayList<Test> tests = new ArrayList<>();

      final Connector conn = opts.getConnector();

      for (final KeyExtent ke : tabletsToTest) {
        Test test = new Test(ke) {
          @Override
          public int runTest() throws Exception {
            return scanTablet(conn, opts.getTableName(), opts.auths, scanOpts.scanBatchSize, ke.getPrevEndRow(), ke.getEndRow(), columns);
          }
        };

        tests.add(test);
      }

      runTest("read tablet data through accumulo", tests, opts.numThreads, threadPool);
    }

    for (final KeyExtent ke : tabletsToTest) {
      final Connector conn = opts.getConnector();

      threadPool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            calcTabletStats(conn, opts.getTableName(), opts.auths, scanOpts.scanBatchSize, ke, columns);
          } catch (Exception e) {
            log.error("Failed to calculate tablet stats.", e);
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

    @Override
    public void run() {

      try {
        startCdl.await();
      } catch (InterruptedException e) {
        log.error("startCdl.await() failed.", e);
      }

      t1 = System.currentTimeMillis();

      try {
        count = runTest();
      } catch (Exception e) {
        log.error("runTest() failed.", e);
      }

      t2 = System.currentTimeMillis();

      double time = (t2 - t1) / 1000.0;

      System.out.printf("\t\ttablet: " + ke.getUUID() + "  thread: " + Thread.currentThread().getId()
          + " count: %,d cells  time: %6.2f  rate: %,6.2f cells/sec%n", count, time, count / time);

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
    System.out.printf("\tAggregate stats  count: %,d cells  time: %6.2f  rate: %,6.2f cells/sec%n", count, time, count / time);
    System.out.println();

    // run the gc between test so that object created during previous test are not
    // collected in following test
    System.gc();
    System.gc();
    System.gc();

  }

  private static List<KeyExtent> findTablets(ClientContext context, boolean selectLocalTablets, String tableName, SortedMap<KeyExtent,String> tabletLocations)
      throws Exception {

    String tableId = Tables.getNameToIdMap(context.getInstance()).get(tableName);
    MetadataServicer.forTableId(context, tableId).getTabletLocations(tabletLocations);

    InetAddress localaddress = InetAddress.getLocalHost();

    List<KeyExtent> candidates = new ArrayList<>();

    for (Entry<KeyExtent,String> entry : tabletLocations.entrySet()) {
      String loc = entry.getValue();
      if (loc != null) {
        boolean isLocal = HostAndPort.fromString(entry.getValue()).getHostText().equals(localaddress.getHostName());

        if (selectLocalTablets && isLocal) {
          candidates.add(entry.getKey());
        } else if (!selectLocalTablets && !isLocal) {
          candidates.add(entry.getKey());
        }
      }
    }
    return candidates;
  }

  private static List<KeyExtent> selectRandomTablets(int numThreads, List<KeyExtent> candidates) {
    List<KeyExtent> tabletsToTest = new ArrayList<>();

    Random rand = new Random();
    for (int i = 0; i < numThreads; i++) {
      int rindex = rand.nextInt(candidates.size());
      tabletsToTest.add(candidates.get(rindex));
      Collections.swap(candidates, rindex, candidates.size() - 1);
      candidates = candidates.subList(0, candidates.size() - 1);
    }
    return tabletsToTest;
  }

  private static List<FileRef> getTabletFiles(ClientContext context, String tableId, KeyExtent ke) throws IOException {
    return new ArrayList<>(MetadataTableUtil.getDataFileSizes(ke, context).keySet());
  }

  private static void reportHdfsBlockLocations(List<FileRef> files) throws Exception {
    VolumeManager fs = VolumeManagerImpl.get();

    System.out.println("\t\tFile block report : ");
    for (FileRef file : files) {
      FileStatus status = fs.getFileStatus(file.path());

      if (status.isDirectory()) {
        // assume it is a map file
        status = fs.getFileStatus(new Path(file + "/data"));
      }
      FileSystem ns = fs.getVolumeByPath(file.path()).getFileSystem();
      BlockLocation[] locs = ns.getFileBlockLocations(status, 0, status.getLen());

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

    List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(mapfiles.size() + 1);

    iters.addAll(mapfiles);
    iters.add(smi);

    MultiIterator multiIter = new MultiIterator(iters, ke);
    DeletingIterator delIter = new DeletingIterator(multiIter, false);
    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);
    SortedKeyValueIterator<Key,Value> colFilter = ColumnQualifierFilter.wrap(cfsi, columnSet);
    SortedKeyValueIterator<Key,Value> visFilter = VisibilityFilter.wrap(colFilter, authorizations, defaultLabels);

    if (useTableIterators)
      return IteratorUtil.loadIterators(IteratorScope.scan, visFilter, ke, conf, ssiList, ssio, null);
    return visFilter;
  }

  private static int readFiles(VolumeManager fs, AccumuloConfiguration aconf, List<FileRef> files, KeyExtent ke, String[] columns) throws Exception {

    int count = 0;

    HashSet<ByteSequence> columnSet = createColumnBSS(columns);

    for (FileRef file : files) {
      FileSystem ns = fs.getVolumeByPath(file.path()).getFileSystem();
      FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder().forFile(file.path().toString(), ns, ns.getConf()).withTableConfiguration(aconf)
          .build();
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
    HashSet<ByteSequence> columnSet = new HashSet<>();
    for (String c : columns) {
      columnSet.add(new ArrayByteSequence(c));
    }
    return columnSet;
  }

  private static int readFilesUsingIterStack(VolumeManager fs, ServerConfigurationFactory aconf, List<FileRef> files, Authorizations auths, KeyExtent ke,
      String[] columns, boolean useTableIterators) throws Exception {

    SortedKeyValueIterator<Key,Value> reader;

    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());

    for (FileRef file : files) {
      FileSystem ns = fs.getVolumeByPath(file.path()).getFileSystem();
      readers.add(FileOperations.getInstance().newReaderBuilder().forFile(file.path().toString(), ns, ns.getConf())
          .withTableConfiguration(aconf.getSystemConfiguration()).build());
    }

    List<IterInfo> emptyIterinfo = Collections.emptyList();
    Map<String,Map<String,String>> emptySsio = Collections.emptyMap();
    TableConfiguration tconf = aconf.getTableConfiguration(ke.getTableId());
    reader = createScanIterator(ke, readers, auths, new byte[] {}, new HashSet<Column>(), emptyIterinfo, emptySsio, useTableIterators, tconf);

    HashSet<ByteSequence> columnSet = createColumnBSS(columns);

    reader.seek(new Range(ke.getPrevEndRow(), false, ke.getEndRow(), true), columnSet, columnSet.size() == 0 ? false : true);

    int count = 0;

    while (reader.hasTop()) {
      count++;
      reader.next();
    }

    return count;

  }

  private static int scanTablet(Connector conn, String table, Authorizations auths, int batchSize, Text prevEndRow, Text endRow, String[] columns)
      throws Exception {

    Scanner scanner = conn.createScanner(table, auths);
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

  private static void calcTabletStats(Connector conn, String table, Authorizations auths, int batchSize, KeyExtent ke, String[] columns) throws Exception {

    // long t1 = System.currentTimeMillis();

    Scanner scanner = conn.createScanner(table, auths);
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
    System.out.printf("\t\tDescription: [%30s]  average: %,6.2f  std dev: %,6.2f  min: %,d  max: %,d %n", desc, s.getAverage(), s.getStdDev(), s.getMin(),
        s.getMax());

  }

}

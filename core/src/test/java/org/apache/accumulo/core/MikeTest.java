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
package org.apache.accumulo.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.StatsIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by miller on 3/30/17.
 */
public class MikeTest {
  private File testFile;
  private LocalFileSystem localFs;
  private Scanner scanner;

  @Before
  public void setUp() throws Exception {
    testFile = new File("Mike.rf");
    localFs = FileSystem.getLocal(new Configuration());
  }

  @After
  public void tearDown() throws Exception {
    if(testFile.delete())
      System.out.println("Test finished and removed " + testFile.getAbsolutePath());
  }

  String colStr(int c) {
    return String.format("%08x", c);
  }

  public void populateRfile() throws IOException {
    // write some test files

    System.out.println("Populating RFile");
    RFileWriter writer = RFile.newWriter().to(testFile.getAbsolutePath()).withFileSystem(localFs).build();
    for (int i = 0; i < 1_000_000; i++) {
      Integer num = new Random().nextInt(100);
      Integer day = new Random().nextInt(365);
      String[] names = {"Mike", "Keith", "Christopher", "Luis", "Sterling"};
      writer.append(new Key("person" + colStr(i), "day"), new Value(day.toString()));
      writer.append(new Key("person" + colStr(i), "name", "first"), new Value(names[new Random().nextInt(5)]));
      writer.append(new Key("person" + colStr(i), "rank"), new Value(num.toString()));
    }
    writer.close();
    scanner = RFile.newScanner().from(testFile.getAbsolutePath()).withFileSystem(localFs).build();
  }

  private static SortedKeyValueIterator<Key,Value> createIteratorStack(List<SortedKeyValueIterator<Key,Value>> sources, AtomicBoolean interruptFlag,
      AtomicLong scannedCount, AtomicLong seekCount, KeyExtent extent, AccumuloConfiguration accuConf) throws IOException {

    MultiIterator multiIter = new MultiIterator(sources, extent);

    IteratorEnvironment iterEnv = new IteratorEnvironment() {

      public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
        throw new UnsupportedOperationException();
      }

      public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Authorizations getAuthorizations() {
        return null;
      }

      @Override
      public IteratorEnvironment cloneWithSamplingEnabled() {
        return null;
      }

      @Override
      public boolean isSamplingEnabled() {
        return false;
      }

      @Override
      public SamplerConfiguration getSamplerConfiguration() {
        return null;
      }

      public boolean isFullMajorCompaction() {
        return false;
      }

      public IteratorScope getIteratorScope() {
        return IteratorScope.scan;
      }

      public AccumuloConfiguration getConfig() {
        return null;
      }
    };

    StatsIterator statsIterator = new StatsIterator(multiIter, seekCount, scannedCount);

    DeletingIterator delIter = new DeletingIterator(statsIterator, false);

    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);

    ColumnQualifierFilter colFilter = new ColumnQualifierFilter(cfsi, Collections.<Column> emptySet());

    VisibilityFilter visFilter = new VisibilityFilter(cfsi, new Authorizations(), new byte[0]);

    return IteratorUtil.loadIterators(IteratorScope.scan, (SortedKeyValueIterator<Key,Value>) visFilter, extent, accuConf, Collections.<IterInfo> emptyList(),
        Collections.<String,Map<String,String>> emptyMap(), iterEnv);
  }

  private double runScan() {
    scanner.setRange(new Range());
    long mark = System.currentTimeMillis();

    long count = 0L;
    for (Entry<Key,Value> entry : scanner) {
      entry.getKey();
      entry.getValue();
      count++;
    }

    long lastMark = mark;
    mark = System.currentTimeMillis();
    return count / (.001 * (mark - lastMark));
  }

  @Test
  public void testStack() throws IOException {
    AtomicBoolean interruptFlag = new AtomicBoolean();
    AtomicLong scannedCount = new AtomicLong();
    AtomicLong seekCount = new AtomicLong();
    KeyExtent extent = new KeyExtent();
    extent.setPrevEndRow(null);
    extent.setEndRow(null);
    AccumuloConfiguration accuConf = AccumuloConfiguration.getDefaultConfiguration();

    TreeMap<Key,Value> imm1 = new TreeMap<Key,Value>();
    TreeMap<Key,Value> imm2 = new TreeMap<Key,Value>();
    String row = null;
    String col = null;
    Random r = new Random();
    byte[] value = new byte[0];
    for (int i = 0; i < 1000000; i++) {
      row = String.format("%04d", i % 10000);
      col = String.format("%04d", i / 10000);
      r.nextBytes(value);
      if ((i / 10) % 2 == 0)
        imm1.put(new Key(row, "cf", col), new Value(value));
      else
        imm2.put(new Key(row, "cf", col), new Value(value));
    }

    System.out.println(imm1.firstKey());

    List<SortedKeyValueIterator<Key,Value>> sourceIters = new ArrayList<SortedKeyValueIterator<Key,Value>>();
    sourceIters.add(new SortedMapIterator(imm1));
    sourceIters.add(new SortedMapIterator(imm2));

    // scan and measure performance of iterators
    long count = 0;
    long mark = System.currentTimeMillis();
    long time = 0;
    long total = 0;
    for (int i = 0; i < 12; i++) {
      // create an iterator stack
      SortedKeyValueIterator<Key,Value> iterStack = createIteratorStack(sourceIters, interruptFlag, scannedCount, seekCount, extent, accuConf);
      Range everything = new Range();
      iterStack.seek(everything, Collections.<ByteSequence> emptySet(), false);
      while (iterStack.hasTop()) {
        iterStack.next();
        count++;
        if (count % 1000000 == 0) {
          long lastMark = mark;
          mark = System.currentTimeMillis();
          time = (1000000 / (mark - lastMark));
          System.out.println(time);
        }
      }
      if (i != 0 && i != 1) {
        total += time;
      }
    }
    System.out.println("Total Average of 10 runs = " + total / 10);
  }

  @Test
  public void testScan1() throws IOException {
    populateRfile();
    double total = 0;
    double entriesScanedPerSecond = runScan();
    System.out.println("Dump first 1st scans " + entriesScanedPerSecond);
    entriesScanedPerSecond = runScan();
    System.out.println("Dump first 2nd scan " + entriesScanedPerSecond);
    entriesScanedPerSecond = runScan();
    // System.out.println("Test1 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test2 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test3 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test4 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test5 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test6 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test7 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test8 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test9 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;
    entriesScanedPerSecond = runScan();
    // System.out.println("Test10 Entries scanned per second = " + String.format("%.0f", entriesScanedPerSecond));
    total += entriesScanedPerSecond;

    System.out.println("Average scanned per sec = " + String.format("%.0f", (total / 10)));
  }

}

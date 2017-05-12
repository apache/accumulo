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

package org.apache.accumulo.core.client.rfile;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.CounterSummary;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.client.summary.summarizers.FamilySummarizer;
import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class RFileTest {

  // method created to foil findbugs... it was complaining ret val not used when it did not matter
  private void foo(boolean b) {}

  private String createTmpTestFile() throws IOException {
    File dir = new File(System.getProperty("user.dir") + "/target/rfile-test");
    foo(dir.mkdirs());
    File testFile = File.createTempFile("test", ".rf", dir);
    foo(testFile.delete());
    return testFile.getAbsolutePath();
  }

  String rowStr(int r) {
    return String.format("%06x", r);
  }

  String colStr(int c) {
    return String.format("%04x", c);
  }

  private SortedMap<Key,Value> createTestData(int rows, int families, int qualifiers) {
    return createTestData(0, rows, 0, families, qualifiers);
  }

  private SortedMap<Key,Value> createTestData(int startRow, int rows, int startFamily, int families, int qualifiers) {
    return createTestData(startRow, rows, startFamily, families, qualifiers, "");
  }

  private SortedMap<Key,Value> createTestData(int startRow, int rows, int startFamily, int families, int qualifiers, String... vis) {
    TreeMap<Key,Value> testData = new TreeMap<>();

    for (int r = 0; r < rows; r++) {
      String row = rowStr(r + startRow);
      for (int f = 0; f < families; f++) {
        String fam = colStr(f + startFamily);
        for (int q = 0; q < qualifiers; q++) {
          String qual = colStr(q);
          for (String v : vis) {
            Key k = new Key(row, fam, qual, v);
            testData.put(k, new Value((k.hashCode() + "").getBytes()));
          }
        }
      }
    }

    return testData;
  }

  private String createRFile(SortedMap<Key,Value> testData) throws Exception {
    String testFile = createTmpTestFile();

    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(FileSystem.getLocal(new Configuration())).build()) {
      writer.append(testData.entrySet());
      // TODO ensure compressors are returned
    }

    return testFile;
  }

  @Test
  public void testIndependance() throws Exception {
    // test to ensure two iterators allocated from same RFile scanner are independent.

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    SortedMap<Key,Value> testData = createTestData(10, 10, 10);

    String testFile = createRFile(testData);

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    Range range1 = Range.exact(rowStr(5));
    scanner.setRange(range1);
    Iterator<Entry<Key,Value>> scnIter1 = scanner.iterator();
    Iterator<Entry<Key,Value>> mapIter1 = testData.subMap(range1.getStartKey(), range1.getEndKey()).entrySet().iterator();

    Range range2 = new Range(rowStr(3), true, rowStr(4), true);
    scanner.setRange(range2);
    Iterator<Entry<Key,Value>> scnIter2 = scanner.iterator();
    Iterator<Entry<Key,Value>> mapIter2 = testData.subMap(range2.getStartKey(), range2.getEndKey()).entrySet().iterator();

    while (scnIter1.hasNext() || scnIter2.hasNext()) {
      if (scnIter1.hasNext()) {
        Assert.assertTrue(mapIter1.hasNext());
        Assert.assertEquals(scnIter1.next(), mapIter1.next());
      } else {
        Assert.assertFalse(mapIter1.hasNext());
      }

      if (scnIter2.hasNext()) {
        Assert.assertTrue(mapIter2.hasNext());
        Assert.assertEquals(scnIter2.next(), mapIter2.next());
      } else {
        Assert.assertFalse(mapIter2.hasNext());
      }
    }

    Assert.assertFalse(mapIter1.hasNext());
    Assert.assertFalse(mapIter2.hasNext());

    scanner.close();
  }

  SortedMap<Key,Value> toMap(Scanner scanner) {
    TreeMap<Key,Value> map = new TreeMap<>();
    for (Entry<Key,Value> entry : scanner) {
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  @Test
  public void testMultipleSources() throws Exception {
    SortedMap<Key,Value> testData1 = createTestData(10, 10, 10);
    SortedMap<Key,Value> testData2 = createTestData(0, 10, 0, 10, 10);

    String testFile1 = createRFile(testData1);
    String testFile2 = createRFile(testData2);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    Scanner scanner = RFile.newScanner().from(testFile1, testFile2).withFileSystem(localFs).build();

    TreeMap<Key,Value> expected = new TreeMap<>(testData1);
    expected.putAll(testData2);

    Assert.assertEquals(expected, toMap(scanner));

    Range range = new Range(rowStr(3), true, rowStr(14), true);
    scanner.setRange(range);
    Assert.assertEquals(expected.subMap(range.getStartKey(), range.getEndKey()), toMap(scanner));

    scanner.close();
  }

  @Test
  public void testWriterTableProperties() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    String testFile = createTmpTestFile();

    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K");
    props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), "1K");
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).withTableProperties(props).build();

    SortedMap<Key,Value> testData1 = createTestData(10, 10, 10);
    writer.append(testData1.entrySet());
    writer.close();

    Reader reader = getReader(localFs, testFile);
    FileSKVIterator iiter = reader.getIndex();

    int count = 0;
    while (iiter.hasTop()) {
      count++;
      iiter.next();
    }

    // if settings are used then should create multiple index entries
    Assert.assertTrue(count > 10);

    reader.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    Assert.assertEquals(testData1, toMap(scanner));
    scanner.close();
  }

  @Test
  public void testLocalityGroups() throws Exception {

    SortedMap<Key,Value> testData1 = createTestData(0, 10, 0, 2, 10);
    SortedMap<Key,Value> testData2 = createTestData(0, 10, 2, 1, 10);
    SortedMap<Key,Value> defaultData = createTestData(0, 10, 3, 7, 10);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();

    writer.startNewLocalityGroup("z", colStr(0), colStr(1));
    writer.append(testData1.entrySet());

    writer.startNewLocalityGroup("h", colStr(2));
    writer.append(testData2.entrySet());

    writer.startDefaultLocalityGroup();
    writer.append(defaultData.entrySet());

    writer.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();

    scanner.fetchColumnFamily(new Text(colStr(0)));
    scanner.fetchColumnFamily(new Text(colStr(1)));
    Assert.assertEquals(testData1, toMap(scanner));

    scanner.clearColumns();
    scanner.fetchColumnFamily(new Text(colStr(2)));
    Assert.assertEquals(testData2, toMap(scanner));

    scanner.clearColumns();
    for (int i = 3; i < 10; i++) {
      scanner.fetchColumnFamily(new Text(colStr(i)));
    }
    Assert.assertEquals(defaultData, toMap(scanner));

    scanner.clearColumns();
    Assert.assertEquals(createTestData(10, 10, 10), toMap(scanner));

    scanner.close();

    Reader reader = getReader(localFs, testFile);
    Map<String,ArrayList<ByteSequence>> lGroups = reader.getLocalityGroupCF();
    Assert.assertTrue(lGroups.containsKey("z"));
    Assert.assertTrue(lGroups.get("z").size() == 2);
    Assert.assertTrue(lGroups.get("z").contains(new ArrayByteSequence(colStr(0))));
    Assert.assertTrue(lGroups.get("z").contains(new ArrayByteSequence(colStr(1))));
    Assert.assertTrue(lGroups.containsKey("h"));
    Assert.assertEquals(Arrays.asList(new ArrayByteSequence(colStr(2))), lGroups.get("h"));
    reader.close();
  }

  @Test
  public void testIterators() throws Exception {

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    SortedMap<Key,Value> testData = createTestData(10, 10, 10);
    String testFile = createRFile(testData);

    IteratorSetting is = new IteratorSetting(50, "regex", RegExFilter.class);
    RegExFilter.setRegexs(is, ".*00000[78].*", null, null, null, false);

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    scanner.addScanIterator(is);

    Assert.assertEquals(createTestData(7, 2, 0, 10, 10), toMap(scanner));

    scanner.close();
  }

  @Test
  public void testAuths() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();

    Key k1 = new Key("r1", "f1", "q1", "A&B");
    Key k2 = new Key("r1", "f1", "q2", "A");
    Key k3 = new Key("r1", "f1", "q3");

    Value v1 = new Value("p".getBytes());
    Value v2 = new Value("c".getBytes());
    Value v3 = new Value("t".getBytes());

    writer.append(k1, v1);
    writer.append(k2, v2);
    writer.append(k3, v3);
    writer.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withAuthorizations(new Authorizations("A")).build();
    Assert.assertEquals(ImmutableMap.of(k2, v2, k3, v3), toMap(scanner));
    Assert.assertEquals(new Authorizations("A"), scanner.getAuthorizations());
    scanner.close();

    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withAuthorizations(new Authorizations("A", "B")).build();
    Assert.assertEquals(ImmutableMap.of(k1, v1, k2, v2, k3, v3), toMap(scanner));
    Assert.assertEquals(new Authorizations("A", "B"), scanner.getAuthorizations());
    scanner.close();

    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withAuthorizations(new Authorizations("B")).build();
    Assert.assertEquals(ImmutableMap.of(k3, v3), toMap(scanner));
    Assert.assertEquals(new Authorizations("B"), scanner.getAuthorizations());
    scanner.close();
  }

  @Test
  public void testNoSystemIters() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();

    Key k1 = new Key("r1", "f1", "q1");
    k1.setTimestamp(3);

    Key k2 = new Key("r1", "f1", "q1");
    k2.setTimestamp(6);
    k2.setDeleted(true);

    Value v1 = new Value("p".getBytes());
    Value v2 = new Value("".getBytes());

    writer.append(k2, v2);
    writer.append(k1, v1);
    writer.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    Assert.assertFalse(scanner.iterator().hasNext());
    scanner.close();

    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withoutSystemIterators().build();
    Assert.assertEquals(ImmutableMap.of(k2, v2, k1, v1), toMap(scanner));
    scanner.setRange(new Range("r2"));
    Assert.assertFalse(scanner.iterator().hasNext());
    scanner.close();
  }

  @Test
  public void testBounds() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    SortedMap<Key,Value> testData = createTestData(10, 10, 10);
    String testFile = createRFile(testData);

    // set a lower bound row
    Range bounds = new Range(rowStr(3), false, null, true);
    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withBounds(bounds).build();
    Assert.assertEquals(createTestData(4, 6, 0, 10, 10), toMap(scanner));
    scanner.close();

    // set an upper bound row
    bounds = new Range(null, false, rowStr(7), true);
    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withBounds(bounds).build();
    Assert.assertEquals(createTestData(8, 10, 10), toMap(scanner));
    scanner.close();

    // set row bounds
    bounds = new Range(rowStr(3), false, rowStr(7), true);
    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withBounds(bounds).build();
    Assert.assertEquals(createTestData(4, 4, 0, 10, 10), toMap(scanner));
    scanner.close();

    // set a row family bound
    bounds = Range.exact(rowStr(3), colStr(5));
    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withBounds(bounds).build();
    Assert.assertEquals(createTestData(3, 1, 5, 1, 10), toMap(scanner));
    scanner.close();
  }

  @Test
  public void testScannerTableProperties() throws Exception {
    NewTableConfiguration ntc = new NewTableConfiguration();

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build();

    Key k1 = new Key("r1", "f1", "q1");
    k1.setTimestamp(3);

    Key k2 = new Key("r1", "f1", "q1");
    k2.setTimestamp(6);

    Value v1 = new Value("p".getBytes());
    Value v2 = new Value("q".getBytes());

    writer.append(k2, v2);
    writer.append(k1, v1);
    writer.close();

    // pass in table config that has versioning iterator configured
    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withTableProperties(ntc.getProperties()).build();
    Assert.assertEquals(ImmutableMap.of(k2, v2), toMap(scanner));
    scanner.close();

    scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    Assert.assertEquals(ImmutableMap.of(k2, v2, k1, v1), toMap(scanner));
    scanner.close();
  }

  @Test
  public void testSampling() throws Exception {

    SortedMap<Key,Value> testData1 = createTestData(1000, 2, 1);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();

    SamplerConfiguration sc = new SamplerConfiguration(RowSampler.class).setOptions(ImmutableMap.of("hasher", "murmur3_32", "modulus", "19"));

    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).withSampler(sc).build();
    writer.append(testData1.entrySet());
    writer.close();

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();
    scanner.setSamplerConfiguration(sc);

    RowSampler rowSampler = new RowSampler();
    rowSampler.init(sc);

    SortedMap<Key,Value> sampleData = new TreeMap<>();
    for (Entry<Key,Value> e : testData1.entrySet()) {
      if (rowSampler.accept(e.getKey())) {
        sampleData.put(e.getKey(), e.getValue());
      }
    }

    Assert.assertTrue(sampleData.size() < testData1.size());

    Assert.assertEquals(sampleData, toMap(scanner));

    scanner.clearSamplerConfiguration();

    Assert.assertEquals(testData1, toMap(scanner));

  }

  @Test
  public void testAppendScanner() throws Exception {
    SortedMap<Key,Value> testData = createTestData(10000, 1, 1);
    String testFile = createRFile(testData);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());

    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).build();

    String testFile2 = createTmpTestFile();
    RFileWriter writer = RFile.newWriter().to(testFile2).build();
    writer.append(scanner);
    writer.close();
    scanner.close();

    scanner = RFile.newScanner().from(testFile2).withFileSystem(localFs).build();
    Assert.assertEquals(testData, toMap(scanner));
    scanner.close();
  }

  @Test
  public void testCache() throws Exception {
    SortedMap<Key,Value> testData = createTestData(10000, 1, 1);
    String testFile = createRFile(testData);

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withIndexCache(1000000).withDataCache(10000000).build();

    Random rand = new Random(5);

    for (int i = 0; i < 100; i++) {
      int r = rand.nextInt(10000);
      scanner.setRange(new Range(rowStr(r)));
      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(rowStr(r), iter.next().getKey().getRow().toString());
      Assert.assertFalse(iter.hasNext());
    }

    scanner.close();
  }

  @Test
  public void testSummaries() throws Exception {
    SummarizerConfiguration sc1 = SummarizerConfiguration.builder(VisibilitySummarizer.class).build();
    SummarizerConfiguration sc2 = SummarizerConfiguration.builder(FamilySummarizer.class).build();

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();

    SortedMap<Key,Value> testData1 = createTestData(0, 100, 0, 4, 1, "A&B", "A&B&C");

    RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).withSummarizers(sc1, sc2).build();
    writer.append(testData1.entrySet());
    writer.close();

    // verify summary data
    Collection<Summary> summaries = RFile.summaries().from(testFile).withFileSystem(localFs).read();
    Assert.assertEquals(2, summaries.size());
    for (Summary summary : summaries) {
      Assert.assertEquals(0, summary.getFileStatistics().getInaccurate());
      Assert.assertEquals(1, summary.getFileStatistics().getTotal());
      String className = summary.getSummarizerConfiguration().getClassName();
      CounterSummary counterSummary = new CounterSummary(summary);
      if (className.equals(FamilySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();
        Map<String,Long> expected = ImmutableMap.of("0000", 200l, "0001", 200l, "0002", 200l, "0003", 200l);
        Assert.assertEquals(expected, counters);
      } else if (className.equals(VisibilitySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();
        Map<String,Long> expected = ImmutableMap.of("A&B", 400l, "A&B&C", 400l);
        Assert.assertEquals(expected, counters);
      } else {
        Assert.fail("Unexpected classname " + className);
      }
    }

    // check if writing summary data impacted normal rfile functionality
    Scanner scanner = RFile.newScanner().from(testFile).withFileSystem(localFs).withAuthorizations(new Authorizations("A", "B", "C")).build();
    Assert.assertEquals(testData1, toMap(scanner));
    scanner.close();

    String testFile2 = createTmpTestFile();
    SortedMap<Key,Value> testData2 = createTestData(100, 100, 0, 4, 1, "A&B", "A&B&C");
    writer = RFile.newWriter().to(testFile2).withFileSystem(localFs).withSummarizers(sc1, sc2).build();
    writer.append(testData2.entrySet());
    writer.close();

    // verify reading summaries from multiple files works
    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).read();
    Assert.assertEquals(2, summaries.size());
    for (Summary summary : summaries) {
      Assert.assertEquals(0, summary.getFileStatistics().getInaccurate());
      Assert.assertEquals(2, summary.getFileStatistics().getTotal());
      String className = summary.getSummarizerConfiguration().getClassName();
      CounterSummary counterSummary = new CounterSummary(summary);
      if (className.equals(FamilySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();
        Map<String,Long> expected = ImmutableMap.of("0000", 400l, "0001", 400l, "0002", 400l, "0003", 400l);
        Assert.assertEquals(expected, counters);
      } else if (className.equals(VisibilitySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();
        Map<String,Long> expected = ImmutableMap.of("A&B", 800l, "A&B&C", 800l);
        Assert.assertEquals(expected, counters);
      } else {
        Assert.fail("Unexpected classname " + className);
      }
    }

    // verify reading a subset of summaries works
    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 0);

    // the following test check boundry conditions for start row and end row
    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(99)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 400l, "A&B&C", 400l), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(98)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(0)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow("#").read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(100)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 400l, "A&B&C", 400l), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).endRow(rowStr(99)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 400l, "A&B&C", 400l), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).endRow(rowStr(100)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).endRow(rowStr(199)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(50))
        .endRow(rowStr(150)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 2);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(120))
        .endRow(rowStr(150)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 400l, "A&B&C", 400l), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(50))
        .endRow(rowStr(199)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow("#").endRow(rowStr(150))
        .read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 800l, "A&B&C", 800l), 1);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(199)).read();
    checkSummaries(summaries, ImmutableMap.of(), 0);
    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).startRow(rowStr(200)).read();
    checkSummaries(summaries, ImmutableMap.of(), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).endRow("#").read();
    checkSummaries(summaries, ImmutableMap.of(), 0);

    summaries = RFile.summaries().from(testFile, testFile2).withFileSystem(localFs).selectSummaries(sc -> sc.equals(sc1)).endRow(rowStr(0)).read();
    checkSummaries(summaries, ImmutableMap.of("A&B", 400l, "A&B&C", 400l), 1);
  }

  private void checkSummaries(Collection<Summary> summaries, Map<String,Long> expected, int extra) {
    Assert.assertEquals(1, summaries.size());
    for (Summary summary : summaries) {
      Assert.assertEquals(extra, summary.getFileStatistics().getInaccurate());
      Assert.assertEquals(extra, summary.getFileStatistics().getExtra());
      Assert.assertEquals(2, summary.getFileStatistics().getTotal());
      String className = summary.getSummarizerConfiguration().getClassName();
      CounterSummary counterSummary = new CounterSummary(summary);
      if (className.equals(VisibilitySummarizer.class.getName())) {
        Map<String,Long> counters = counterSummary.getCounters();

        Assert.assertEquals(expected, counters);
      } else {
        Assert.fail("Unexpected classname " + className);
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOutOfOrder() throws Exception {
    // test that exception declared in API is thrown
    Key k1 = new Key("r1", "f1", "q1");
    Value v1 = new Value("1".getBytes());

    Key k2 = new Key("r2", "f1", "q1");
    Value v2 = new Value("2".getBytes());

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.append(k2, v2);
      writer.append(k1, v1);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOutOfOrderIterable() throws Exception {
    // test that exception declared in API is thrown
    Key k1 = new Key("r1", "f1", "q1");
    Value v1 = new Value("1".getBytes());

    Key k2 = new Key("r2", "f1", "q1");
    Value v2 = new Value("2".getBytes());

    ArrayList<Entry<Key,Value>> data = new ArrayList<>();
    data.add(new AbstractMap.SimpleEntry<>(k2, v2));
    data.add(new AbstractMap.SimpleEntry<>(k1, v1));

    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.append(data);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadVis() throws Exception {
    // this test has two purposes ensure an exception is thrown and ensure the exception document in the javadoc is thrown
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startDefaultLocalityGroup();
      Key k1 = new Key("r1", "f1", "q1", "(A&(B");
      writer.append(k1, new Value("".getBytes()));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadVisIterable() throws Exception {
    // test append(iterable) method
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startDefaultLocalityGroup();
      Key k1 = new Key("r1", "f1", "q1", "(A&(B");
      Entry<Key,Value> entry = new AbstractMap.SimpleEntry<>(k1, new Value("".getBytes()));
      writer.append(Collections.singletonList(entry));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleStart() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startDefaultLocalityGroup();
      writer.startDefaultLocalityGroup();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testAppendStartDefault() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.append(new Key("r1", "f1", "q1"), new Value("1".getBytes()));
      writer.startDefaultLocalityGroup();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testStartAfter() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      Key k1 = new Key("r1", "f1", "q1");
      writer.append(k1, new Value("".getBytes()));
      writer.startNewLocalityGroup("lg1", "fam1");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalColumn() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startNewLocalityGroup("lg1", "fam1");
      Key k1 = new Key("r1", "f1", "q1");
      // should not be able to append the column family f1
      writer.append(k1, new Value("".getBytes()));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongGroup() throws Exception {
    LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    String testFile = createTmpTestFile();
    try (RFileWriter writer = RFile.newWriter().to(testFile).withFileSystem(localFs).build()) {
      writer.startNewLocalityGroup("lg1", "fam1");
      Key k1 = new Key("r1", "fam1", "q1");
      writer.append(k1, new Value("".getBytes()));
      writer.startDefaultLocalityGroup();
      // should not be able to append the column family fam1 to default locality group
      Key k2 = new Key("r1", "fam1", "q2");
      writer.append(k2, new Value("".getBytes()));
    }
  }

  private Reader getReader(LocalFileSystem localFs, String testFile) throws IOException {
    Reader reader = (Reader) FileOperations.getInstance().newReaderBuilder().forFile(testFile).inFileSystem(localFs, localFs.getConf())
        .withTableConfiguration(DefaultConfiguration.getInstance()).build();
    return reader;
  }
}

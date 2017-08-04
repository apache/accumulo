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

package org.apache.accumulo.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.OfflineScanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class SampleIT extends AccumuloClusterHarness {

  private static final Map<String,String> OPTIONS_1 = ImmutableMap.of("hasher", "murmur3_32", "modulus", "1009");
  private static final Map<String,String> OPTIONS_2 = ImmutableMap.of("hasher", "murmur3_32", "modulus", "997");

  private static final SamplerConfiguration SC1 = new SamplerConfiguration(RowSampler.class.getName()).setOptions(OPTIONS_1);
  private static final SamplerConfiguration SC2 = new SamplerConfiguration(RowSampler.class.getName()).setOptions(OPTIONS_2);

  public static class IteratorThatUsesSample extends WrappingIterator {
    private SortedKeyValueIterator<Key,Value> sampleDC;
    private boolean hasTop;

    @Override
    public boolean hasTop() {
      return hasTop && super.hasTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

      int sampleCount = 0;
      sampleDC.seek(range, columnFamilies, inclusive);

      while (sampleDC.hasTop()) {
        sampleCount++;
        sampleDC.next();
      }

      if (sampleCount < 10) {
        hasTop = true;
        super.seek(range, columnFamilies, inclusive);
      } else {
        // its too much data
        hasTop = false;
      }
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      IteratorEnvironment sampleEnv = env.cloneWithSamplingEnabled();

      sampleDC = source.deepCopy(sampleEnv);
    }
  }

  @Test
  public void testBasic() throws Exception {

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    String clone = tableName + "_clone";

    conn.tableOperations().create(tableName, new NewTableConfiguration().enableSampling(SC1));

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    TreeMap<Key,Value> expected = new TreeMap<>();
    String someRow = writeData(bw, SC1, expected);
    Assert.assertEquals(20, expected.size());

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    Scanner isoScanner = new IsolatedScanner(conn.createScanner(tableName, Authorizations.EMPTY));
    Scanner csiScanner = new ClientSideIteratorScanner(conn.createScanner(tableName, Authorizations.EMPTY));
    scanner.setSamplerConfiguration(SC1);
    csiScanner.setSamplerConfiguration(SC1);
    isoScanner.setSamplerConfiguration(SC1);
    isoScanner.setBatchSize(10);

    BatchScanner bScanner = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);
    bScanner.setSamplerConfiguration(SC1);
    bScanner.setRanges(Arrays.asList(new Range()));

    check(expected, scanner, bScanner, isoScanner, csiScanner);

    conn.tableOperations().flush(tableName, null, null, true);

    Scanner oScanner = newOfflineScanner(conn, tableName, clone, SC1);
    check(expected, scanner, bScanner, isoScanner, csiScanner, oScanner);

    // ensure non sample data can be scanned after scanning sample data
    for (ScannerBase sb : Arrays.asList(scanner, bScanner, isoScanner, csiScanner, oScanner)) {
      sb.clearSamplerConfiguration();
      Assert.assertEquals(20000, Iterables.size(sb));
      sb.setSamplerConfiguration(SC1);
    }

    Iterator<Key> it = expected.keySet().iterator();
    while (it.hasNext()) {
      Key k = it.next();
      if (k.getRow().toString().equals(someRow)) {
        it.remove();
      }
    }

    expected.put(new Key(someRow, "cf1", "cq1", 8), new Value("42".getBytes()));
    expected.put(new Key(someRow, "cf1", "cq3", 8), new Value("suprise".getBytes()));

    Mutation m = new Mutation(someRow);

    m.put("cf1", "cq1", 8, "42");
    m.putDelete("cf1", "cq2", 8);
    m.put("cf1", "cq3", 8, "suprise");

    bw.addMutation(m);
    bw.close();

    check(expected, scanner, bScanner, isoScanner, csiScanner);

    conn.tableOperations().flush(tableName, null, null, true);

    oScanner = newOfflineScanner(conn, tableName, clone, SC1);
    check(expected, scanner, bScanner, isoScanner, csiScanner, oScanner);

    scanner.setRange(new Range(someRow));
    isoScanner.setRange(new Range(someRow));
    csiScanner.setRange(new Range(someRow));
    oScanner.setRange(new Range(someRow));
    bScanner.setRanges(Arrays.asList(new Range(someRow)));

    expected.clear();

    expected.put(new Key(someRow, "cf1", "cq1", 8), new Value("42".getBytes()));
    expected.put(new Key(someRow, "cf1", "cq3", 8), new Value("suprise".getBytes()));

    check(expected, scanner, bScanner, isoScanner, csiScanner, oScanner);

    bScanner.close();
  }

  private Scanner newOfflineScanner(Connector conn, String tableName, String clone, SamplerConfiguration sc) throws Exception {
    if (conn.tableOperations().exists(clone)) {
      conn.tableOperations().delete(clone);
    }
    Map<String,String> em = Collections.emptyMap();
    Set<String> es = Collections.emptySet();
    conn.tableOperations().clone(tableName, clone, false, em, es);
    conn.tableOperations().offline(clone, true);
    Table.ID cloneID = Table.ID.of(conn.tableOperations().tableIdMap().get(clone));
    OfflineScanner oScanner = new OfflineScanner(conn.getInstance(), new Credentials(getAdminPrincipal(), getAdminToken()), cloneID, Authorizations.EMPTY);
    if (sc != null) {
      oScanner.setSamplerConfiguration(sc);
    }
    return oScanner;
  }

  private void updateExpected(SamplerConfiguration sc, TreeMap<Key,Value> expected) {
    expected.clear();

    RowSampler sampler = new RowSampler();
    sampler.init(sc);

    for (int i = 0; i < 10000; i++) {
      String row = String.format("r_%06d", i);

      Key k1 = new Key(row, "cf1", "cq1", 7);
      if (sampler.accept(k1)) {
        expected.put(k1, new Value(("" + i).getBytes()));
      }

      Key k2 = new Key(row, "cf1", "cq2", 7);
      if (sampler.accept(k2)) {
        expected.put(k2, new Value(("" + (100000000 - i)).getBytes()));
      }
    }
  }

  private String writeData(BatchWriter bw, SamplerConfiguration sc, TreeMap<Key,Value> expected) throws MutationsRejectedException {
    int count = 0;
    String someRow = null;

    RowSampler sampler = new RowSampler();
    sampler.init(sc);

    for (int i = 0; i < 10000; i++) {
      String row = String.format("r_%06d", i);
      Mutation m = new Mutation(row);

      m.put("cf1", "cq1", 7, "" + i);
      m.put("cf1", "cq2", 7, "" + (100000000 - i));

      bw.addMutation(m);

      Key k1 = new Key(row, "cf1", "cq1", 7);
      if (sampler.accept(k1)) {
        expected.put(k1, new Value(("" + i).getBytes()));
        count++;
        if (count == 5) {
          someRow = row;
        }
      }

      Key k2 = new Key(row, "cf1", "cq2", 7);
      if (sampler.accept(k2)) {
        expected.put(k2, new Value(("" + (100000000 - i)).getBytes()));
      }
    }

    bw.flush();

    return someRow;
  }

  private int countEntries(Iterable<Entry<Key,Value>> scanner) {

    int count = 0;
    Iterator<Entry<Key,Value>> iter = scanner.iterator();

    while (iter.hasNext()) {
      iter.next();
      count++;
    }

    return count;
  }

  private void setRange(Range range, List<? extends ScannerBase> scanners) {
    for (ScannerBase s : scanners) {
      if (s instanceof Scanner) {
        ((Scanner) s).setRange(range);
      } else {
        ((BatchScanner) s).setRanges(Collections.singleton(range));
      }

    }
  }

  @Test
  public void testIterator() throws Exception {
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    String clone = tableName + "_clone";

    conn.tableOperations().create(tableName, new NewTableConfiguration().enableSampling(SC1));

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    TreeMap<Key,Value> expected = new TreeMap<>();
    writeData(bw, SC1, expected);

    ArrayList<Key> keys = new ArrayList<>(expected.keySet());

    Range range1 = new Range(keys.get(6), true, keys.get(11), true);

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    Scanner isoScanner = new IsolatedScanner(conn.createScanner(tableName, Authorizations.EMPTY));
    ClientSideIteratorScanner csiScanner = new ClientSideIteratorScanner(conn.createScanner(tableName, Authorizations.EMPTY));
    BatchScanner bScanner = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);

    csiScanner.setIteratorSamplerConfiguration(SC1);

    List<? extends ScannerBase> scanners = Arrays.asList(scanner, isoScanner, bScanner, csiScanner);

    for (ScannerBase s : scanners) {
      s.addScanIterator(new IteratorSetting(100, IteratorThatUsesSample.class));
    }

    // the iterator should see less than 10 entries in sample data, and return data
    setRange(range1, scanners);
    for (ScannerBase s : scanners) {
      Assert.assertEquals(2954, countEntries(s));
    }

    Range range2 = new Range(keys.get(5), true, keys.get(18), true);
    setRange(range2, scanners);

    // the iterator should see more than 10 entries in sample data, and return no data
    for (ScannerBase s : scanners) {
      Assert.assertEquals(0, countEntries(s));
    }

    // flush an rerun same test against files
    conn.tableOperations().flush(tableName, null, null, true);

    Scanner oScanner = newOfflineScanner(conn, tableName, clone, null);
    oScanner.addScanIterator(new IteratorSetting(100, IteratorThatUsesSample.class));
    scanners = Arrays.asList(scanner, isoScanner, bScanner, csiScanner, oScanner);

    setRange(range1, scanners);
    for (ScannerBase s : scanners) {
      Assert.assertEquals(2954, countEntries(s));
    }

    setRange(range2, scanners);
    for (ScannerBase s : scanners) {
      Assert.assertEquals(0, countEntries(s));
    }

    updateSamplingConfig(conn, tableName, SC2);

    csiScanner.setIteratorSamplerConfiguration(SC2);

    oScanner = newOfflineScanner(conn, tableName, clone, null);
    oScanner.addScanIterator(new IteratorSetting(100, IteratorThatUsesSample.class));
    scanners = Arrays.asList(scanner, isoScanner, bScanner, csiScanner, oScanner);

    for (ScannerBase s : scanners) {
      try {
        countEntries(s);
        Assert.fail("Expected SampleNotPresentException, but it did not happen : " + s.getClass().getSimpleName());
      } catch (SampleNotPresentException e) {

      }
    }
  }

  private void setSamplerConfig(SamplerConfiguration sc, ScannerBase... scanners) {
    for (ScannerBase s : scanners) {
      s.setSamplerConfiguration(sc);
    }
  }

  @Test
  public void testSampleNotPresent() throws Exception {

    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    String clone = tableName + "_clone";

    conn.tableOperations().create(tableName);

    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    TreeMap<Key,Value> expected = new TreeMap<>();
    writeData(bw, SC1, expected);

    Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    Scanner isoScanner = new IsolatedScanner(conn.createScanner(tableName, Authorizations.EMPTY));
    isoScanner.setBatchSize(10);
    Scanner csiScanner = new ClientSideIteratorScanner(conn.createScanner(tableName, Authorizations.EMPTY));
    BatchScanner bScanner = conn.createBatchScanner(tableName, Authorizations.EMPTY, 2);
    bScanner.setRanges(Arrays.asList(new Range()));

    // ensure sample not present exception occurs when sampling is not configured
    assertSampleNotPresent(SC1, scanner, isoScanner, bScanner, csiScanner);

    conn.tableOperations().flush(tableName, null, null, true);

    Scanner oScanner = newOfflineScanner(conn, tableName, clone, SC1);
    assertSampleNotPresent(SC1, scanner, isoScanner, bScanner, csiScanner, oScanner);

    // configure sampling, however there exist an rfile w/o sample data... so should still see sample not present exception

    updateSamplingConfig(conn, tableName, SC1);

    // create clone with new config
    oScanner = newOfflineScanner(conn, tableName, clone, SC1);

    assertSampleNotPresent(SC1, scanner, isoScanner, bScanner, csiScanner, oScanner);

    // create rfile with sample data present
    conn.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    // should be able to scan sample now
    oScanner = newOfflineScanner(conn, tableName, clone, SC1);
    setSamplerConfig(SC1, scanner, csiScanner, isoScanner, bScanner, oScanner);
    check(expected, scanner, isoScanner, bScanner, csiScanner, oScanner);

    // change sampling config
    updateSamplingConfig(conn, tableName, SC2);

    // create clone with new config
    oScanner = newOfflineScanner(conn, tableName, clone, SC2);

    // rfile should have different sample config than table, and scan should not work
    assertSampleNotPresent(SC2, scanner, isoScanner, bScanner, csiScanner, oScanner);

    // create rfile that has same sample data as table config
    conn.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    // should be able to scan sample now
    updateExpected(SC2, expected);
    oScanner = newOfflineScanner(conn, tableName, clone, SC2);
    setSamplerConfig(SC2, scanner, csiScanner, isoScanner, bScanner, oScanner);
    check(expected, scanner, isoScanner, bScanner, csiScanner, oScanner);

    bScanner.close();
  }

  private void updateSamplingConfig(Connector conn, String tableName, SamplerConfiguration sc) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException {
    conn.tableOperations().setSamplerConfiguration(tableName, sc);
    // wait for for config change
    conn.tableOperations().offline(tableName, true);
    conn.tableOperations().online(tableName, true);
  }

  private void assertSampleNotPresent(SamplerConfiguration sc, ScannerBase... scanners) {

    for (ScannerBase scanner : scanners) {
      SamplerConfiguration csc = scanner.getSamplerConfiguration();

      scanner.setSamplerConfiguration(sc);

      try {
        for (Iterator<Entry<Key,Value>> i = scanner.iterator(); i.hasNext();) {
          Entry<Key,Value> entry = i.next();
          entry.getKey();
        }
        Assert.fail("Expected SampleNotPresentException, but it did not happen : " + scanner.getClass().getSimpleName());
      } catch (SampleNotPresentException e) {

      }

      scanner.clearSamplerConfiguration();
      for (Iterator<Entry<Key,Value>> i = scanner.iterator(); i.hasNext();) {
        Entry<Key,Value> entry = i.next();
        entry.getKey();
      }

      if (csc == null) {
        scanner.clearSamplerConfiguration();
      } else {
        scanner.setSamplerConfiguration(csc);
      }
    }
  }

  private void check(TreeMap<Key,Value> expected, ScannerBase... scanners) {
    TreeMap<Key,Value> actual = new TreeMap<>();
    for (ScannerBase s : scanners) {
      actual.clear();
      for (Entry<Key,Value> entry : s) {
        actual.put(entry.getKey(), entry.getValue());
      }
      Assert.assertEquals(String.format("Saw %d instead of %d entries using %s", actual.size(), expected.size(), s.getClass().getSimpleName()), expected,
          actual);
    }
  }
}

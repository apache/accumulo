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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.OfflineScanner;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

public class SampleIT extends AccumuloClusterHarness {

  private static final Map<String,String> OPTIONS_1 =
      Map.of("hasher", "murmur3_32", "modulus", "1009");
  private static final Map<String,String> OPTIONS_2 =
      Map.of("hasher", "murmur3_32", "modulus", "997");

  private static final SamplerConfiguration SC1 =
      new SamplerConfiguration(RowSampler.class.getName()).setOptions(OPTIONS_1);
  private static final SamplerConfiguration SC2 =
      new SamplerConfiguration(RowSampler.class.getName()).setOptions(OPTIONS_2);

  public static class IteratorThatUsesSample extends WrappingIterator {
    private SortedKeyValueIterator<Key,Value> sampleDC;
    private boolean hasTop;

    @Override
    public boolean hasTop() {
      return hasTop && super.hasTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {

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
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      IteratorEnvironment sampleEnv = env.cloneWithSamplingEnabled();

      sampleDC = source.deepCopy(sampleEnv);
    }
  }

  @Test
  public void testBasic() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      String clone = tableName + "_clone";

      client.tableOperations().create(tableName, new NewTableConfiguration().enableSampling(SC1));

      BatchWriter bw = client.createBatchWriter(tableName);

      TreeMap<Key,Value> expected = new TreeMap<>();
      String someRow = writeData(bw, SC1, expected);
      assertEquals(20, expected.size());

      Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
      Scanner isoScanner =
          new IsolatedScanner(client.createScanner(tableName, Authorizations.EMPTY));
      Scanner csiScanner =
          new ClientSideIteratorScanner(client.createScanner(tableName, Authorizations.EMPTY));
      scanner.setSamplerConfiguration(SC1);
      csiScanner.setSamplerConfiguration(SC1);
      isoScanner.setSamplerConfiguration(SC1);
      isoScanner.setBatchSize(10);

      try (BatchScanner bScanner = client.createBatchScanner(tableName)) {
        bScanner.setSamplerConfiguration(SC1);
        bScanner.setRanges(Arrays.asList(new Range()));

        check(expected, scanner, bScanner, isoScanner, csiScanner);

        client.tableOperations().flush(tableName, null, null, true);

        Scanner oScanner = newOfflineScanner(client, tableName, clone, SC1);
        check(expected, scanner, bScanner, isoScanner, csiScanner, oScanner);

        // ensure non sample data can be scanned after scanning sample data
        for (ScannerBase sb : Arrays.asList(scanner, bScanner, isoScanner, csiScanner, oScanner)) {
          sb.clearSamplerConfiguration();
          assertEquals(20000, Iterables.size(sb));
          sb.setSamplerConfiguration(SC1);
        }

        expected.keySet().removeIf(k -> k.getRow().toString().equals(someRow));

        expected.put(new Key(someRow, "cf1", "cq1", 8), new Value("42"));
        expected.put(new Key(someRow, "cf1", "cq3", 8), new Value("suprise"));

        Mutation m = new Mutation(someRow);

        m.put("cf1", "cq1", 8, "42");
        m.putDelete("cf1", "cq2", 8);
        m.put("cf1", "cq3", 8, "suprise");

        bw.addMutation(m);
        bw.close();

        check(expected, scanner, bScanner, isoScanner, csiScanner);

        client.tableOperations().flush(tableName, null, null, true);

        oScanner = newOfflineScanner(client, tableName, clone, SC1);
        check(expected, scanner, bScanner, isoScanner, csiScanner, oScanner);

        scanner.setRange(new Range(someRow));
        isoScanner.setRange(new Range(someRow));
        csiScanner.setRange(new Range(someRow));
        oScanner.setRange(new Range(someRow));
        bScanner.setRanges(Arrays.asList(new Range(someRow)));

        expected.clear();

        expected.put(new Key(someRow, "cf1", "cq1", 8), new Value("42"));
        expected.put(new Key(someRow, "cf1", "cq3", 8), new Value("suprise"));

        check(expected, scanner, bScanner, isoScanner, csiScanner, oScanner);
      }
    }
  }

  private Scanner newOfflineScanner(AccumuloClient client, String tableName, String clone,
      SamplerConfiguration sc) throws Exception {
    if (client.tableOperations().exists(clone)) {
      client.tableOperations().delete(clone);
    }
    Map<String,String> em = Collections.emptyMap();
    Set<String> es = Collections.emptySet();
    client.tableOperations().clone(tableName, clone, false, em, es);
    client.tableOperations().offline(clone, true);
    TableId cloneID = TableId.of(client.tableOperations().tableIdMap().get(clone));
    OfflineScanner oScanner =
        new OfflineScanner((ClientContext) client, cloneID, Authorizations.EMPTY);
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
        expected.put(k1, new Value("" + i));
      }

      Key k2 = new Key(row, "cf1", "cq2", 7);
      if (sampler.accept(k2)) {
        expected.put(k2, new Value("" + (100000000 - i)));
      }
    }
  }

  private String writeData(BatchWriter bw, SamplerConfiguration sc, TreeMap<Key,Value> expected)
      throws MutationsRejectedException {
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
        expected.put(k1, new Value("" + i));
        count++;
        if (count == 5) {
          someRow = row;
        }
      }

      Key k2 = new Key(row, "cf1", "cq2", 7);
      if (sampler.accept(k2)) {
        expected.put(k2, new Value("" + (100000000 - i)));
      }
    }

    bw.flush();

    return someRow;
  }

  private int countEntries(Iterable<Entry<Key,Value>> scanner) {
    return Iterables.size(scanner);
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
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      String clone = tableName + "_clone";

      client.tableOperations().create(tableName, new NewTableConfiguration().enableSampling(SC1));

      TreeMap<Key,Value> expected = new TreeMap<>();
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        writeData(bw, SC1, expected);
      }

      ArrayList<Key> keys = new ArrayList<>(expected.keySet());

      Range range1 = new Range(keys.get(6), true, keys.get(11), true);

      Scanner scanner = null;
      Scanner isoScanner = null;
      ClientSideIteratorScanner csiScanner = null;
      BatchScanner bScanner = null;
      Scanner oScanner = null;
      try {
        scanner = client.createScanner(tableName, Authorizations.EMPTY);
        isoScanner = new IsolatedScanner(client.createScanner(tableName, Authorizations.EMPTY));
        csiScanner =
            new ClientSideIteratorScanner(client.createScanner(tableName, Authorizations.EMPTY));
        bScanner = client.createBatchScanner(tableName, Authorizations.EMPTY, 2);

        csiScanner.setIteratorSamplerConfiguration(SC1);

        List<? extends ScannerBase> scanners =
            Arrays.asList(scanner, isoScanner, bScanner, csiScanner);

        for (ScannerBase s : scanners) {
          s.addScanIterator(new IteratorSetting(100, IteratorThatUsesSample.class));
        }

        // the iterator should see less than 10 entries in sample data, and return data
        setRange(range1, scanners);
        for (ScannerBase s : scanners) {
          assertEquals(2954, countEntries(s));
        }

        Range range2 = new Range(keys.get(5), true, keys.get(18), true);
        setRange(range2, scanners);

        // the iterator should see more than 10 entries in sample data, and return no data
        for (ScannerBase s : scanners) {
          assertEquals(0, countEntries(s));
        }

        // flush an rerun same test against files
        client.tableOperations().flush(tableName, null, null, true);

        oScanner = newOfflineScanner(client, tableName, clone, null);
        oScanner.addScanIterator(new IteratorSetting(100, IteratorThatUsesSample.class));
        scanners = Arrays.asList(scanner, isoScanner, bScanner, csiScanner, oScanner);

        setRange(range1, scanners);
        for (ScannerBase s : scanners) {
          assertEquals(2954, countEntries(s));
        }

        setRange(range2, scanners);
        for (ScannerBase s : scanners) {
          assertEquals(0, countEntries(s));
        }

        updateSamplingConfig(client, tableName, SC2);

        csiScanner.setIteratorSamplerConfiguration(SC2);

        oScanner = newOfflineScanner(client, tableName, clone, null);
        oScanner.addScanIterator(new IteratorSetting(100, IteratorThatUsesSample.class));
        scanners = Arrays.asList(scanner, isoScanner, bScanner, csiScanner, oScanner);

        for (ScannerBase s : scanners) {
          assertThrows(SampleNotPresentException.class, () -> countEntries(s),
              "Expected SampleNotPresentException, but it did not happen : "
                  + s.getClass().getSimpleName());
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
        if (bScanner != null) {
          bScanner.close();
        }
        if (isoScanner != null) {
          isoScanner.close();
        }
        if (csiScanner != null) {
          csiScanner.close();
        }
        if (oScanner != null) {
          oScanner.close();
        }
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

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      String clone = tableName + "_clone";

      client.tableOperations().create(tableName);

      TreeMap<Key,Value> expected = new TreeMap<>();
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        writeData(bw, SC1, expected);
      }

      Scanner scanner = client.createScanner(tableName);
      Scanner isoScanner = new IsolatedScanner(client.createScanner(tableName));
      isoScanner.setBatchSize(10);
      Scanner csiScanner = new ClientSideIteratorScanner(client.createScanner(tableName));
      try (BatchScanner bScanner = client.createBatchScanner(tableName)) {
        bScanner.setRanges(Arrays.asList(new Range()));

        // ensure sample not present exception occurs when sampling is not configured
        assertSampleNotPresent(SC1, scanner, isoScanner, bScanner, csiScanner);

        client.tableOperations().flush(tableName, null, null, true);

        Scanner oScanner = newOfflineScanner(client, tableName, clone, SC1);
        assertSampleNotPresent(SC1, scanner, isoScanner, bScanner, csiScanner, oScanner);

        // configure sampling, however there exist an rfile w/o sample data... so should still see
        // sample not present exception

        updateSamplingConfig(client, tableName, SC1);

        // create clone with new config
        oScanner = newOfflineScanner(client, tableName, clone, SC1);

        assertSampleNotPresent(SC1, scanner, isoScanner, bScanner, csiScanner, oScanner);

        // create rfile with sample data present
        client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

        // should be able to scan sample now
        oScanner = newOfflineScanner(client, tableName, clone, SC1);
        setSamplerConfig(SC1, scanner, csiScanner, isoScanner, bScanner, oScanner);
        check(expected, scanner, isoScanner, bScanner, csiScanner, oScanner);

        // change sampling config
        updateSamplingConfig(client, tableName, SC2);

        // create clone with new config
        oScanner = newOfflineScanner(client, tableName, clone, SC2);

        // rfile should have different sample config than table, and scan should not work
        assertSampleNotPresent(SC2, scanner, isoScanner, bScanner, csiScanner, oScanner);

        // create rfile that has same sample data as table config
        client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

        // should be able to scan sample now
        updateExpected(SC2, expected);
        oScanner = newOfflineScanner(client, tableName, clone, SC2);
        setSamplerConfig(SC2, scanner, csiScanner, isoScanner, bScanner, oScanner);
        check(expected, scanner, isoScanner, bScanner, csiScanner, oScanner);
      }
    }
  }

  private void updateSamplingConfig(AccumuloClient client, String tableName,
      SamplerConfiguration sc)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    client.tableOperations().setSamplerConfiguration(tableName, sc);
    // wait for for config change
    client.tableOperations().offline(tableName, true);
    client.tableOperations().online(tableName, true);
  }

  private void assertSampleNotPresent(SamplerConfiguration sc, ScannerBase... scanners) {

    for (ScannerBase scanner : scanners) {
      SamplerConfiguration csc = scanner.getSamplerConfiguration();

      scanner.setSamplerConfiguration(sc);

      final String message = "Expected SampleNotPresentException, but it did not happen : "
          + scanner.getClass().getSimpleName();
      assertThrows(SampleNotPresentException.class, () -> scanner.iterator().next(), message);

      scanner.clearSamplerConfiguration();
      scanner.forEach((k, v) -> {});

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
      assertEquals(expected, actual, String.format("Saw %d instead of %d entries using %s",
          actual.size(), expected.size(), s.getClass().getSimpleName()));
    }
  }
}

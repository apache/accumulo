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
package org.apache.accumulo.tserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.sample.impl.SamplerFactory;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.tserver.InMemoryMap.MemoryIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class InMemoryMapTest extends WithTestNames {

  private static class SampleIE implements IteratorEnvironment {

    private final SamplerConfiguration sampleConfig;

    public SampleIE() {
      this.sampleConfig = null;
    }

    public SampleIE(SamplerConfigurationImpl sampleConfig) {
      this.sampleConfig = sampleConfig.toSamplerConfiguration();
    }

    @Override
    public boolean isSamplingEnabled() {
      return sampleConfig != null;
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
      return sampleConfig;
    }
  }

  public static ServerContext getServerContext() {
    Configuration hadoopConf = new Configuration();
    ServerContext context = EasyMock.createMock(ServerContext.class);
    TableConfiguration tConf = EasyMock.createMock(TableConfiguration.class);
    EasyMock.expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance())
        .anyTimes();
    EasyMock.expect(context.getTableConfiguration(EasyMock.anyObject())).andReturn(tConf)
        .anyTimes();
    EasyMock.expect(tConf.getCryptoService()).andReturn(NoCryptoServiceFactory.NONE).anyTimes();
    EasyMock.expect(context.getHadoopConf()).andReturn(hadoopConf).anyTimes();
    EasyMock.replay(context, tConf);
    return context;
  }

  @TempDir
  private static File tempDir;

  public void mutate(InMemoryMap imm, String row, String column, long ts, String value) {
    Mutation m = new Mutation(new Text(row));
    String[] sa = column.split(":");
    m.put(new Text(sa[0]), new Text(sa[1]), ts, new Value(value));

    imm.mutate(Collections.singletonList(m), 1);
  }

  static Key newKey(String row, String column, long ts) {
    String[] sa = column.split(":");
    return new Key(new Text(row), new Text(sa[0]), new Text(sa[1]), ts);
  }

  static void testAndCallNext(SortedKeyValueIterator<Key,Value> dc, String row, String column,
      int ts, String val) throws IOException {
    assertTrue(dc.hasTop());
    assertEquals(newKey(row, column, ts), dc.getTopKey());
    assertEquals(new Value(val), dc.getTopValue());
    dc.next();

  }

  static void assertEqualsNoNext(SortedKeyValueIterator<Key,Value> dc, String row, String column,
      int ts, String val) {
    assertTrue(dc.hasTop());
    assertEquals(newKey(row, column, ts), dc.getTopKey());
    assertEquals(new Value(val), dc.getTopValue());

  }

  static Set<ByteSequence> newCFSet(String... cfs) {
    HashSet<ByteSequence> cfSet = new HashSet<>();
    for (String cf : cfs) {
      cfSet.add(new ArrayByteSequence(cf));
    }
    return cfSet;
  }

  static Set<Text> toTextSet(String... cfs) {
    HashSet<Text> cfSet = new HashSet<>();
    for (String cf : cfs) {
      cfSet.add(new Text(cf));
    }
    return cfSet;
  }

  static ConfigurationCopy newConfig(String memDumpDir) {
    ConfigurationCopy config = new ConfigurationCopy(DefaultConfiguration.getInstance());
    config.set(Property.TSERV_NATIVEMAP_ENABLED, "" + false);
    config.set(Property.TSERV_MEMDUMP_DIR, memDumpDir);
    return config;
  }

  static InMemoryMap newInMemoryMap(boolean useNative, String memDumpDir) {
    ConfigurationCopy config = new ConfigurationCopy(DefaultConfiguration.getInstance());
    config.set(Property.TSERV_NATIVEMAP_ENABLED, "" + useNative);
    config.set(Property.TSERV_MEMDUMP_DIR, memDumpDir);
    return new InMemoryMap(config, getServerContext(), TableId.of("--TEST--"));
  }

  @Test
  public void test2() throws Exception {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    MemoryIterator ski1 = imm.skvIterator(null);
    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    MemoryIterator ski2 = imm.skvIterator(null);

    ski1.seek(new Range(), Set.of(), false);
    assertFalse(ski1.hasTop());

    ski2.seek(new Range(), Set.of(), false);
    assertTrue(ski2.hasTop());
    testAndCallNext(ski2, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski2.hasTop());

  }

  @Test
  public void test3() throws Exception {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq1", 3, "bar2");
    MemoryIterator ski1 = imm.skvIterator(null);
    mutate(imm, "r1", "foo:cq1", 3, "bar3");

    mutate(imm, "r3", "foo:cq1", 3, "bar9");
    mutate(imm, "r3", "foo:cq1", 3, "bara");

    MemoryIterator ski2 = imm.skvIterator(null);

    ski1.seek(new Range(new Text("r1")), Set.of(), false);
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar2");
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski2.seek(new Range(new Text("r3")), Set.of(), false);
    testAndCallNext(ski2, "r3", "foo:cq1", 3, "bara");
    testAndCallNext(ski2, "r3", "foo:cq1", 3, "bar9");
    assertFalse(ski1.hasTop());

  }

  @Test
  public void test4() throws Exception {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq1", 3, "bar2");
    MemoryIterator ski1 = imm.skvIterator(null);
    mutate(imm, "r1", "foo:cq1", 3, "bar3");

    imm.delete(0);

    ski1.seek(new Range(new Text("r1")), Set.of(), false);
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar2");
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski1.seek(new Range(new Text("r1")), Set.of(), false);
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar2");
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski1.seek(new Range(new Text("r2")), Set.of(), false);
    assertFalse(ski1.hasTop());

    ski1.seek(new Range(newKey("r1", "foo:cq1", 3), null), Set.of(), false);
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar2");
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski1.close();
  }

  @Test
  public void testDecodeValueModification() throws Exception {
    // This test case is the fix for ACCUMULO-4483
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    mutate(imm, "r1", "foo:cq1", 3, "");
    MemoryIterator ski1 = imm.skvIterator(null);

    imm.delete(0);

    ski1.seek(new Range(new Text("r1")), Set.of(), false);
    assertEqualsNoNext(ski1, "r1", "foo:cq1", 3, "");
    ski1.seek(new Range(new Text("r1")), Set.of(), false);
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "");
    assertFalse(ski1.hasTop());

    ski1.close();
  }

  @Test
  public void test5() throws Exception {
    String[] newFolders = uniqueDirPaths(2);
    InMemoryMap imm = newInMemoryMap(false, newFolders[0]);

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq1", 3, "bar2");
    mutate(imm, "r1", "foo:cq1", 3, "bar3");

    MemoryIterator ski1 = imm.skvIterator(null);
    ski1.seek(new Range(new Text("r1")), Set.of(), false);
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar3");

    imm.delete(0);

    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar2");
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski1.close();

    imm = newInMemoryMap(false, newFolders[1]);

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq2", 3, "bar2");
    mutate(imm, "r1", "foo:cq3", 3, "bar3");

    ski1 = imm.skvIterator(null);
    ski1.seek(new Range(new Text("r1")), Set.of(), false);
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar1");

    imm.delete(0);

    testAndCallNext(ski1, "r1", "foo:cq2", 3, "bar2");
    testAndCallNext(ski1, "r1", "foo:cq3", 3, "bar3");
    assertFalse(ski1.hasTop());

    ski1.close();
  }

  @Test
  public void test6() throws Exception {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq2", 3, "bar2");
    mutate(imm, "r1", "foo:cq3", 3, "bar3");
    mutate(imm, "r1", "foo:cq4", 3, "bar4");

    MemoryIterator ski1 = imm.skvIterator(null);

    mutate(imm, "r1", "foo:cq5", 3, "bar5");

    SortedKeyValueIterator<Key,Value> dc = ski1.deepCopy(new SampleIE());

    ski1.seek(new Range(newKey("r1", "foo:cq1", 3), null), Set.of(), false);
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar1");

    dc.seek(new Range(newKey("r1", "foo:cq2", 3), null), Set.of(), false);
    testAndCallNext(dc, "r1", "foo:cq2", 3, "bar2");

    imm.delete(0);

    testAndCallNext(ski1, "r1", "foo:cq2", 3, "bar2");
    testAndCallNext(dc, "r1", "foo:cq3", 3, "bar3");
    testAndCallNext(ski1, "r1", "foo:cq3", 3, "bar3");
    testAndCallNext(dc, "r1", "foo:cq4", 3, "bar4");
    testAndCallNext(ski1, "r1", "foo:cq4", 3, "bar4");
    assertFalse(ski1.hasTop());
    assertFalse(dc.hasTop());

    ski1.seek(new Range(newKey("r1", "foo:cq3", 3), null), Set.of(), false);

    dc.seek(new Range(newKey("r1", "foo:cq4", 3), null), Set.of(), false);
    testAndCallNext(dc, "r1", "foo:cq4", 3, "bar4");
    assertFalse(dc.hasTop());

    testAndCallNext(ski1, "r1", "foo:cq3", 3, "bar3");
    testAndCallNext(ski1, "r1", "foo:cq4", 3, "bar4");
    assertFalse(ski1.hasTop());
    assertFalse(dc.hasTop());

    ski1.close();
  }

  private void deepCopyAndDelete(int interleaving, boolean interrupt) throws Exception {
    // interleaving == 0 intentionally omitted, this runs the test w/o deleting in mem map

    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq2", 3, "bar2");

    MemoryIterator ski1 = imm.skvIterator(null);

    AtomicBoolean iflag = new AtomicBoolean(false);
    ski1.setInterruptFlag(iflag);

    if (interleaving == 1) {
      imm.delete(0);
      if (interrupt) {
        iflag.set(true);
      }
    }

    SortedKeyValueIterator<Key,Value> dc = ski1.deepCopy(new SampleIE());

    if (interleaving == 2) {
      imm.delete(0);
      if (interrupt) {
        iflag.set(true);
      }
    }

    dc.seek(new Range(), Set.of(), false);
    ski1.seek(new Range(), Set.of(), false);

    if (interleaving == 3) {
      imm.delete(0);
      if (interrupt) {
        iflag.set(true);
      }
    }

    testAndCallNext(dc, "r1", "foo:cq1", 3, "bar1");
    testAndCallNext(ski1, "r1", "foo:cq1", 3, "bar1");
    dc.seek(new Range(), Set.of(), false);

    if (interleaving == 4) {
      imm.delete(0);
      if (interrupt) {
        iflag.set(true);
      }
    }

    testAndCallNext(ski1, "r1", "foo:cq2", 3, "bar2");
    testAndCallNext(dc, "r1", "foo:cq1", 3, "bar1");
    testAndCallNext(dc, "r1", "foo:cq2", 3, "bar2");
    assertFalse(dc.hasTop());
    assertFalse(ski1.hasTop());

    if (interrupt) {
      dc.seek(new Range(), Set.of(), false);
    }
  }

  @Test
  public void testDeepCopyAndDelete() throws Exception {
    for (int i = 0; i <= 4; i++) {
      deepCopyAndDelete(i, false);
    }

    for (int i = 1; i <= 4; i++) {
      final int finalI = i;
      assertThrows(IterationInterruptedException.class, () -> deepCopyAndDelete(finalI, true),
          "i = " + finalI);
    }
  }

  @Test
  public void testBug1() throws Exception {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    for (int i = 0; i < 20; i++) {
      mutate(imm, "r1", "foo:cq" + i, 3, "bar" + i);
    }

    for (int i = 0; i < 20; i++) {
      mutate(imm, "r2", "foo:cq" + i, 3, "bar" + i);
    }

    MemoryIterator ski1 = imm.skvIterator(null);
    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(ski1);

    imm.delete(0);

    ArrayList<ByteSequence> columns = new ArrayList<>();
    columns.add(new ArrayByteSequence("bar"));

    // this seek resulted in an infinite loop before a bug was fixed
    cfsi.seek(new Range("r1"), columns, true);

    assertFalse(cfsi.hasTop());

    ski1.close();
  }

  @Test
  public void testSeekBackWards() throws Exception {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq2", 3, "bar2");
    mutate(imm, "r1", "foo:cq3", 3, "bar3");
    mutate(imm, "r1", "foo:cq4", 3, "bar4");

    MemoryIterator skvi1 = imm.skvIterator(null);

    skvi1.seek(new Range(newKey("r1", "foo:cq3", 3), null), Set.of(), false);
    testAndCallNext(skvi1, "r1", "foo:cq3", 3, "bar3");

    skvi1.seek(new Range(newKey("r1", "foo:cq1", 3), null), Set.of(), false);
    testAndCallNext(skvi1, "r1", "foo:cq1", 3, "bar1");

  }

  @Test
  public void testDuplicateKey() throws Exception {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("foo"), new Text("cq"), 3, new Value("v1"));
    m.put(new Text("foo"), new Text("cq"), 3, new Value("v2"));
    imm.mutate(Collections.singletonList(m), 2);

    MemoryIterator skvi1 = imm.skvIterator(null);
    skvi1.seek(new Range(), Set.of(), false);
    testAndCallNext(skvi1, "r1", "foo:cq", 3, "v2");
    testAndCallNext(skvi1, "r1", "foo:cq", 3, "v1");
  }

  @Test
  public void testLocalityGroups() throws Exception {
    ConfigurationCopy config = newConfig(uniqueDirPaths(1)[0]);
    config.set(Property.TABLE_LOCALITY_GROUP_PREFIX + "lg1",
        LocalityGroupUtil.encodeColumnFamilies(toTextSet("cf1", "cf2")));
    config.set(Property.TABLE_LOCALITY_GROUP_PREFIX + "lg2",
        LocalityGroupUtil.encodeColumnFamilies(toTextSet("cf3", "cf4")));
    config.set(Property.TABLE_LOCALITY_GROUPS.getKey(), "lg1,lg2");

    InMemoryMap imm = new InMemoryMap(config, getServerContext(), TableId.of("--TEST--"));

    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "x", 2, "1");
    m1.put("cf1", "y", 2, "2");
    m1.put("cf3", "z", 2, "3");
    m1.put("foo", "b", 2, "9");

    Mutation m2 = new Mutation("r2");
    m2.put("cf2", "x", 3, "5");

    Mutation m3 = new Mutation("r3");
    m3.put("foo", "b", 4, "6");

    Mutation m4 = new Mutation("r4");
    m4.put("foo", "b", 5, "7");
    m4.put("cf4", "z", 5, "8");

    Mutation m5 = new Mutation("r5");
    m5.put("cf3", "z", 6, "A");
    m5.put("cf4", "z", 6, "B");

    imm.mutate(Arrays.asList(m1, m2, m3, m4, m5), 10);

    MemoryIterator iter1 = imm.skvIterator(null);

    seekLocalityGroups(iter1);
    SortedKeyValueIterator<Key,Value> dc1 = iter1.deepCopy(new SampleIE());
    seekLocalityGroups(dc1);

    assertEquals(10, imm.getNumEntries());
    assertTrue(imm.estimatedSizeInBytes() > 0);

    imm.delete(0);

    seekLocalityGroups(iter1);
    seekLocalityGroups(dc1);
    // tests ACCUMULO-1628
    seekLocalityGroups(iter1.deepCopy(null));
  }

  @Test
  public void testSample() throws Exception {

    SamplerConfigurationImpl sampleConfig = new SamplerConfigurationImpl(RowSampler.class.getName(),
        Map.of("hasher", "murmur3_32", "modulus", "7"));
    Sampler sampler = SamplerFactory.newSampler(sampleConfig, DefaultConfiguration.getInstance());
    String[] newFolders = uniqueDirPaths(2);
    ConfigurationCopy config1 = newConfig(newFolders[0]);
    for (Entry<String,String> entry : sampleConfig.toTablePropertiesMap().entrySet()) {
      config1.set(entry.getKey(), entry.getValue());
    }

    ConfigurationCopy config2 = newConfig(newFolders[1]);
    config2.set(Property.TABLE_LOCALITY_GROUP_PREFIX + "lg1",
        LocalityGroupUtil.encodeColumnFamilies(toTextSet("cf2")));
    config2.set(Property.TABLE_LOCALITY_GROUPS.getKey(), "lg1");
    for (Entry<String,String> entry : sampleConfig.toTablePropertiesMap().entrySet()) {
      config2.set(entry.getKey(), entry.getValue());
    }

    for (ConfigurationCopy config : Arrays.asList(config1, config2)) {
      InMemoryMap imm = new InMemoryMap(config, getServerContext(), TableId.of("--TEST--"));

      TreeMap<Key,Value> expectedSample = new TreeMap<>();
      TreeMap<Key,Value> expectedAll = new TreeMap<>();
      TreeMap<Key,Value> expectedNone = new TreeMap<>();

      MemoryIterator iter0 = imm.skvIterator(sampleConfig);

      for (int r = 0; r < 100; r++) {
        String row = String.format("r%06d", r);
        mutate(imm, row, "cf1:cq1", 5, "v" + (2 * r), sampler, expectedSample, expectedAll);
        mutate(imm, row, "cf2:cq2", 5, "v" + ((2 * r) + 1), sampler, expectedSample, expectedAll);
      }

      assertTrue(!expectedSample.isEmpty());

      MemoryIterator iter1 = imm.skvIterator(sampleConfig);
      MemoryIterator iter2 = imm.skvIterator(null);
      SortedKeyValueIterator<Key,Value> iter0dc1 = iter0.deepCopy(new SampleIE());
      SortedKeyValueIterator<Key,Value> iter0dc2 = iter0.deepCopy(new SampleIE(sampleConfig));
      SortedKeyValueIterator<Key,Value> iter1dc1 = iter1.deepCopy(new SampleIE());
      SortedKeyValueIterator<Key,Value> iter1dc2 = iter1.deepCopy(new SampleIE(sampleConfig));
      SortedKeyValueIterator<Key,Value> iter2dc1 = iter2.deepCopy(new SampleIE());
      SortedKeyValueIterator<Key,Value> iter2dc2 = iter2.deepCopy(new SampleIE(sampleConfig));

      assertEquals(expectedNone, readAll(iter0));
      assertEquals(expectedNone, readAll(iter0dc1));
      assertEquals(expectedNone, readAll(iter0dc2));
      assertEquals(expectedSample, readAll(iter1));
      assertEquals(expectedAll, readAll(iter2));
      assertEquals(expectedAll, readAll(iter1dc1));
      assertEquals(expectedAll, readAll(iter2dc1));
      assertEquals(expectedSample, readAll(iter1dc2));
      assertEquals(expectedSample, readAll(iter2dc2));

      imm.delete(0);

      assertEquals(expectedNone, readAll(iter0));
      assertEquals(expectedNone, readAll(iter0dc1));
      assertEquals(expectedNone, readAll(iter0dc2));
      assertEquals(expectedSample, readAll(iter1));
      assertEquals(expectedAll, readAll(iter2));
      assertEquals(expectedAll, readAll(iter1dc1));
      assertEquals(expectedAll, readAll(iter2dc1));
      assertEquals(expectedSample, readAll(iter1dc2));
      assertEquals(expectedSample, readAll(iter2dc2));

      SortedKeyValueIterator<Key,Value> iter0dc3 = iter0.deepCopy(new SampleIE());
      SortedKeyValueIterator<Key,Value> iter0dc4 = iter0.deepCopy(new SampleIE(sampleConfig));
      SortedKeyValueIterator<Key,Value> iter1dc3 = iter1.deepCopy(new SampleIE());
      SortedKeyValueIterator<Key,Value> iter1dc4 = iter1.deepCopy(new SampleIE(sampleConfig));
      SortedKeyValueIterator<Key,Value> iter2dc3 = iter2.deepCopy(new SampleIE());
      SortedKeyValueIterator<Key,Value> iter2dc4 = iter2.deepCopy(new SampleIE(sampleConfig));

      assertEquals(expectedNone, readAll(iter0dc3));
      assertEquals(expectedNone, readAll(iter0dc4));
      assertEquals(expectedAll, readAll(iter1dc3));
      assertEquals(expectedAll, readAll(iter2dc3));
      assertEquals(expectedSample, readAll(iter1dc4));
      assertEquals(expectedSample, readAll(iter2dc4));

      iter1.close();
      iter2.close();
    }
  }

  @Test
  public void testInterruptingSample() throws Exception {
    runInterruptSampleTest(false, false, false);
    runInterruptSampleTest(false, true, false);
    runInterruptSampleTest(true, false, false);
    runInterruptSampleTest(true, true, false);
    runInterruptSampleTest(true, true, true);
  }

  private void runInterruptSampleTest(boolean deepCopy, boolean delete, boolean dcAfterDelete)
      throws Exception {
    SamplerConfigurationImpl sampleConfig1 = new SamplerConfigurationImpl(
        RowSampler.class.getName(), Map.of("hasher", "murmur3_32", "modulus", "2"));
    Sampler sampler = SamplerFactory.newSampler(sampleConfig1, DefaultConfiguration.getInstance());

    ConfigurationCopy config1 = newConfig(uniqueDirPaths(1)[0]);
    for (Entry<String,String> entry : sampleConfig1.toTablePropertiesMap().entrySet()) {
      config1.set(entry.getKey(), entry.getValue());
    }

    InMemoryMap imm = new InMemoryMap(config1, getServerContext(), TableId.of("--TEST--"));

    TreeMap<Key,Value> expectedSample = new TreeMap<>();
    TreeMap<Key,Value> expectedAll = new TreeMap<>();

    for (int r = 0; r < 1000; r++) {
      String row = String.format("r%06d", r);
      mutate(imm, row, "cf1:cq1", 5, "v" + (2 * r), sampler, expectedSample, expectedAll);
      mutate(imm, row, "cf2:cq2", 5, "v" + ((2 * r) + 1), sampler, expectedSample, expectedAll);
    }

    assertTrue(!expectedSample.isEmpty());

    MemoryIterator miter = imm.skvIterator(sampleConfig1);
    AtomicBoolean iFlag = new AtomicBoolean(false);
    miter.setInterruptFlag(iFlag);
    SortedKeyValueIterator<Key,Value> iter = miter;

    if (delete && !dcAfterDelete) {
      imm.delete(0);
    }

    if (deepCopy) {
      iter = iter.deepCopy(new SampleIE(sampleConfig1));
    }

    if (delete && dcAfterDelete) {
      imm.delete(0);
    }

    assertEquals(expectedSample, readAll(iter));
    iFlag.set(true);
    final var finalIter = iter;
    assertThrows(IterationInterruptedException.class, () -> readAll(finalIter));

    miter.close();
  }

  private void mutate(InMemoryMap imm, String row, String cols, int ts, String val, Sampler sampler,
      TreeMap<Key,Value> expectedSample, TreeMap<Key,Value> expectedAll) {
    mutate(imm, row, cols, ts, val);
    Key k1 = newKey(row, cols, ts);
    if (sampler.accept(k1)) {
      expectedSample.put(k1, new Value(val));
    }
    expectedAll.put(k1, new Value(val));
  }

  @Test
  public void testDifferentSampleConfig() {
    SamplerConfigurationImpl sampleConfig = new SamplerConfigurationImpl(RowSampler.class.getName(),
        Map.of("hasher", "murmur3_32", "modulus", "7"));

    ConfigurationCopy config1 = newConfig(uniqueDirPaths(1)[0]);
    for (Entry<String,String> entry : sampleConfig.toTablePropertiesMap().entrySet()) {
      config1.set(entry.getKey(), entry.getValue());
    }

    InMemoryMap imm = new InMemoryMap(config1, getServerContext(), TableId.of("--TEST--"));

    mutate(imm, "r", "cf:cq", 5, "b");

    SamplerConfigurationImpl sampleConfig2 = new SamplerConfigurationImpl(
        RowSampler.class.getName(), Map.of("hasher", "murmur3_32", "modulus", "9"));
    MemoryIterator iter = imm.skvIterator(sampleConfig2);
    assertThrows(SampleNotPresentException.class, () -> iter.seek(new Range(), Set.of(), false));
  }

  @Test
  public void testNoSampleConfig() {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    mutate(imm, "r", "cf:cq", 5, "b");

    SamplerConfigurationImpl sampleConfig2 = new SamplerConfigurationImpl(
        RowSampler.class.getName(), Map.of("hasher", "murmur3_32", "modulus", "9"));
    MemoryIterator iter = imm.skvIterator(sampleConfig2);
    assertThrows(SampleNotPresentException.class, () -> iter.seek(new Range(), Set.of(), false));
  }

  @Test
  public void testEmptyNoSampleConfig() throws Exception {
    InMemoryMap imm = newInMemoryMap(false, uniqueDirPaths(1)[0]);

    SamplerConfigurationImpl sampleConfig2 = new SamplerConfigurationImpl(
        RowSampler.class.getName(), Map.of("hasher", "murmur3_32", "modulus", "9"));

    // when in mem map is empty should be able to get sample iterator with any sample config
    MemoryIterator iter = imm.skvIterator(sampleConfig2);
    iter.seek(new Range(), Set.of(), false);
    assertFalse(iter.hasTop());
  }

  @Test
  public void testDeferredSamplerCreation() throws Exception {
    SamplerConfigurationImpl sampleConfig1 = new SamplerConfigurationImpl(
        RowSampler.class.getName(), Map.of("hasher", "murmur3_32", "modulus", "9"));

    ConfigurationCopy config1 = newConfig(uniqueDirPaths(1)[0]);
    for (Entry<String,String> entry : sampleConfig1.toTablePropertiesMap().entrySet()) {
      config1.set(entry.getKey(), entry.getValue());
    }

    InMemoryMap imm = new InMemoryMap(config1, getServerContext(), TableId.of("--TEST--"));

    // change sampler config after creating in mem map.
    SamplerConfigurationImpl sampleConfig2 = new SamplerConfigurationImpl(
        RowSampler.class.getName(), Map.of("hasher", "murmur3_32", "modulus", "7"));
    for (Entry<String,String> entry : sampleConfig2.toTablePropertiesMap().entrySet()) {
      config1.set(entry.getKey(), entry.getValue());
    }

    TreeMap<Key,Value> expectedSample = new TreeMap<>();
    TreeMap<Key,Value> expectedAll = new TreeMap<>();
    Sampler sampler = SamplerFactory.newSampler(sampleConfig2, config1);

    for (int i = 0; i < 100; i++) {
      mutate(imm, "r" + i, "cf:cq", 5, "v" + i, sampler, expectedSample, expectedAll);
    }

    MemoryIterator iter = imm.skvIterator(sampleConfig2);
    iter.seek(new Range(), Set.of(), false);
    assertEquals(expectedSample, readAll(iter));

    SortedKeyValueIterator<Key,Value> dc = iter.deepCopy(new SampleIE(sampleConfig2));
    dc.seek(new Range(), Set.of(), false);
    assertEquals(expectedSample, readAll(dc));

    iter = imm.skvIterator(null);
    iter.seek(new Range(), Set.of(), false);
    assertEquals(expectedAll, readAll(iter));

    final MemoryIterator finalIter = imm.skvIterator(sampleConfig1);
    assertThrows(SampleNotPresentException.class,
        () -> finalIter.seek(new Range(), Set.of(), false));
  }

  private String[] uniqueDirPaths(int numOfDirs) {
    String[] newDirs = new String[numOfDirs];
    for (int i = 0; i < newDirs.length; i++) {
      File newDir = new File(tempDir, testName() + i);
      assertTrue(newDir.isDirectory() || newDir.mkdir(), "Failed to create directory: " + newDir);
      newDirs[i] = newDir.getAbsolutePath();
    }
    return newDirs;
  }

  private TreeMap<Key,Value> readAll(SortedKeyValueIterator<Key,Value> iter) throws IOException {
    iter.seek(new Range(), Set.of(), false);

    TreeMap<Key,Value> actual = new TreeMap<>();
    while (iter.hasTop()) {
      actual.put(iter.getTopKey(), iter.getTopValue());
      iter.next();
    }
    return actual;
  }

  private void seekLocalityGroups(SortedKeyValueIterator<Key,Value> iter1) throws IOException {
    iter1.seek(new Range(), newCFSet("cf1"), true);
    testAndCallNext(iter1, "r1", "cf1:x", 2, "1");
    testAndCallNext(iter1, "r1", "cf1:y", 2, "2");
    testAndCallNext(iter1, "r2", "cf2:x", 3, "5");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range("r2", "r4"), newCFSet("cf1"), true);
    testAndCallNext(iter1, "r2", "cf2:x", 3, "5");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), newCFSet("cf3"), true);
    testAndCallNext(iter1, "r1", "cf3:z", 2, "3");
    testAndCallNext(iter1, "r4", "cf4:z", 5, "8");
    testAndCallNext(iter1, "r5", "cf3:z", 6, "A");
    testAndCallNext(iter1, "r5", "cf4:z", 6, "B");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), newCFSet("foo"), true);
    testAndCallNext(iter1, "r1", "foo:b", 2, "9");
    testAndCallNext(iter1, "r3", "foo:b", 4, "6");
    testAndCallNext(iter1, "r4", "foo:b", 5, "7");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), newCFSet("cf1", "cf3"), true);
    testAndCallNext(iter1, "r1", "cf1:x", 2, "1");
    testAndCallNext(iter1, "r1", "cf1:y", 2, "2");
    testAndCallNext(iter1, "r1", "cf3:z", 2, "3");
    testAndCallNext(iter1, "r2", "cf2:x", 3, "5");
    testAndCallNext(iter1, "r4", "cf4:z", 5, "8");
    testAndCallNext(iter1, "r5", "cf3:z", 6, "A");
    testAndCallNext(iter1, "r5", "cf4:z", 6, "B");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range("r2", "r4"), newCFSet("cf1", "cf3"), true);
    testAndCallNext(iter1, "r2", "cf2:x", 3, "5");
    testAndCallNext(iter1, "r4", "cf4:z", 5, "8");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), newCFSet("cf1", "cf3", "foo"), true);
    assertAll(iter1);

    iter1.seek(new Range("r1", "r2"), newCFSet("cf1", "cf3", "foo"), true);
    testAndCallNext(iter1, "r1", "cf1:x", 2, "1");
    testAndCallNext(iter1, "r1", "cf1:y", 2, "2");
    testAndCallNext(iter1, "r1", "cf3:z", 2, "3");
    testAndCallNext(iter1, "r1", "foo:b", 2, "9");
    testAndCallNext(iter1, "r2", "cf2:x", 3, "5");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), Set.of(), false);
    assertAll(iter1);

    iter1.seek(new Range(), newCFSet("cf1"), false);
    assertAll(iter1);

    iter1.seek(new Range(), newCFSet("cf1", "cf2"), false);
    testAndCallNext(iter1, "r1", "cf3:z", 2, "3");
    testAndCallNext(iter1, "r1", "foo:b", 2, "9");
    testAndCallNext(iter1, "r3", "foo:b", 4, "6");
    testAndCallNext(iter1, "r4", "cf4:z", 5, "8");
    testAndCallNext(iter1, "r4", "foo:b", 5, "7");
    testAndCallNext(iter1, "r5", "cf3:z", 6, "A");
    testAndCallNext(iter1, "r5", "cf4:z", 6, "B");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range("r2"), newCFSet("cf1", "cf3", "foo"), true);
    testAndCallNext(iter1, "r2", "cf2:x", 3, "5");
    assertFalse(iter1.hasTop());
  }

  private void assertAll(SortedKeyValueIterator<Key,Value> iter1) throws IOException {
    testAndCallNext(iter1, "r1", "cf1:x", 2, "1");
    testAndCallNext(iter1, "r1", "cf1:y", 2, "2");
    testAndCallNext(iter1, "r1", "cf3:z", 2, "3");
    testAndCallNext(iter1, "r1", "foo:b", 2, "9");
    testAndCallNext(iter1, "r2", "cf2:x", 3, "5");
    testAndCallNext(iter1, "r3", "foo:b", 4, "6");
    testAndCallNext(iter1, "r4", "cf4:z", 5, "8");
    testAndCallNext(iter1, "r4", "foo:b", 5, "7");
    testAndCallNext(iter1, "r5", "cf3:z", 6, "A");
    testAndCallNext(iter1, "r5", "cf4:z", 6, "B");
    assertFalse(iter1.hasTop());
  }
}

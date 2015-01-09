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
package org.apache.accumulo.tserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ZooConfiguration;
import org.apache.accumulo.tserver.InMemoryMap.MemoryIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class InMemoryMapTest {

  @BeforeClass
  public static void setUp() throws Exception {
    // suppress log messages having to do with not having an instance
    Logger.getLogger(ZooConfiguration.class).setLevel(Level.OFF);
    Logger.getLogger(HdfsZooInstance.class).setLevel(Level.OFF);
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  public void mutate(InMemoryMap imm, String row, String column, long ts) {
    Mutation m = new Mutation(new Text(row));
    String[] sa = column.split(":");
    m.putDelete(new Text(sa[0]), new Text(sa[1]), ts);

    imm.mutate(Collections.singletonList(m));
  }

  public void mutate(InMemoryMap imm, String row, String column, long ts, String value) {
    Mutation m = new Mutation(new Text(row));
    String[] sa = column.split(":");
    m.put(new Text(sa[0]), new Text(sa[1]), ts, new Value(value.getBytes()));

    imm.mutate(Collections.singletonList(m));
  }

  static Key nk(String row, String column, long ts) {
    String[] sa = column.split(":");
    Key k = new Key(new Text(row), new Text(sa[0]), new Text(sa[1]), ts);
    return k;
  }

  static void ae(SortedKeyValueIterator<Key,Value> dc, String row, String column, int ts, String val) throws IOException {
    assertTrue(dc.hasTop());
    assertEquals(nk(row, column, ts), dc.getTopKey());
    assertEquals(new Value(val.getBytes()), dc.getTopValue());
    dc.next();

  }

  static Set<ByteSequence> newCFSet(String... cfs) {
    HashSet<ByteSequence> cfSet = new HashSet<ByteSequence>();
    for (String cf : cfs) {
      cfSet.add(new ArrayByteSequence(cf));
    }
    return cfSet;
  }

  @Test
  public void test2() throws Exception {
    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    MemoryIterator ski1 = imm.skvIterator();
    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    MemoryIterator ski2 = imm.skvIterator();

    ski1.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);
    assertFalse(ski1.hasTop());

    ski2.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);
    assertTrue(ski2.hasTop());
    ae(ski2, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski2.hasTop());

  }

  @Test
  public void test3() throws Exception {
    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq1", 3, "bar2");
    MemoryIterator ski1 = imm.skvIterator();
    mutate(imm, "r1", "foo:cq1", 3, "bar3");

    mutate(imm, "r3", "foo:cq1", 3, "bar9");
    mutate(imm, "r3", "foo:cq1", 3, "bara");

    MemoryIterator ski2 = imm.skvIterator();

    ski1.seek(new Range(new Text("r1")), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(ski1, "r1", "foo:cq1", 3, "bar2");
    ae(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski2.seek(new Range(new Text("r3")), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(ski2, "r3", "foo:cq1", 3, "bara");
    ae(ski2, "r3", "foo:cq1", 3, "bar9");
    assertFalse(ski1.hasTop());

  }

  @Test
  public void test4() throws Exception {
    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq1", 3, "bar2");
    MemoryIterator ski1 = imm.skvIterator();
    mutate(imm, "r1", "foo:cq1", 3, "bar3");

    imm.delete(0);

    ski1.seek(new Range(new Text("r1")), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(ski1, "r1", "foo:cq1", 3, "bar2");
    ae(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski1.seek(new Range(new Text("r1")), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(ski1, "r1", "foo:cq1", 3, "bar2");
    ae(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski1.seek(new Range(new Text("r2")), LocalityGroupUtil.EMPTY_CF_SET, false);
    assertFalse(ski1.hasTop());

    ski1.seek(new Range(nk("r1", "foo:cq1", 3), null), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(ski1, "r1", "foo:cq1", 3, "bar2");
    ae(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski1.close();
  }

  @Test
  public void test5() throws Exception {
    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq1", 3, "bar2");
    mutate(imm, "r1", "foo:cq1", 3, "bar3");

    MemoryIterator ski1 = imm.skvIterator();
    ski1.seek(new Range(new Text("r1")), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(ski1, "r1", "foo:cq1", 3, "bar3");

    imm.delete(0);

    ae(ski1, "r1", "foo:cq1", 3, "bar2");
    ae(ski1, "r1", "foo:cq1", 3, "bar1");
    assertFalse(ski1.hasTop());

    ski1.close();

    imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq2", 3, "bar2");
    mutate(imm, "r1", "foo:cq3", 3, "bar3");

    ski1 = imm.skvIterator();
    ski1.seek(new Range(new Text("r1")), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(ski1, "r1", "foo:cq1", 3, "bar1");

    imm.delete(0);

    ae(ski1, "r1", "foo:cq2", 3, "bar2");
    ae(ski1, "r1", "foo:cq3", 3, "bar3");
    assertFalse(ski1.hasTop());

    ski1.close();
  }

  @Test
  public void test6() throws Exception {
    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq2", 3, "bar2");
    mutate(imm, "r1", "foo:cq3", 3, "bar3");
    mutate(imm, "r1", "foo:cq4", 3, "bar4");

    MemoryIterator ski1 = imm.skvIterator();

    mutate(imm, "r1", "foo:cq5", 3, "bar5");

    SortedKeyValueIterator<Key,Value> dc = ski1.deepCopy(null);

    ski1.seek(new Range(nk("r1", "foo:cq1", 3), null), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(ski1, "r1", "foo:cq1", 3, "bar1");

    dc.seek(new Range(nk("r1", "foo:cq2", 3), null), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(dc, "r1", "foo:cq2", 3, "bar2");

    imm.delete(0);

    ae(ski1, "r1", "foo:cq2", 3, "bar2");
    ae(dc, "r1", "foo:cq3", 3, "bar3");
    ae(ski1, "r1", "foo:cq3", 3, "bar3");
    ae(dc, "r1", "foo:cq4", 3, "bar4");
    ae(ski1, "r1", "foo:cq4", 3, "bar4");
    assertFalse(ski1.hasTop());
    assertFalse(dc.hasTop());

    ski1.seek(new Range(nk("r1", "foo:cq3", 3), null), LocalityGroupUtil.EMPTY_CF_SET, false);

    dc.seek(new Range(nk("r1", "foo:cq4", 3), null), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(dc, "r1", "foo:cq4", 3, "bar4");
    assertFalse(dc.hasTop());

    ae(ski1, "r1", "foo:cq3", 3, "bar3");
    ae(ski1, "r1", "foo:cq4", 3, "bar4");
    assertFalse(ski1.hasTop());
    assertFalse(dc.hasTop());

    ski1.close();
  }

  private void deepCopyAndDelete(int interleaving, boolean interrupt) throws Exception {
    // interleaving == 0 intentionally omitted, this runs the test w/o deleting in mem map

    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq2", 3, "bar2");

    MemoryIterator ski1 = imm.skvIterator();

    AtomicBoolean iflag = new AtomicBoolean(false);
    ski1.setInterruptFlag(iflag);

    if (interleaving == 1) {
      imm.delete(0);
      if (interrupt)
        iflag.set(true);
    }

    SortedKeyValueIterator<Key,Value> dc = ski1.deepCopy(null);

    if (interleaving == 2) {
      imm.delete(0);
      if (interrupt)
        iflag.set(true);
    }

    dc.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);
    ski1.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);

    if (interleaving == 3) {
      imm.delete(0);
      if (interrupt)
        iflag.set(true);
    }

    ae(dc, "r1", "foo:cq1", 3, "bar1");
    ae(ski1, "r1", "foo:cq1", 3, "bar1");
    dc.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);

    if (interleaving == 4) {
      imm.delete(0);
      if (interrupt)
        iflag.set(true);
    }

    ae(ski1, "r1", "foo:cq2", 3, "bar2");
    ae(dc, "r1", "foo:cq1", 3, "bar1");
    ae(dc, "r1", "foo:cq2", 3, "bar2");
    assertFalse(dc.hasTop());
    assertFalse(ski1.hasTop());

    if (interrupt)
      dc.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);
  }

  @Test
  public void testDeepCopyAndDelete() throws Exception {
    for (int i = 0; i <= 4; i++)
      deepCopyAndDelete(i, false);

    for (int i = 1; i <= 4; i++)
      try {
        deepCopyAndDelete(i, true);
        fail("i = " + i);
      } catch (IterationInterruptedException iie) {}
  }

  @Test
  public void testBug1() throws Exception {
    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    for (int i = 0; i < 20; i++) {
      mutate(imm, "r1", "foo:cq" + i, 3, "bar" + i);
    }

    for (int i = 0; i < 20; i++) {
      mutate(imm, "r2", "foo:cq" + i, 3, "bar" + i);
    }

    MemoryIterator ski1 = imm.skvIterator();
    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(ski1);

    imm.delete(0);

    ArrayList<ByteSequence> columns = new ArrayList<ByteSequence>();
    columns.add(new ArrayByteSequence("bar"));

    // this seek resulted in an infinite loop before a bug was fixed
    cfsi.seek(new Range("r1"), columns, true);

    assertFalse(cfsi.hasTop());

    ski1.close();
  }

  @Test
  public void testSeekBackWards() throws Exception {
    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    mutate(imm, "r1", "foo:cq1", 3, "bar1");
    mutate(imm, "r1", "foo:cq2", 3, "bar2");
    mutate(imm, "r1", "foo:cq3", 3, "bar3");
    mutate(imm, "r1", "foo:cq4", 3, "bar4");

    MemoryIterator skvi1 = imm.skvIterator();

    skvi1.seek(new Range(nk("r1", "foo:cq3", 3), null), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(skvi1, "r1", "foo:cq3", 3, "bar3");

    skvi1.seek(new Range(nk("r1", "foo:cq1", 3), null), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(skvi1, "r1", "foo:cq1", 3, "bar1");

  }

  @Test
  public void testDuplicateKey() throws Exception {
    InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());

    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("foo"), new Text("cq"), 3, new Value("v1".getBytes()));
    m.put(new Text("foo"), new Text("cq"), 3, new Value("v2".getBytes()));
    imm.mutate(Collections.singletonList(m));

    MemoryIterator skvi1 = imm.skvIterator();
    skvi1.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);
    ae(skvi1, "r1", "foo:cq", 3, "v2");
    ae(skvi1, "r1", "foo:cq", 3, "v1");
  }

  private static final Logger log = Logger.getLogger(InMemoryMapTest.class);

  static long sum(long[] counts) {
    long result = 0;
    for (int i = 0; i < counts.length; i++)
      result += counts[i];
    return result;
  }

  // - hard to get this timing test to run well on apache build machines
  @Test
  @Ignore
  public void parallelWriteSpeed() throws InterruptedException, IOException {
    List<Double> timings = new ArrayList<Double>();
    for (int threads : new int[] {1, 2, 16, /* 64, 256 */}) {
      final long now = System.currentTimeMillis();
      final long counts[] = new long[threads];
      final InMemoryMap imm = new InMemoryMap(false, tempFolder.newFolder().getAbsolutePath());
      ExecutorService e = Executors.newFixedThreadPool(threads);
      for (int j = 0; j < threads; j++) {
        final int threadId = j;
        e.execute(new Runnable() {
          @Override
          public void run() {
            while (System.currentTimeMillis() - now < 1000) {
              for (int k = 0; k < 1000; k++) {
                Mutation m = new Mutation("row");
                m.put("cf", "cq", new Value("v".getBytes()));
                List<Mutation> mutations = Collections.singletonList(m);
                imm.mutate(mutations);
                counts[threadId]++;
              }
            }
          }
        });
      }
      e.shutdown();
      e.awaitTermination(10, TimeUnit.SECONDS);
      imm.delete(10000);
      double mutationsPerSecond = sum(counts) / ((System.currentTimeMillis() - now) / 1000.);
      timings.add(mutationsPerSecond);
      log.info(String.format("%.1f mutations per second with %d threads", mutationsPerSecond, threads));
    }
    // verify that more threads doesn't go a lot faster, or a lot slower than one thread
    for (int i = 0; i < timings.size(); i++) {
      double ratioFirst = timings.get(0) / timings.get(i);
      assertTrue(ratioFirst < 3);
      assertTrue(ratioFirst > 0.3);
    }
  }

  @Test
  public void testLocalityGroups() throws Exception {

    Map<String,Set<ByteSequence>> lggroups1 = new HashMap<String,Set<ByteSequence>>();
    lggroups1.put("lg1", newCFSet("cf1", "cf2"));
    lggroups1.put("lg2", newCFSet("cf3", "cf4"));

    InMemoryMap imm = new InMemoryMap(lggroups1, false, tempFolder.newFolder().getAbsolutePath());

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

    imm.mutate(Arrays.asList(m1, m2, m3, m4, m5));

    MemoryIterator iter1 = imm.skvIterator();

    seekLocalityGroups(iter1);
    SortedKeyValueIterator<Key,Value> dc1 = iter1.deepCopy(null);
    seekLocalityGroups(dc1);

    assertTrue(imm.getNumEntries() == 10);
    assertTrue(imm.estimatedSizeInBytes() > 0);

    imm.delete(0);

    seekLocalityGroups(iter1);
    seekLocalityGroups(dc1);
    // TODO uncomment following when ACCUMULO-1628 is fixed
    // seekLocalityGroups(iter1.deepCopy(null));
  }

  private void seekLocalityGroups(SortedKeyValueIterator<Key,Value> iter1) throws IOException {
    iter1.seek(new Range(), newCFSet("cf1"), true);
    ae(iter1, "r1", "cf1:x", 2, "1");
    ae(iter1, "r1", "cf1:y", 2, "2");
    ae(iter1, "r2", "cf2:x", 3, "5");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range("r2", "r4"), newCFSet("cf1"), true);
    ae(iter1, "r2", "cf2:x", 3, "5");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), newCFSet("cf3"), true);
    ae(iter1, "r1", "cf3:z", 2, "3");
    ae(iter1, "r4", "cf4:z", 5, "8");
    ae(iter1, "r5", "cf3:z", 6, "A");
    ae(iter1, "r5", "cf4:z", 6, "B");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), newCFSet("foo"), true);
    ae(iter1, "r1", "foo:b", 2, "9");
    ae(iter1, "r3", "foo:b", 4, "6");
    ae(iter1, "r4", "foo:b", 5, "7");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), newCFSet("cf1", "cf3"), true);
    ae(iter1, "r1", "cf1:x", 2, "1");
    ae(iter1, "r1", "cf1:y", 2, "2");
    ae(iter1, "r1", "cf3:z", 2, "3");
    ae(iter1, "r2", "cf2:x", 3, "5");
    ae(iter1, "r4", "cf4:z", 5, "8");
    ae(iter1, "r5", "cf3:z", 6, "A");
    ae(iter1, "r5", "cf4:z", 6, "B");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range("r2", "r4"), newCFSet("cf1", "cf3"), true);
    ae(iter1, "r2", "cf2:x", 3, "5");
    ae(iter1, "r4", "cf4:z", 5, "8");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), newCFSet("cf1", "cf3", "foo"), true);
    assertAll(iter1);

    iter1.seek(new Range("r1", "r2"), newCFSet("cf1", "cf3", "foo"), true);
    ae(iter1, "r1", "cf1:x", 2, "1");
    ae(iter1, "r1", "cf1:y", 2, "2");
    ae(iter1, "r1", "cf3:z", 2, "3");
    ae(iter1, "r1", "foo:b", 2, "9");
    ae(iter1, "r2", "cf2:x", 3, "5");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);
    assertAll(iter1);

    iter1.seek(new Range(), newCFSet("cf1"), false);
    assertAll(iter1);

    iter1.seek(new Range(), newCFSet("cf1", "cf2"), false);
    ae(iter1, "r1", "cf3:z", 2, "3");
    ae(iter1, "r1", "foo:b", 2, "9");
    ae(iter1, "r3", "foo:b", 4, "6");
    ae(iter1, "r4", "cf4:z", 5, "8");
    ae(iter1, "r4", "foo:b", 5, "7");
    ae(iter1, "r5", "cf3:z", 6, "A");
    ae(iter1, "r5", "cf4:z", 6, "B");
    assertFalse(iter1.hasTop());

    iter1.seek(new Range("r2"), newCFSet("cf1", "cf3", "foo"), true);
    ae(iter1, "r2", "cf2:x", 3, "5");
    assertFalse(iter1.hasTop());
  }

  private void assertAll(SortedKeyValueIterator<Key,Value> iter1) throws IOException {
    ae(iter1, "r1", "cf1:x", 2, "1");
    ae(iter1, "r1", "cf1:y", 2, "2");
    ae(iter1, "r1", "cf3:z", 2, "3");
    ae(iter1, "r1", "foo:b", 2, "9");
    ae(iter1, "r2", "cf2:x", 3, "5");
    ae(iter1, "r3", "foo:b", 4, "6");
    ae(iter1, "r4", "cf4:z", 5, "8");
    ae(iter1, "r4", "foo:b", 5, "7");
    ae(iter1, "r5", "cf3:z", 6, "A");
    ae(iter1, "r5", "cf4:z", 6, "B");
    assertFalse(iter1.hasTop());
  }
}

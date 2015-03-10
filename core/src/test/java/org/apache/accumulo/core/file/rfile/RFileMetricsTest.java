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
package org.apache.accumulo.core.file.rfile;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.util.concurrent.AtomicLongMap;

/**
 *
 */
public class RFileMetricsTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  static {
    Logger.getLogger(org.apache.hadoop.io.compress.CodecPool.class).setLevel(Level.OFF);
    Logger.getLogger(org.apache.hadoop.util.NativeCodeLoader.class).setLevel(Level.OFF);
  }

  private TestRFile trf = null;

  @Before
  public void makeTestRFile() {
    trf = new TestRFile();
  }

  @After
  public void cleanUpTestRFile() {
    // do our best to clean up first
    if (trf != null) {
      if (trf.writer != null) {
        try {
          trf.closeWriter();
        } catch (IOException e) {
          // ignore
        }
      }
      if (trf.reader != null) {
        try {
          trf.closeReader();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    trf = null;
  }

  public static class TestRFile extends RFileTest.TestRFile {

    public TestRFile() {
      super(null);
    }

    public VisMetricsGatherer gatherMetrics() throws IOException {
      VisMetricsGatherer vmg = new VisMetricsGatherer();
      reader.registerMetrics(vmg);
      Map<String,ArrayList<ByteSequence>> localityGroupCF = reader.getLocalityGroupCF();

      for (Entry<String,ArrayList<ByteSequence>> cf : localityGroupCF.entrySet()) {

        reader.seek(new Range((Key) null, (Key) null), cf.getValue(), true);
        while (reader.hasTop()) {
          reader.next();
        }
      }
      return vmg;
    }
  }

  @Test
  public void emptyFile() throws IOException {
    // test an empty file

    trf.openWriter();
    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    Map<String,AtomicLongMap<String>> metrics = vmg.metric;
    Map<String,AtomicLongMap<String>> blocks = vmg.blocks;
    assertEquals(0, metrics.size());

    assertEquals(0, blocks.size());

    trf.closeReader();
  }

  @Test
  public void oneEntryDefaultLocGroup() throws IOException {
    // test an rfile with one entry in the default locality group

    trf.openWriter();
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get(null);
    AtomicLongMap<String> blocks = vmg.blocks.get(null);
    assertEquals(1, metrics.get("L1"));

    assertEquals(1, blocks.get("L1"));

    assertEquals(1, vmg.numEntries.get(vmg.localityGroups.indexOf(null)).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf(null)).longValue());

    trf.closeReader();
  }

  @Test
  public void twoEntriesDefaultLocGroup() throws IOException {
    // test an rfile with two entries in the default locality group

    trf.openWriter();
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L2", 55), RFileTest.nv("foo"));
    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get(null);
    AtomicLongMap<String> blocks = vmg.blocks.get(null);
    assertEquals(1, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(1, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(2, vmg.numEntries.get(vmg.localityGroups.indexOf(null)).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf(null)).longValue());

    trf.closeReader();

  }

  @Test
  public void oneEntryNonDefaultLocGroup() throws IOException {
    // test an rfile with two entries in a non-default locality group

    trf.openWriter(false);
    Set<ByteSequence> lg1 = new HashSet<>();
    lg1.add(new ArrayByteSequence("cf1"));

    trf.writer.startNewLocalityGroup("lg1", lg1);
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get("lg1");
    AtomicLongMap<String> blocks = vmg.blocks.get("lg1");
    assertEquals(1, metrics.get("L1"));

    assertEquals(1, blocks.get("L1"));

    assertEquals(1, vmg.numEntries.get(vmg.localityGroups.indexOf("lg1")).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf("lg1")).longValue());

    trf.closeReader();

  }

  @Test
  public void twoEntryNonDefaultLocGroup() throws IOException {
    // test an rfile with two entries in a non-default locality group

    trf.openWriter(false);
    Set<ByteSequence> lg1 = new HashSet<>();
    lg1.add(new ArrayByteSequence("cf1"));

    trf.writer.startNewLocalityGroup("lg1", lg1);
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L2", 55), RFileTest.nv("foo"));
    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get("lg1");
    AtomicLongMap<String> blocks = vmg.blocks.get("lg1");
    assertEquals(1, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(1, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(2, vmg.numEntries.get(vmg.localityGroups.indexOf("lg1")).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf("lg1")).longValue());

    trf.closeReader();

  }

  @Test
  public void twoNonDefaultLocGroups() throws IOException {
    // test an rfile with two entries in 2 non-default locality groups

    trf.openWriter(false);
    Set<ByteSequence> lg1 = new HashSet<>();
    lg1.add(new ArrayByteSequence("cf1"));

    trf.writer.startNewLocalityGroup("lg1", lg1);
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L2", 55), RFileTest.nv("foo"));

    Set<ByteSequence> lg2 = new HashSet<>();
    lg2.add(new ArrayByteSequence("cf2"));

    trf.writer.startNewLocalityGroup("lg2", lg2);
    trf.writer.append(RFileTest.nk("r1", "cf2", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf2", "cq1", "L2", 55), RFileTest.nv("foo"));

    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get("lg1");
    AtomicLongMap<String> blocks = vmg.blocks.get("lg1");
    assertEquals(1, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(1, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(2, vmg.numEntries.get(vmg.localityGroups.indexOf("lg1")).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf("lg1")).longValue());

    metrics = vmg.metric.get("lg2");
    blocks = vmg.blocks.get("lg2");
    assertEquals(1, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(1, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(2, vmg.numEntries.get(vmg.localityGroups.indexOf("lg2")).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf("lg2")).longValue());

    trf.closeReader();

  }

  @Test
  public void nonDefaultAndDefaultLocGroup() throws IOException {
    // test an rfile with 3 entries in a non-default locality group and the default locality group

    trf.openWriter(false);
    Set<ByteSequence> lg1 = new HashSet<>();
    lg1.add(new ArrayByteSequence("cf1"));

    trf.writer.startNewLocalityGroup("lg1", lg1);
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq2", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq2", "L2", 55), RFileTest.nv("foo"));

    trf.writer.startDefaultLocalityGroup();
    trf.writer.append(RFileTest.nk("r1", "cf2", "cq1", "A", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf2", "cq1", "B", 55), RFileTest.nv("foo"));

    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get("lg1");
    AtomicLongMap<String> blocks = vmg.blocks.get("lg1");
    assertEquals(2, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(1, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(3, vmg.numEntries.get(vmg.localityGroups.indexOf("lg1")).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf("lg1")).longValue());

    metrics = vmg.metric.get(null);
    blocks = vmg.blocks.get(null);
    assertEquals(1, metrics.get("A"));
    assertEquals(1, metrics.get("B"));

    assertEquals(1, blocks.get("A"));
    assertEquals(1, blocks.get("B"));

    assertEquals(2, vmg.numEntries.get(vmg.localityGroups.indexOf(null)).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf(null)).longValue());

    trf.closeReader();

  }

  @Test
  public void multiCFNonDefaultAndDefaultLocGroup() throws IOException {
    // test an rfile with multiple column families in a non-default locality group and the default locality group

    trf.openWriter(false);
    Set<ByteSequence> lg1 = new HashSet<>();
    lg1.add(new ArrayByteSequence("cf1"));
    lg1.add(new ArrayByteSequence("cf3"));

    trf.writer.startNewLocalityGroup("lg1", lg1);
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq2", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf3", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf3", "cq2", "L2", 55), RFileTest.nv("foo"));

    trf.writer.startDefaultLocalityGroup();
    trf.writer.append(RFileTest.nk("r1", "cf2", "cq1", "A", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf2", "cq1", "B", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf4", "cq1", "A", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf4", "cq1", "B", 55), RFileTest.nv("foo"));

    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get("lg1");
    AtomicLongMap<String> blocks = vmg.blocks.get("lg1");
    assertEquals(3, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(1, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(4, vmg.numEntries.get(vmg.localityGroups.indexOf("lg1")).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf("lg1")).longValue());

    metrics = vmg.metric.get(null);
    blocks = vmg.blocks.get(null);
    assertEquals(2, metrics.get("A"));
    assertEquals(2, metrics.get("B"));

    assertEquals(1, blocks.get("A"));
    assertEquals(1, blocks.get("B"));

    assertEquals(4, vmg.numEntries.get(vmg.localityGroups.indexOf(null)).longValue());
    assertEquals(1, vmg.numBlocks.get(vmg.localityGroups.indexOf(null)).longValue());

    trf.closeReader();

  }

  @Test
  public void multiBlockDefaultLocGroup() throws IOException {
    // test an rfile with four blocks in the default locality group

    trf.openWriter(20);// Each entry is a block
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq2", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf3", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf3", "cq2", "L2", 55), RFileTest.nv("foo"));
    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get(null);
    AtomicLongMap<String> blocks = vmg.blocks.get(null);
    assertEquals(3, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(3, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(4, vmg.numEntries.get(vmg.localityGroups.indexOf(null)).longValue());
    assertEquals(4, vmg.numBlocks.get(vmg.localityGroups.indexOf(null)).longValue());

    trf.closeReader();

  }

  @Test
  public void multiBlockNonDefaultLocGroup() throws IOException {
    // test an rfile with four blocks in a non-default locality group

    trf.openWriter(false, 20);// Each entry is a block
    Set<ByteSequence> lg1 = new HashSet<>();
    lg1.add(new ArrayByteSequence("cf1"));
    lg1.add(new ArrayByteSequence("cf3"));

    trf.writer.startNewLocalityGroup("lg1", lg1);
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq2", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf3", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf3", "cq2", "L2", 55), RFileTest.nv("foo"));
    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get("lg1");
    AtomicLongMap<String> blocks = vmg.blocks.get("lg1");
    assertEquals(3, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(3, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(4, vmg.numEntries.get(vmg.localityGroups.indexOf("lg1")).longValue());
    assertEquals(4, vmg.numBlocks.get(vmg.localityGroups.indexOf("lg1")).longValue());

    trf.closeReader();

  }

  @Test
  public void multiBlockMultiCFNonDefaultAndDefaultLocGroup() throws IOException {
    // test an rfile with multiple column families and multiple blocks in a non-default locality group and the default locality group

    trf.openWriter(false, 20);// Each entry is a block
    Set<ByteSequence> lg1 = new HashSet<>();
    lg1.add(new ArrayByteSequence("cf1"));
    lg1.add(new ArrayByteSequence("cf3"));

    trf.writer.startNewLocalityGroup("lg1", lg1);
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf1", "cq2", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf3", "cq1", "L1", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf3", "cq2", "L2", 55), RFileTest.nv("foo"));

    trf.writer.startDefaultLocalityGroup();
    trf.writer.append(RFileTest.nk("r1", "cf2", "cq1", "A", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf2", "cq1", "B", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf4", "cq1", "A", 55), RFileTest.nv("foo"));
    trf.writer.append(RFileTest.nk("r1", "cf4", "cq1", "B", 55), RFileTest.nv("foo"));

    trf.closeWriter();

    trf.openReader(false);

    VisMetricsGatherer vmg = trf.gatherMetrics();

    AtomicLongMap<String> metrics = vmg.metric.get("lg1");
    AtomicLongMap<String> blocks = vmg.blocks.get("lg1");
    assertEquals(3, metrics.get("L1"));
    assertEquals(1, metrics.get("L2"));

    assertEquals(3, blocks.get("L1"));
    assertEquals(1, blocks.get("L2"));

    assertEquals(4, vmg.numEntries.get(vmg.localityGroups.indexOf("lg1")).longValue());
    assertEquals(4, vmg.numBlocks.get(vmg.localityGroups.indexOf("lg1")).longValue());

    metrics = vmg.metric.get(null);
    blocks = vmg.blocks.get(null);
    assertEquals(2, metrics.get("A"));
    assertEquals(2, metrics.get("B"));

    assertEquals(2, blocks.get("A"));
    assertEquals(2, blocks.get("B"));

    assertEquals(4, vmg.numEntries.get(vmg.localityGroups.indexOf(null)).longValue());
    assertEquals(4, vmg.numBlocks.get(vmg.localityGroups.indexOf(null)).longValue());

    trf.closeReader();
  }

}

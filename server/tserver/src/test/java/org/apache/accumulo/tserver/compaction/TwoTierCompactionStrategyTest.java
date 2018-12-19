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
package org.apache.accumulo.tserver.compaction;

import static org.apache.accumulo.tserver.compaction.DefaultCompactionStrategyTest.getServerContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.tserver.InMemoryMapTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests org.apache.accumulo.tserver.compaction.TwoTierCompactionStrategy
 */
public class TwoTierCompactionStrategyTest {
  private String largeCompressionType = "gz";
  private TwoTierCompactionStrategy ttcs = null;
  private MajorCompactionRequest mcr = null;
  private AccumuloConfiguration conf = null;
  private HashMap<String,String> opts = new HashMap<>();

  private Map<FileRef,DataFileValue> createFileMap(String... sa) {

    HashMap<FileRef,DataFileValue> ret = new HashMap<>();
    for (int i = 0; i < sa.length; i += 2) {
      ret.put(new FileRef("hdfs://nn1/accumulo/tables/5/t-0001/" + sa[i]),
          new DataFileValue(ConfigurationTypeHelper.getFixedMemoryAsBytes(sa[i + 1]), 1));
    }

    return ret;
  }

  @Before
  public void setup() {
    opts.put(TwoTierCompactionStrategy.LARGE_FILE_COMPRESSION_TYPE, largeCompressionType);
    opts.put(TwoTierCompactionStrategy.LARGE_FILE_COMPRESSION_THRESHOLD, "500M");
    ttcs = new TwoTierCompactionStrategy();
  }

  @Test
  public void testDefaultCompaction() {
    ttcs.init(opts);
    conf = DefaultConfiguration.getInstance();
    KeyExtent ke = new KeyExtent(Table.ID.of("0"), null, null);
    mcr = new MajorCompactionRequest(ke, MajorCompactionReason.NORMAL, conf,
        InMemoryMapTest.getServerContext());
    Map<FileRef,DataFileValue> fileMap = createFileMap("f1", "10M", "f2", "10M", "f3", "10M", "f4",
        "10M", "f5", "100M", "f6", "100M", "f7", "100M", "f8", "100M");
    mcr.setFiles(fileMap);

    assertTrue(ttcs.shouldCompact(mcr));
    assertEquals(8, mcr.getFiles().size());

    List<FileRef> filesToCompact = ttcs.getCompactionPlan(mcr).inputFiles;
    assertEquals(fileMap.keySet(), new HashSet<>(filesToCompact));
    assertEquals(8, filesToCompact.size());
    assertNull(ttcs.getCompactionPlan(mcr).writeParameters.getCompressType());
  }

  @Test
  public void testLargeCompaction() {
    ttcs.init(opts);
    conf = DefaultConfiguration.getInstance();
    KeyExtent ke = new KeyExtent(Table.ID.of("0"), null, null);
    mcr = new MajorCompactionRequest(ke, MajorCompactionReason.NORMAL, conf, getServerContext());
    Map<FileRef,DataFileValue> fileMap = createFileMap("f1", "2G", "f2", "2G", "f3", "2G", "f4",
        "2G");
    mcr.setFiles(fileMap);

    assertTrue(ttcs.shouldCompact(mcr));
    assertEquals(4, mcr.getFiles().size());

    List<FileRef> filesToCompact = ttcs.getCompactionPlan(mcr).inputFiles;
    assertEquals(fileMap.keySet(), new HashSet<>(filesToCompact));
    assertEquals(4, filesToCompact.size());
    assertEquals(largeCompressionType,
        ttcs.getCompactionPlan(mcr).writeParameters.getCompressType());
  }

  @Test
  public void testMissingConfigProperties() {
    try {
      opts.clear();
      ttcs.init(opts);
      fail("IllegalArgumentException should have been thrown.");
    } catch (IllegalArgumentException iae) {} catch (Throwable t) {
      fail("IllegalArgumentException should have been thrown.");
    }
  }

  @Test
  public void testFileSubsetCompaction() {
    ttcs.init(opts);
    conf = DefaultConfiguration.getInstance();
    KeyExtent ke = new KeyExtent(Table.ID.of("0"), null, null);
    mcr = new MajorCompactionRequest(ke, MajorCompactionReason.NORMAL, conf, getServerContext());
    Map<FileRef,DataFileValue> fileMap = createFileMap("f1", "1G", "f2", "10M", "f3", "10M", "f4",
        "10M", "f5", "10M", "f6", "10M", "f7", "10M");
    Map<FileRef,DataFileValue> filesToCompactMap = createFileMap("f2", "10M", "f3", "10M", "f4",
        "10M", "f5", "10M", "f6", "10M", "f7", "10M");
    mcr.setFiles(fileMap);

    assertTrue(ttcs.shouldCompact(mcr));
    assertEquals(7, mcr.getFiles().size());

    List<FileRef> filesToCompact = ttcs.getCompactionPlan(mcr).inputFiles;
    assertEquals(filesToCompactMap.keySet(), new HashSet<>(filesToCompact));
    assertEquals(6, filesToCompact.size());
    assertNull(ttcs.getCompactionPlan(mcr).writeParameters.getCompressType());
  }

}

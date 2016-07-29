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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;
import org.junit.Assert;
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

  private Map<FileRef,DataFileValue> createFileMap(String... sa) {

    HashMap<FileRef,DataFileValue> ret = new HashMap<>();
    for (int i = 0; i < sa.length; i += 2) {
      ret.put(new FileRef("hdfs://nn1/accumulo/tables/5/t-0001/" + sa[i]), new DataFileValue(AccumuloConfiguration.getMemoryInBytes(sa[i + 1]), 1));
    }

    return ret;
  }

  private AccumuloConfiguration createProperTableConfiguration() {
    ConfigurationCopy result = new ConfigurationCopy(AccumuloConfiguration.getDefaultConfiguration());
    result.set(TwoTierCompactionStrategy.TABLE_LARGE_FILE_COMPRESSION_TYPE, largeCompressionType);
    result.set(TwoTierCompactionStrategy.TABLE_LARGE_FILE_COMPRESSION_THRESHOLD, "500M");
    return result;
  }

  @Before
  public void setup() {
    ttcs = new TwoTierCompactionStrategy();
  }

  @Test
  public void testDefaultCompaction() throws IOException {
    conf = createProperTableConfiguration();
    KeyExtent ke = new KeyExtent("0", null, null);
    mcr = new MajorCompactionRequest(ke, MajorCompactionReason.NORMAL, null, conf);
    Map<FileRef,DataFileValue> fileMap = createFileMap("f1", "10M", "f2", "10M", "f3", "10M", "f4", "10M", "f5", "100M", "f6", "100M", "f7", "100M", "f8",
        "100M");
    mcr.setFiles(fileMap);

    Assert.assertTrue(ttcs.shouldCompact(mcr));
    Assert.assertEquals(fileMap.keySet(), new HashSet<>(ttcs.getCompactionPlan(mcr).inputFiles));
    Assert.assertEquals(8, mcr.getFiles().size());
    Assert.assertEquals(null, ttcs.getCompactionPlan(mcr).writeParameters.getCompressType());
  }

  @Test
  public void testLargeCompaction() throws IOException {
    conf = createProperTableConfiguration();
    KeyExtent ke = new KeyExtent("0", null, null);
    mcr = new MajorCompactionRequest(ke, MajorCompactionReason.NORMAL, null, conf);
    Map<FileRef,DataFileValue> fileMap = createFileMap("f1", "2G", "f2", "2G", "f3", "2G", "f4", "2G");

    mcr.setFiles(fileMap);

    Assert.assertTrue(ttcs.shouldCompact(mcr));
    Assert.assertEquals(fileMap.keySet(), new HashSet<>(ttcs.getCompactionPlan(mcr).inputFiles));
    Assert.assertEquals(4, mcr.getFiles().size());
    Assert.assertEquals(largeCompressionType, ttcs.getCompactionPlan(mcr).writeParameters.getCompressType());
  }

  @Test
  public void testMissingConfigProperties() {
    conf = AccumuloConfiguration.getDefaultConfiguration();
    KeyExtent ke = new KeyExtent("0", null, null);
    mcr = new MajorCompactionRequest(ke, MajorCompactionReason.NORMAL, null, conf);
    Map<FileRef,DataFileValue> fileMap = createFileMap("f1", "10M", "f2", "10M", "f3", "10M", "f4", "10M", "f5", "100M", "f6", "100M", "f7", "100M", "f8",
        "100M");
    mcr.setFiles(fileMap);

    try {
      ttcs.getCompactionPlan(mcr);
      Assert.assertTrue("IllegalArgumentException should have been thrown.", false);
    } catch (IllegalArgumentException iae) {} catch (Throwable t) {
      Assert.assertTrue("IllegalArgumentException should have been thrown.", false);
    }
  }

}

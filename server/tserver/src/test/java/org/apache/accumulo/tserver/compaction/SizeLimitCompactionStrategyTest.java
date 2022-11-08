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
package org.apache.accumulo.tserver.compaction;

import static org.apache.accumulo.tserver.compaction.DefaultCompactionStrategyTest.getServerContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.junit.jupiter.api.Test;

@SuppressWarnings("removal")
public class SizeLimitCompactionStrategyTest {

  private static Map<StoredTabletFile,DataFileValue> nfl(String... sa) {

    HashMap<StoredTabletFile,DataFileValue> ret = new HashMap<>();
    for (int i = 0; i < sa.length; i += 2) {
      ret.put(new StoredTabletFile("hdfs://nn1/accumulo/tables/5/t-0001/" + sa[i]),
          new DataFileValue(ConfigurationTypeHelper.getFixedMemoryAsBytes(sa[i + 1]), 1));
    }

    return ret;
  }

  public static void testSizeLimit(String opt, CompactionStrategy slcs) throws IOException {
    HashMap<String,String> opts = new HashMap<>();
    opts.put(opt, "1G");

    slcs.init(opts);

    KeyExtent ke = new KeyExtent(TableId.of("0"), null, null);
    MajorCompactionRequest mcr = new MajorCompactionRequest(ke, MajorCompactionReason.NORMAL,
        DefaultConfiguration.getInstance(), getServerContext());

    mcr.setFiles(nfl("f1", "2G", "f2", "2G", "f3", "2G", "f4", "2G"));

    assertFalse(slcs.shouldCompact(mcr));
    assertEquals(0, slcs.getCompactionPlan(mcr).inputFiles.size());
    assertEquals(4, mcr.getFiles().size());

    mcr.setFiles(nfl("f1", "2G", "f2", "2G", "f3", "2G", "f4", "2G", "f5", "500M", "f6", "500M",
        "f7", "500M", "f8", "500M"));

    assertTrue(slcs.shouldCompact(mcr));
    assertEquals(nfl("f5", "500M", "f6", "500M", "f7", "500M", "f8", "500M").keySet(),
        new HashSet<>(slcs.getCompactionPlan(mcr).inputFiles));
    assertEquals(8, mcr.getFiles().size());
  }

  @Test
  public void testLimits() throws IOException {
    SizeLimitCompactionStrategy slcs = new SizeLimitCompactionStrategy();

    testSizeLimit(SizeLimitCompactionStrategy.SIZE_LIMIT_OPT, slcs);
  }
}

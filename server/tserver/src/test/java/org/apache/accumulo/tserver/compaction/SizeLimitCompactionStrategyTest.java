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

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SizeLimitCompactionStrategyTest {
  private Map<FileRef,DataFileValue> nfl(String... sa) {

    HashMap<FileRef,DataFileValue> ret = new HashMap<>();
    for (int i = 0; i < sa.length; i += 2) {
      ret.put(new FileRef("hdfs://nn1/accumulo/tables/5/t-0001/" + sa[i]), new DataFileValue(ConfigurationTypeHelper.getFixedMemoryAsBytes(sa[i + 1]), 1));
    }

    return ret;
  }

  @Test
  public void testLimits() throws IOException {
    SizeLimitCompactionStrategy slcs = new SizeLimitCompactionStrategy();
    HashMap<String,String> opts = new HashMap<>();
    opts.put(SizeLimitCompactionStrategy.SIZE_LIMIT_OPT, "1G");

    slcs.init(opts);

    KeyExtent ke = new KeyExtent(Table.ID.of("0"), null, null);
    MajorCompactionRequest mcr = new MajorCompactionRequest(ke, MajorCompactionReason.NORMAL, DefaultConfiguration.getInstance());

    mcr.setFiles(nfl("f1", "2G", "f2", "2G", "f3", "2G", "f4", "2G"));

    Assert.assertFalse(slcs.shouldCompact(mcr));
    Assert.assertEquals(0, slcs.getCompactionPlan(mcr).inputFiles.size());
    Assert.assertEquals(4, mcr.getFiles().size());

    mcr.setFiles(nfl("f1", "2G", "f2", "2G", "f3", "2G", "f4", "2G", "f5", "500M", "f6", "500M", "f7", "500M", "f8", "500M"));

    Assert.assertTrue(slcs.shouldCompact(mcr));
    Assert.assertEquals(nfl("f5", "500M", "f6", "500M", "f7", "500M", "f8", "500M").keySet(), new HashSet<>(slcs.getCompactionPlan(mcr).inputFiles));
    Assert.assertEquals(8, mcr.getFiles().size());
  }
}

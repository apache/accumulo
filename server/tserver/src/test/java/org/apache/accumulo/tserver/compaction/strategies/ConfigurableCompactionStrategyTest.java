/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.compaction.strategies;

import static org.apache.accumulo.tserver.compaction.DefaultCompactionStrategyTest.getServerContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.compaction.CompactionSettings;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.junit.Test;

public class ConfigurableCompactionStrategyTest {

  // file selection options are adequately tested by ShellServerIT

  @Test
  public void testOutputOptions() {
    MajorCompactionRequest mcr =
        new MajorCompactionRequest(new KeyExtent(TableId.of("1"), null, null),
            MajorCompactionReason.USER, null, getServerContext());

    Map<StoredTabletFile,DataFileValue> files = new HashMap<>();
    files.put(new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-009/F00001.rf"),
        new DataFileValue(50000, 400));
    mcr.setFiles(files);

    // test setting no output options
    ConfigurableCompactionStrategy ccs = new ConfigurableCompactionStrategy();

    Map<String,String> opts = new HashMap<>();
    ccs.init(opts);

    CompactionPlan plan = ccs.getCompactionPlan(mcr);

    assertEquals(0, plan.writeParameters.getBlockSize());
    assertEquals(0, plan.writeParameters.getHdfsBlockSize());
    assertEquals(0, plan.writeParameters.getIndexBlockSize());
    assertEquals(0, plan.writeParameters.getReplication());
    assertNull(plan.writeParameters.getCompressType());

    // test setting all output options
    ccs = new ConfigurableCompactionStrategy();

    CompactionSettings.OUTPUT_BLOCK_SIZE_OPT.put(opts, "64K");
    CompactionSettings.OUTPUT_COMPRESSION_OPT.put(opts, "snappy");
    CompactionSettings.OUTPUT_HDFS_BLOCK_SIZE_OPT.put(opts, "256M");
    CompactionSettings.OUTPUT_INDEX_BLOCK_SIZE_OPT.put(opts, "32K");
    CompactionSettings.OUTPUT_REPLICATION_OPT.put(opts, "5");

    ccs.init(opts);

    plan = ccs.getCompactionPlan(mcr);

    assertEquals(ConfigurationTypeHelper.getFixedMemoryAsBytes("64K"),
        plan.writeParameters.getBlockSize());
    assertEquals(ConfigurationTypeHelper.getFixedMemoryAsBytes("256M"),
        plan.writeParameters.getHdfsBlockSize());
    assertEquals(ConfigurationTypeHelper.getFixedMemoryAsBytes("32K"),
        plan.writeParameters.getIndexBlockSize());
    assertEquals(5, plan.writeParameters.getReplication());
    assertEquals("snappy", plan.writeParameters.getCompressType());

  }
}

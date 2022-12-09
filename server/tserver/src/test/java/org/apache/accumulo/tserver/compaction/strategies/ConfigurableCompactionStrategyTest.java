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
package org.apache.accumulo.tserver.compaction.strategies;

import static org.apache.accumulo.core.conf.ConfigurationTypeHelper.getFixedMemoryAsBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer.Overrides;
import org.apache.accumulo.core.compaction.CompactionSettings;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.junit.jupiter.api.Test;

public class ConfigurableCompactionStrategyTest {

  // file selection options are adequately tested by ShellServerIT

  @Test
  public void testOutputOptions() throws URISyntaxException {

    Collection<CompactableFile> files = Set.of(CompactableFile
        .create(new URI("hdfs://nn1/accumulo/tables/1/t-009/F00001.rf"), 50000, 400));

    // test setting no output options
    ConfigurableCompactionStrategy ccs = new ConfigurableCompactionStrategy();

    Map<String,String> opts = new HashMap<>();

    var initParams = new CompactionConfigurer.InitParameters() {

      @Override
      public TableId getTableId() {
        return TableId.of("1");
      }

      @Override
      public Map<String,String> getOptions() {
        return opts;
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return null;
      }
    };

    ccs.init(initParams);

    var inputParams = new CompactionConfigurer.InputParameters() {

      @Override
      public TableId getTableId() {
        return null;
      }

      @Override
      public TabletId getTabletId() {
        return null;
      }

      @Override
      public Collection<CompactableFile> getInputFiles() {
        return files;
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return null;
      }
    };

    Overrides plan = ccs.override(inputParams);

    assertTrue(plan.getOverrides().isEmpty());

    // test setting all output options
    ccs = new ConfigurableCompactionStrategy();

    CompactionSettings.OUTPUT_BLOCK_SIZE_OPT.put(null, opts, "64K");
    CompactionSettings.OUTPUT_COMPRESSION_OPT.put(null, opts, "snappy");
    CompactionSettings.OUTPUT_HDFS_BLOCK_SIZE_OPT.put(null, opts, "256M");
    CompactionSettings.OUTPUT_INDEX_BLOCK_SIZE_OPT.put(null, opts, "32K");
    CompactionSettings.OUTPUT_REPLICATION_OPT.put(null, opts, "5");

    ccs.init(initParams);

    plan = ccs.override(inputParams);

    Map<String,
        String> expected = Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "snappy",
            Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), getFixedMemoryAsBytes("64K") + "",
            Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(),
            getFixedMemoryAsBytes("32K") + "", Property.TABLE_FILE_BLOCK_SIZE.getKey(),
            getFixedMemoryAsBytes("256M") + "", Property.TABLE_FILE_REPLICATION.getKey(), "5");

    assertEquals(expected, plan.getOverrides());

  }
}

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
package org.apache.accumulo.core.compaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.conf.Property;

/**
 * The compaction configurer is used by the shell compact command. It exists in accumulo-core, so it
 * is on the class path for the shell and servers that run compactions.
 */
public class ShellCompactCommandConfigurer implements CompactionConfigurer {

  private Map<String,String> overrides = new HashMap<>();

  @Override
  public void init(InitParameters iparams) {
    Set<Entry<String,String>> es = iparams.getOptions().entrySet();
    for (Entry<String,String> entry : es) {

      switch (CompactionSettings.valueOf(entry.getKey())) {
        case OUTPUT_COMPRESSION_OPT:
          overrides.put(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), entry.getValue());
          break;
        case OUTPUT_BLOCK_SIZE_OPT:
          overrides.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), entry.getValue());
          break;
        case OUTPUT_INDEX_BLOCK_SIZE_OPT:
          overrides.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), entry.getValue());
          break;
        case OUTPUT_HDFS_BLOCK_SIZE_OPT:
          overrides.put(Property.TABLE_FILE_BLOCK_SIZE.getKey(), entry.getValue());
          break;
        case OUTPUT_REPLICATION_OPT:
          overrides.put(Property.TABLE_FILE_REPLICATION.getKey(), entry.getValue());
          break;
        default:
          throw new IllegalArgumentException("Unknown option " + entry.getKey());
      }
    }
  }

  @Override
  public Overrides override(InputParameters params) {
    return new Overrides(overrides);
  }

}

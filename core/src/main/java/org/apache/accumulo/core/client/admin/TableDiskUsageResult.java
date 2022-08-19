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
package org.apache.accumulo.core.client.admin;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.TableId;

import com.google.common.collect.ImmutableMap;

/**
 * Class for returning the disk usage results computed by
 * {@link org.apache.accumulo.core.util.tables.MetadataTableDiskUsage}
 */
public class TableDiskUsageResult {

  // Track the total disk usage across all files for the Table
  private final Map<TableId,AtomicLong> tableUsages;

  // Track the total disk usage across a volume for each Table
  private final Map<TableId,Map<String,AtomicLong>> volumeUsages;

  // This tracks disk usage shared across a set of tables
  private final List<DiskUsage> sharedDiskUsages;

  // This tracks any shared tables (tables that have shared files)
  private final Map<TableId,Set<TableId>> sharedTables;

  public TableDiskUsageResult(final Map<TableId,AtomicLong> tableUsages,
      final Map<TableId,Map<String,AtomicLong>> volumeUsages,
      final SortedMap<SortedSet<String>,Long> usageMap,
      final Map<TableId,Set<TableId>> referencedTables) {

    this.tableUsages = ImmutableMap.copyOf(tableUsages);
    this.volumeUsages = ImmutableMap.copyOf(volumeUsages);
    this.sharedTables = ImmutableMap.copyOf(referencedTables);
    this.sharedDiskUsages =
        usageMap.entrySet().stream().map(usage -> new DiskUsage(usage.getKey(), usage.getValue()))
            .collect(Collectors.toUnmodifiableList());
  }

  /**
   * @return total disk usage across all files for the Table
   */
  public Map<TableId,AtomicLong> getTableUsages() {
    return tableUsages;
  }

  /**
   * @return total disk usage across a volume for each Table
   */
  public Map<TableId,Map<String,AtomicLong>> getVolumeUsages() {
    return volumeUsages;
  }

  /**
   * @return disk usage shared across a set of tables
   */
  public List<DiskUsage> getSharedDiskUsages() {
    return sharedDiskUsages;
  }

  /**
   * @return shared tables (tables that have shared files)
   */
  public Map<TableId,Set<TableId>> getSharedTables() {
    return sharedTables;
  }
}

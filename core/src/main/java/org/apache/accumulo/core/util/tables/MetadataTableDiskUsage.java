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
package org.apache.accumulo.core.util.tables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableDiskUsageResult;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.fs.Path;

/**
 * This utility class will scan the Accumulo Metadata table to compute the disk usage for a table or
 * table(s) by using the size value stored in columns that contain the column family
 * {@link MetadataSchema.TabletsSection.DataFileColumnFamily}.
 *
 * This class will also track shared files and usage across all tables that are provided as part of
 * the Set of tables when getting disk usage.
 */
public class MetadataTableDiskUsage extends TableDiskUsage {

  public MetadataTableDiskUsage(final Set<TableId> tableIds) {
    // Add each tableID
    Objects.requireNonNull(tableIds).forEach(tableId -> addTable(tableId));
  }

  public static TableDiskUsageResult getDiskUsage(Set<String> tableNames, AccumuloClient client,
      Authorizations auths) throws TableNotFoundException {
    return getDiskUsage(tableNames, true, client, auths);
  }

  public static TableDiskUsageResult getDiskUsage(Set<String> tableNames, boolean computeShared,
      AccumuloClient client, Authorizations auths) throws TableNotFoundException {
    final Set<TableId> tableIds = new HashSet<>();

    // Get table Ids for all tables requested to be 'du'
    for (String tableName : tableNames) {
      tableIds.add(Optional.ofNullable(((ClientContext) client).getTableId(tableName)).orElseThrow(
          () -> new TableNotFoundException(null, tableName, "Table " + tableName + " not found")));
    }

    final MetadataTableDiskUsage tdu = new MetadataTableDiskUsage(tableIds);
    // This tracks any shared tables (tables that have shared files)
    final Map<TableId,Set<TableId>> sharedTables = new HashMap<>();
    // Track the total disk usage across all files for the Table
    final Map<TableId,AtomicLong> tableUsages = new HashMap<>();
    // Track the total disk usage across a volume for the Table
    final Map<TableId,Map<String,AtomicLong>> volumeUsages = new HashMap<>();

    // For each table ID
    for (TableId tableId : tableIds) {
      // if the table to compute usage is for the metadata table itself then we need to scan the
      // root table, else we scan the metadata table
      try (Scanner mdScanner =
          tableId.equals(MetadataTable.ID) ? client.createScanner(RootTable.NAME, auths)
              : client.createScanner(MetadataTable.NAME, auths)) {
        mdScanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
        mdScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());

        // initialize the table to a size of 0, so we don't miss any tables with no usage
        tableUsages.put(tableId, new AtomicLong(0));
        final Set<TabletFile> files = new HashSet<>();

        // Read each file referenced by that table
        for (Map.Entry<Key,Value> entry : mdScanner) {
          final TabletFile file =
              new TabletFile(new Path(entry.getKey().getColumnQualifier().toString()));

          // get the table referenced by the file which may not be the same as the current
          // table we are scanning if the file is shared between multiple tables
          final TableId fileTableRef = file.getTableId();

          // if this is a ref to a different table then we need to make sure the shared file is
          // tracked property
          if (!fileTableRef.equals(tableId)) {
            // if table not already added we need to track it
            tdu.addTableIfAbsent(fileTableRef);

            // track that the table is shared
            sharedTables.computeIfAbsent(tableId, tid -> new HashSet<>()).add(fileTableRef);

            // link the table and the shared file for computing shared sizes
            if (computeShared) {
              tdu.linkFileAndTable(fileTableRef, file.getFileName());
            }
          }

          // link the file to the table we are scanning for (even if a shared file)
          if (computeShared) {
            tdu.linkFileAndTable(tableId, file.getFileName());
          }

          // add the file size for the table if not already seen for this scan
          // it's possible the file was already seen/referenced before for bulk imports, etc
          if (files.add(file)) {
            final long fileUsage = new DataFileValue(entry.getValue().get()).getSize();
            // This tracks the file size for individual files for computing shared file statistics
            // later
            tdu.addFileSize(file.getFileName(), fileUsage);
            // Add the file size to the total size for the table being scanned
            tableUsages.computeIfAbsent(tableId, tid -> new AtomicLong()).addAndGet(fileUsage);
            // Also add the file size to the volume this file is on
            volumeUsages.computeIfAbsent(tableId, tid -> new HashMap<>())
                .computeIfAbsent(file.getVolume(), volume -> new AtomicLong()).addAndGet(fileUsage);
          }
        }
      }
    }

    return new TableDiskUsageResult(tableUsages, volumeUsages,
        buildSharedUsageMap(tdu, ((ClientContext) client), getEmptyTables(tableUsages)),
        sharedTables);
  }

  private static Set<TableId> getEmptyTables(final Map<TableId,AtomicLong> tableUsages) {
    return tableUsages.entrySet().stream().filter(entry -> entry.getValue().get() == 0l)
        .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

}

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
package org.apache.accumulo.server.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
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
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/**
 * This utility class will scan the Accumulo Metadata table to compute the disk usage for a table or
 * table(s) by using the size value stored in columns that contain the column family
 * {@link org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily}.
 *
 * This class will also track shared files to computed shared usage across all tables that are
 * provided as part of the Set of tables when getting disk usage.
 *
 * Because the metadata table is used for computing usage and not the actual files in HDFS the
 * results will be an estimate. Older entries may exist with no file metadata (resulting in size 0)
 * and other actions in the cluster can impact the estimated size such as flushes, tablet splits,
 * compactions, etc.
 *
 * For more accurate information a compaction should first be run on all files for the set of tables
 * being computed.
 */
public class TableDiskUsage {

  private static final Logger log = LoggerFactory.getLogger(TableDiskUsage.class);
  private int nextInternalId = 0;
  private Map<TableId,Integer> internalIds = new HashMap<>();
  private Map<Integer,TableId> externalIds = new HashMap<>();
  private Map<String,Integer[]> tableFiles = new HashMap<>();
  private Map<String,Long> fileSizes = new HashMap<>();

  void addTable(TableId tableId) {
    if (internalIds.containsKey(tableId)) {
      throw new IllegalArgumentException("Already added table " + tableId);
    }

    // Keep an internal counter for each table added
    int iid = nextInternalId++;

    // Store the table id to the internal id
    internalIds.put(tableId, iid);
    // Store the internal id to the table id
    externalIds.put(iid, tableId);
  }

  void linkFileAndTable(TableId tableId, String file) {
    // get the internal id for this table
    int internalId = internalIds.get(tableId);

    // Initialize a bitset for tables (internal IDs) that reference this file
    Integer[] tables = tableFiles.get(file);
    if (tables == null) {
      tables = new Integer[internalIds.size()];
      for (int i = 0; i < tables.length; i++) {
        tables[i] = 0;
      }
      tableFiles.put(file, tables);
    }

    // Update the bitset to track that this table has seen this file
    tables[internalId] = 1;
  }

  void addFileSize(String file, long size) {
    fileSizes.put(file, size);
  }

  Map<List<TableId>,Long> calculateUsage() {

    // Bitset of tables that contain a file and total usage by all files that share that usage
    Map<List<Integer>,Long> usage = new HashMap<>();

    if (log.isTraceEnabled()) {
      log.trace("fileSizes {}", fileSizes);
    }
    // For each file w/ referenced-table bitset
    for (Entry<String,Integer[]> entry : tableFiles.entrySet()) {
      if (log.isTraceEnabled()) {
        log.trace("file {} table bitset {}", entry.getKey(), Arrays.toString(entry.getValue()));
      }
      List<Integer> key = Arrays.asList(entry.getValue());
      Long size = fileSizes.get(entry.getKey());

      Long tablesUsage = usage.get(key);
      if (tablesUsage == null) {
        tablesUsage = 0L;
      }

      tablesUsage += size;

      usage.put(key, tablesUsage);

    }

    Map<List<TableId>,Long> externalUsage = new HashMap<>();

    for (Entry<List<Integer>,Long> entry : usage.entrySet()) {
      List<TableId> externalKey = new ArrayList<>();
      List<Integer> key = entry.getKey();
      // table bitset
      for (int i = 0; i < key.size(); i++) {
        if (key.get(i) != 0) {
          // Convert by internal id to the table id
          externalKey.add(externalIds.get(i));
        }
      }

      // list of table ids and size of files shared across the tables
      externalUsage.put(externalKey, entry.getValue());
    }

    // mapping of all enumerations of files being referenced by tables and total size of files who
    // share the same reference
    return externalUsage;
  }

  public interface Printer {
    void print(String line);
  }

  public static void printDiskUsage(Collection<String> tableNames, AccumuloClient client,
      boolean humanReadable) throws TableNotFoundException, IOException {
    printDiskUsage(tableNames, client, System.out::println, humanReadable);
  }

  /**
   * Compute the estimated disk usage for the given set of tables by scanning the Metadata table for
   * file sizes. Optionally computes shared usage across tables.
   *
   * @param tableIds set of tables to compute an estimated disk usage for
   * @param client accumulo client used to scan
   * @return the computed estimated usage results
   *
   * @throws TableNotFoundException if the table(s) do not exist
   */
  public static Map<SortedSet<String>,Long> getDiskUsage(Set<TableId> tableIds,
      AccumuloClient client) throws TableNotFoundException {
    final TableDiskUsage tdu = new TableDiskUsage();

    // Add each tableID
    for (TableId tableId : tableIds) {
      tdu.addTable(tableId);
    }

    HashSet<TableId> emptyTableIds = new HashSet<>();

    // For each table ID
    for (TableId tableId : tableIds) {
      // if the table to compute usage is for the metadata table itself then we need to scan the
      // root table, else we scan the metadata table
      try (Scanner mdScanner = tableId.equals(MetadataTable.ID)
          ? client.createScanner(RootTable.NAME, Authorizations.EMPTY)
          : client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        mdScanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
        mdScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());

        final Set<TabletFile> files = new HashSet<>();

        // Read each file referenced by that table
        for (Map.Entry<Key,Value> entry : mdScanner) {
          final TabletFile file =
              new TabletFile(new Path(entry.getKey().getColumnQualifier().toString()));

          // get the table referenced by the file which may not be the same as the current
          // table we are scanning if the file is shared between multiple tables
          final TableId fileTableRef = file.getTableId();

          // if this is a ref to a different table than the one we are scanning then we need
          // to make sure the table is also linked for this shared file if the table is
          // part of the set of tables we are running du on so we can track shared usages
          if (!fileTableRef.equals(tableId) && tableIds.contains(fileTableRef)) {
            // link the table and the shared file for computing shared sizes
            tdu.linkFileAndTable(fileTableRef, file.getFileName());
          }

          // link the file to the table we are scanning for
          tdu.linkFileAndTable(tableId, file.getFileName());

          // add the file size for the table if not already seen for this scan
          if (files.add(file)) {
            // This tracks the file size for individual files for computing shared file statistics
            // later
            tdu.addFileSize(file.getFileName(),
                new DataFileValue(entry.getValue().get()).getSize());
          }
        }

        // Track tables that are empty with no metadata
        if (files.isEmpty()) {
          emptyTableIds.add(tableId);
        }
      }
    }

    return buildSharedUsageMap(tdu, ((ClientContext) client), emptyTableIds);
  }

  protected static Map<SortedSet<String>,Long> buildSharedUsageMap(final TableDiskUsage tdu,
      final ClientContext clientContext, final Set<TableId> emptyTableIds) {
    final Map<TableId,String> reverseTableIdMap = clientContext.getTableIdToNameMap();

    SortedMap<SortedSet<String>,Long> usage = new TreeMap<>((o1, o2) -> {
      int len1 = o1.size();
      int len2 = o2.size();

      int min = Math.min(len1, len2);

      Iterator<String> iter1 = o1.iterator();
      Iterator<String> iter2 = o2.iterator();

      int count = 0;

      while (count < min) {
        String s1 = iter1.next();
        String s2 = iter2.next();

        int cmp = s1.compareTo(s2);

        if (cmp != 0) {
          return cmp;
        }

        count++;
      }

      return len1 - len2;
    });

    for (Entry<List<TableId>,Long> entry : tdu.calculateUsage().entrySet()) {
      TreeSet<String> tableNames = new TreeSet<>();
      // Convert size shared by each table id into size shared by each table name
      for (TableId tableId : entry.getKey()) {
        tableNames.add(reverseTableIdMap.get(tableId));
      }

      // Make table names to shared file size
      usage.put(tableNames, entry.getValue());
    }

    if (!emptyTableIds.isEmpty()) {
      TreeSet<String> emptyTables = new TreeSet<>();
      for (TableId tableId : emptyTableIds) {
        emptyTables.add(reverseTableIdMap.get(tableId));
      }
      usage.put(emptyTables, 0L);
    }

    return usage;
  }

  public static void printDiskUsage(Collection<String> tableNames, AccumuloClient client,
      Printer printer, boolean humanReadable) throws TableNotFoundException, IOException {

    HashSet<TableId> tableIds = new HashSet<>();

    // Get table IDs for all tables requested to be 'du'
    for (String tableName : tableNames) {
      TableId tableId = ((ClientContext) client).getTableId(tableName);
      if (tableId == null) {
        throw new TableNotFoundException(null, tableName, "Table " + tableName + " not found");
      }

      tableIds.add(tableId);
    }

    Map<SortedSet<String>,Long> usage = getDiskUsage(tableIds, client);

    String valueFormat = humanReadable ? "%9s" : "%,24d";
    for (Entry<SortedSet<String>,Long> entry : usage.entrySet()) {
      Object value = humanReadable ? NumUtil.bigNumberForSize(entry.getValue()) : entry.getValue();
      printer.print(String.format(valueFormat + " %s", value, entry.getKey()));
    }
  }

  static class Opts extends ServerUtilOpts {
    @Parameter(description = " <table> { <table> ... } ")
    List<String> tables = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(TableDiskUsage.class.getName(), args);
    Span span = TraceUtil.startSpan(TableDiskUsage.class, "main");
    try (Scope scope = span.makeCurrent()) {
      try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
        org.apache.accumulo.server.util.TableDiskUsage.printDiskUsage(opts.tables, client, false);
      } finally {
        span.end();
      }
    }
  }

}

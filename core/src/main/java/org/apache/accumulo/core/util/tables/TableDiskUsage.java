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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableDiskUsage {

  private static final Logger log = LoggerFactory.getLogger(TableDiskUsage.class);
  private int nextInternalId = 0;
  private Map<TableId,Integer> internalIds = new HashMap<>();
  private Map<Integer,TableId> externalIds = new HashMap<>();
  private Map<String,Integer[]> tableFiles = new HashMap<>();
  private Map<String,Long> fileSizes = new HashMap<>();

  protected void addTableIfAbsent(TableId tableId) {
    if (!internalIds.containsKey(tableId)) {
      addTable(tableId);
    }
  }

  protected void addTable(TableId tableId) {
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

  protected void linkFileAndTable(TableId tableId, String file) {
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

  protected void addFileSize(String file, long size) {
    fileSizes.put(file, size);
  }

  protected Map<List<TableId>,Long> calculateSharedUsage() {
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

      Long tablesUsage = usage.getOrDefault(key, 0L);
      tablesUsage += size;
      usage.put(key, tablesUsage);
    }

    final Map<List<TableId>,Long> externalUsage = new HashMap<>();

    for (Entry<List<Integer>,Long> entry : usage.entrySet()) {
      List<TableId> externalKey = new ArrayList<>();
      List<Integer> key = entry.getKey();
      // table bitset
      for (int i = 0; i < key.size(); i++)
        if (key.get(i) != 0) {
          // Convert by internal id to the table id
          externalKey.add(externalIds.get(i));
        }

      // list of table ids and size of files shared across the tables
      externalUsage.put(externalKey, entry.getValue());
    }

    // mapping of all enumerations of files being referenced by tables and total size of files who
    // share the same reference
    return externalUsage;
  }

  protected static SortedMap<SortedSet<String>,Long> buildSharedUsageMap(final TableDiskUsage tdu,
      final ClientContext clientContext, final Set<TableId> emptyTableIds) {
    final Map<TableId,String> reverseTableIdMap = clientContext.getTableIdToNameMap();

    final SortedMap<SortedSet<String>,Long> usage = new TreeMap<>((o1, o2) -> {
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

    for (Entry<List<TableId>,Long> entry : tdu.calculateSharedUsage().entrySet()) {
      final SortedSet<String> tableNames = new TreeSet<>();
      // Convert size shared by each table id into size shared by each table name
      for (TableId tableId : entry.getKey()) {
        tableNames.add(reverseTableIdMap.get(tableId));
      }

      // Make table names to shared file size
      usage.put(tableNames, entry.getValue());
    }

    if (!emptyTableIds.isEmpty()) {
      final SortedSet<String> emptyTables = new TreeSet<>();
      for (TableId tableId : emptyTableIds) {
        emptyTables.add(reverseTableIdMap.get(tableId));
      }
      usage.put(emptyTables, 0L);
    }

    return usage;
  }
}

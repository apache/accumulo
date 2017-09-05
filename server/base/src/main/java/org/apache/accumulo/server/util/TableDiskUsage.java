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
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.base.Joiner;

public class TableDiskUsage {

  private static final Logger log = LoggerFactory.getLogger(TableDiskUsage.class);
  private int nextInternalId = 0;
  private Map<Table.ID,Integer> internalIds = new HashMap<>();
  private Map<Integer,Table.ID> externalIds = new HashMap<>();
  private Map<String,Integer[]> tableFiles = new HashMap<>();
  private Map<String,Long> fileSizes = new HashMap<>();

  void addTable(Table.ID tableId) {
    if (internalIds.containsKey(tableId))
      throw new IllegalArgumentException("Already added table " + tableId);

    // Keep an internal counter for each table added
    int iid = nextInternalId++;

    // Store the table id to the internal id
    internalIds.put(tableId, iid);
    // Store the internal id to the table id
    externalIds.put(iid, tableId);
  }

  void linkFileAndTable(Table.ID tableId, String file) {
    // get the internal id for this table
    int internalId = internalIds.get(tableId);

    // Initialize a bitset for tables (internal IDs) that reference this file
    Integer[] tables = tableFiles.get(file);
    if (tables == null) {
      tables = new Integer[internalIds.size()];
      for (int i = 0; i < tables.length; i++)
        tables[i] = 0;
      tableFiles.put(file, tables);
    }

    // Update the bitset to track that this table has seen this file
    tables[internalId] = 1;
  }

  void addFileSize(String file, long size) {
    fileSizes.put(file, size);
  }

  Map<List<Table.ID>,Long> calculateUsage() {

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
      if (tablesUsage == null)
        tablesUsage = 0l;

      tablesUsage += size;

      usage.put(key, tablesUsage);

    }

    Map<List<Table.ID>,Long> externalUsage = new HashMap<>();

    for (Entry<List<Integer>,Long> entry : usage.entrySet()) {
      List<Table.ID> externalKey = new ArrayList<>();
      List<Integer> key = entry.getKey();
      // table bitset
      for (int i = 0; i < key.size(); i++)
        if (key.get(i) != 0)
          // Convert by internal id to the table id
          externalKey.add(externalIds.get(i));

      // list of table ids and size of files shared across the tables
      externalUsage.put(externalKey, entry.getValue());
    }

    // mapping of all enumerations of files being referenced by tables and total size of files who share the same reference
    return externalUsage;
  }

  public interface Printer {
    void print(String line);
  }

  public static void printDiskUsage(Collection<String> tableNames, VolumeManager fs, Connector conn, boolean humanReadable) throws TableNotFoundException,
      IOException {
    printDiskUsage(tableNames, fs, conn, line -> System.out.println(line), humanReadable);
  }

  public static Map<TreeSet<String>,Long> getDiskUsage(Set<Table.ID> tableIds, VolumeManager fs, Connector conn) throws IOException {
    TableDiskUsage tdu = new TableDiskUsage();

    // Add each tableID
    for (Table.ID tableId : tableIds)
      tdu.addTable(tableId);

    HashSet<Table.ID> tablesReferenced = new HashSet<>(tableIds);
    HashSet<Table.ID> emptyTableIds = new HashSet<>();
    HashSet<String> nameSpacesReferenced = new HashSet<>();

    // For each table ID
    for (Table.ID tableId : tableIds) {
      Scanner mdScanner;
      try {
        mdScanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      mdScanner.setRange(new KeyExtent(tableId, null, null).toMetadataRange());

      if (!mdScanner.iterator().hasNext()) {
        emptyTableIds.add(tableId);
      }

      // Read each file referenced by that table
      for (Entry<Key,Value> entry : mdScanner) {
        String file = entry.getKey().getColumnQualifier().toString();
        String parts[] = file.split("/");
        // the filename
        String uniqueName = parts[parts.length - 1];
        if (file.contains(":") || file.startsWith("../")) {
          String ref = parts[parts.length - 3];
          // Track any tables which are referenced externally by the current table
          if (!ref.equals(tableId.canonicalID())) {
            tablesReferenced.add(Table.ID.of(ref));
          }
          if (file.contains(":") && parts.length > 3) {
            List<String> base = Arrays.asList(Arrays.copyOf(parts, parts.length - 3));
            nameSpacesReferenced.add(Joiner.on("/").join(base));
          }
        }

        // add this file to this table
        tdu.linkFileAndTable(tableId, uniqueName);
      }
    }

    // Each table seen (provided by user, or reference by table the user provided)
    for (Table.ID tableId : tablesReferenced) {
      for (String tableDir : nameSpacesReferenced) {
        // Find each file and add its size
        FileStatus[] files = fs.globStatus(new Path(tableDir + "/" + tableId + "/*/*"));
        if (files != null) {
          for (FileStatus fileStatus : files) {
            // Assumes that all filenames are unique
            String name = fileStatus.getPath().getName();
            tdu.addFileSize(name, fileStatus.getLen());
          }
        }
      }
    }

    Map<Table.ID,String> reverseTableIdMap = Tables.getIdToNameMap(conn.getInstance());

    TreeMap<TreeSet<String>,Long> usage = new TreeMap<>((o1, o2) -> {
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

        if (cmp != 0)
          return cmp;

        count++;
      }

      return len1 - len2;
    });

    for (Entry<List<Table.ID>,Long> entry : tdu.calculateUsage().entrySet()) {
      TreeSet<String> tableNames = new TreeSet<>();
      // Convert size shared by each table id into size shared by each table name
      for (Table.ID tableId : entry.getKey())
        tableNames.add(reverseTableIdMap.get(tableId));

      // Make table names to shared file size
      usage.put(tableNames, entry.getValue());
    }

    if (!emptyTableIds.isEmpty()) {
      TreeSet<String> emptyTables = new TreeSet<>();
      for (Table.ID tableId : emptyTableIds) {
        emptyTables.add(reverseTableIdMap.get(tableId));
      }
      usage.put(emptyTables, 0L);
    }

    return usage;
  }

  public static void printDiskUsage(Collection<String> tableNames, VolumeManager fs, Connector conn, Printer printer, boolean humanReadable)
      throws TableNotFoundException, IOException {

    HashSet<Table.ID> tableIds = new HashSet<>();

    // Get table IDs for all tables requested to be 'du'
    for (String tableName : tableNames) {
      Table.ID tableId = Tables.getTableId(conn.getInstance(), tableName);
      if (tableId == null)
        throw new TableNotFoundException(null, tableName, "Table " + tableName + " not found");

      tableIds.add(tableId);
    }

    Map<TreeSet<String>,Long> usage = getDiskUsage(tableIds, fs, conn);

    String valueFormat = humanReadable ? "%9s" : "%,24d";
    for (Entry<TreeSet<String>,Long> entry : usage.entrySet()) {
      Object value = humanReadable ? NumUtil.bigNumberForSize(entry.getValue()) : entry.getValue();
      printer.print(String.format(valueFormat + " %s", value, entry.getKey()));
    }
  }

  static class Opts extends ClientOpts {
    @Parameter(description = " <table> { <table> ... } ")
    List<String> tables = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    VolumeManager fs = VolumeManagerImpl.get();
    Opts opts = new Opts();
    opts.parseArgs(TableDiskUsage.class.getName(), args);
    Connector conn = opts.getConnector();
    org.apache.accumulo.server.util.TableDiskUsage.printDiskUsage(opts.tables, fs, conn, false);
  }

}

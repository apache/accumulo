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
import java.util.Comparator;
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
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class TableDiskUsage {

  private static final Logger log = Logger.getLogger(Logger.class);
  private int nextInternalId = 0;
  private Map<String,Integer> internalIds = new HashMap<String,Integer>();
  private Map<Integer,String> externalIds = new HashMap<Integer,String>();
  private Map<String,Integer[]> tableFiles = new HashMap<String,Integer[]>();
  private Map<String,Long> fileSizes = new HashMap<String,Long>();

  void addTable(String tableId) {
    if (internalIds.containsKey(tableId))
      throw new IllegalArgumentException("Already added table " + tableId);

    int iid = nextInternalId++;

    internalIds.put(tableId, iid);
    externalIds.put(iid, tableId);
  }

  void linkFileAndTable(String tableId, String file) {
    int internalId = internalIds.get(tableId);

    Integer[] tables = tableFiles.get(file);
    if (tables == null) {
      tables = new Integer[internalIds.size()];
      for (int i = 0; i < tables.length; i++)
        tables[i] = 0;
      tableFiles.put(file, tables);
    }

    tables[internalId] = 1;
  }

  void addFileSize(String file, long size) {
    fileSizes.put(file, size);
  }

  Map<List<String>,Long> calculateUsage() {

    Map<List<Integer>,Long> usage = new HashMap<List<Integer>,Long>();

    for (Entry<String,Integer[]> entry : tableFiles.entrySet()) {
      if (log.isTraceEnabled()) {
        log.trace("fileSizes " + fileSizes + " key " + entry.getKey());
      }
      List<Integer> key = Arrays.asList(entry.getValue());
      Long size = fileSizes.get(entry.getKey());

      Long tablesUsage = usage.get(key);
      if (tablesUsage == null)
        tablesUsage = 0l;

      tablesUsage += size;

      usage.put(key, tablesUsage);

    }

    Map<List<String>,Long> externalUsage = new HashMap<List<String>,Long>();

    for (Entry<List<Integer>,Long> entry : usage.entrySet()) {
      List<String> externalKey = new ArrayList<String>();
      List<Integer> key = entry.getKey();
      for (int i = 0; i < key.size(); i++)
        if (key.get(i) != 0)
          externalKey.add(externalIds.get(i));

      externalUsage.put(externalKey, entry.getValue());
    }

    return externalUsage;
  }

  public interface Printer {
    void print(String line);
  }

  public static void printDiskUsage(AccumuloConfiguration acuConf, Collection<String> tables, VolumeManager fs, Connector conn, boolean humanReadable)
      throws TableNotFoundException, IOException {
    printDiskUsage(acuConf, tables, fs, conn, new Printer() {
      @Override
      public void print(String line) {
        System.out.println(line);
      }
    }, humanReadable);
  }

  public static Map<TreeSet<String>,Long> getDiskUsage(AccumuloConfiguration acuConf, Set<String> tableIds, VolumeManager fs, Connector conn)
      throws IOException {
    TableDiskUsage tdu = new TableDiskUsage();

    for (String tableId : tableIds)
      tdu.addTable(tableId);

    HashSet<String> tablesReferenced = new HashSet<String>(tableIds);
    HashSet<String> emptyTableIds = new HashSet<String>();
    HashSet<String> nameSpacesReferenced = new HashSet<String>();

    for (String tableId : tableIds) {
      Scanner mdScanner = null;
      try {
        mdScanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      mdScanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange());

      if (!mdScanner.iterator().hasNext()) {
        emptyTableIds.add(tableId);
      }

      for (Entry<Key,Value> entry : mdScanner) {
        String file = entry.getKey().getColumnQualifier().toString();
        String parts[] = file.split("/");
        String uniqueName = parts[parts.length - 1];
        if (file.contains(":") || file.startsWith("../")) {
          String ref = parts[parts.length - 3];
          if (!ref.equals(tableId)) {
            tablesReferenced.add(ref);
          }
          if (file.contains(":") && parts.length > 3) {
            List<String> base = Arrays.asList(Arrays.copyOf(parts, parts.length - 3));
            nameSpacesReferenced.add(StringUtil.join(base, "/"));
          }
        }

        tdu.linkFileAndTable(tableId, uniqueName);
      }
    }

    for (String tableId : tablesReferenced) {
      for (String tableDir : nameSpacesReferenced) {
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

    HashMap<String,String> reverseTableIdMap = new HashMap<String,String>();
    for (Entry<String,String> entry : conn.tableOperations().tableIdMap().entrySet())
      reverseTableIdMap.put(entry.getValue(), entry.getKey());

    TreeMap<TreeSet<String>,Long> usage = new TreeMap<TreeSet<String>,Long>(new Comparator<TreeSet<String>>() {

      @Override
      public int compare(TreeSet<String> o1, TreeSet<String> o2) {
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
      }
    });

    for (Entry<List<String>,Long> entry : tdu.calculateUsage().entrySet()) {
      TreeSet<String> tableNames = new TreeSet<String>();
      for (String tableId : entry.getKey())
        tableNames.add(reverseTableIdMap.get(tableId));

      usage.put(tableNames, entry.getValue());
    }

    if (!emptyTableIds.isEmpty()) {
      TreeSet<String> emptyTables = new TreeSet<String>();
      for (String tableId : emptyTableIds) {
        emptyTables.add(reverseTableIdMap.get(tableId));
      }
      usage.put(emptyTables, 0L);
    }

    return usage;
  }

  public static void printDiskUsage(AccumuloConfiguration acuConf, Collection<String> tables, VolumeManager fs, Connector conn, Printer printer,
      boolean humanReadable) throws TableNotFoundException, IOException {

    HashSet<String> tableIds = new HashSet<String>();

    for (String tableName : tables) {
      String tableId = conn.tableOperations().tableIdMap().get(tableName);
      if (tableId == null)
        throw new TableNotFoundException(null, tableName, "Table " + tableName + " not found");

      tableIds.add(tableId);
    }

    Map<TreeSet<String>,Long> usage = getDiskUsage(acuConf, tableIds, fs, conn);

    String valueFormat = humanReadable ? "%9s" : "%,24d";
    for (Entry<TreeSet<String>,Long> entry : usage.entrySet()) {
      Object value = humanReadable ? NumUtil.bigNumberForSize(entry.getValue()) : entry.getValue();
      printer.print(String.format(valueFormat + " %s", value, entry.getKey()));
    }
  }

  static class Opts extends ClientOpts {
    @Parameter(description = " <table> { <table> ... } ")
    List<String> tables = new ArrayList<String>();
  }

  public static void main(String[] args) throws Exception {
    VolumeManager fs = VolumeManagerImpl.get();
    Opts opts = new Opts();
    opts.parseArgs(TableDiskUsage.class.getName(), args);
    Connector conn = opts.getConnector();
    org.apache.accumulo.server.util.TableDiskUsage.printDiskUsage(DefaultConfiguration.getInstance(), opts.tables, fs, conn, false);
  }

}

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
package org.apache.accumulo.core.util;

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
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class TableDiskUsage {
  
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
  
  public static void printDiskUsage(AccumuloConfiguration acuConf, Collection<String> tables, FileSystem fs, Connector conn) throws TableNotFoundException,
  IOException {
    printDiskUsage(acuConf, tables, fs, conn, new Printer() {
      @Override
      public void print(String line) {
        System.out.println(line);
      }
    });
  }
  public static void printDiskUsage(AccumuloConfiguration acuConf, Collection<String> tables, FileSystem fs, Connector conn, Printer printer) throws TableNotFoundException,
  IOException {
  
    TableDiskUsage tdu = new TableDiskUsage();
    
    HashSet<String> tableIds = new HashSet<String>();
    
    for (String tableName : tables) {
      String tableId = conn.tableOperations().tableIdMap().get(tableName);
      if (tableId == null)
        throw new TableNotFoundException(null, tableName, "Table " + tableName + " not found");
      
      tableIds.add(tableId);
    }
    
    for (String tableId : tableIds)
      tdu.addTable(tableId);
    
    HashSet<String> tablesReferenced = new HashSet<String>(tableIds);
    HashSet<String> emptyTableIds = new HashSet<String>();
    
    for (String tableId : tableIds) {
      Scanner mdScanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
      mdScanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
      mdScanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange());
      
      if(!mdScanner.iterator().hasNext()) {
        emptyTableIds.add(tableId);
      }
      
      for (Entry<Key,Value> entry : mdScanner) {
        String file = entry.getKey().getColumnQualifier().toString();
        if (file.startsWith("../")) {
          file = file.substring(2);
          tablesReferenced.add(file.split("\\/")[1]);
        } else
          file = "/" + tableId + file;
        
        tdu.linkFileAndTable(tableId, file);
      }
    }
    
    for (String tableId : tablesReferenced) {
      FileStatus[] files = fs.globStatus(new Path(Constants.getTablesDir(acuConf) + "/" + tableId + "/*/*"));
      
      for (FileStatus fileStatus : files) {
        String dir = fileStatus.getPath().getParent().getName();
        String name = fileStatus.getPath().getName();
        
        tdu.addFileSize("/" + tableId + "/" + dir + "/" + name, fileStatus.getLen());
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

    if(!emptyTableIds.isEmpty()) {
      TreeSet<String> emptyTables = new TreeSet<String>();
      for (String tableId : emptyTableIds) {
        emptyTables.add(reverseTableIdMap.get(tableId));
      }
      usage.put(emptyTables, 0L);
    }
    
    for (Entry<TreeSet<String>,Long> entry : usage.entrySet())
      printer.print(String.format("%,24d %s", entry.getValue(), entry.getKey()));
    
  }
  
}

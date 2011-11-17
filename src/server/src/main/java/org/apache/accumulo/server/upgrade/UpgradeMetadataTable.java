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
package org.apache.accumulo.server.upgrade;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.MetadataTable.DataFileValue;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.tabletserver.TabletTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;

public class UpgradeMetadataTable {
  
  private static void processTabletFiles(Configuration conf, FileSystem fs, long maxTime, SortedMap<String,String> tableIds, TreeMap<Key,Value> tabletEntries,
      FileSKVWriter out) throws Exception {
    // determine tablet file and dirs
    
    HashMap<Text,ColumnFQ> colQualMap = new HashMap<Text,ColumnFQ>();
    colQualMap.put(new Text("~pr"), Constants.METADATA_PREV_ROW_COLUMN);
    colQualMap.put(new Text("dir"), Constants.METADATA_DIRECTORY_COLUMN);
    colQualMap.put(new Text("oldprevrow"), Constants.METADATA_OLD_PREV_ROW_COLUMN);
    colQualMap.put(new Text("splitRatio"), Constants.METADATA_SPLIT_RATIO_COLUMN);
    
    List<String> files = new ArrayList<String>();
    Value prevRow = null;
    
    for (Entry<Key,Value> entry : tabletEntries.entrySet()) {
      if (entry.getKey().getColumnFamily().toString().equals("file")) {
        files.add(entry.getKey().getColumnQualifier().toString());
      }
      
      if (entry.getKey().getColumnFamily().toString().equals("~tab") && entry.getKey().getColumnQualifier().toString().equals("~pr")) {
        prevRow = entry.getValue();
      }
    }
    
    if (files.size() == 0)
      throw new Exception("Tablet " + tabletEntries.lastKey().getRow() + " has no files");
    
    if (prevRow == null)
      throw new Exception("Tablet " + tabletEntries.lastKey().getRow() + " has no prevrow");
    
    KeyExtent ke = new KeyExtent(new Text(tabletEntries.lastKey().getRow()), prevRow);
    // System.out.println("*** Processing tablet "+tabletEntries.lastKey().getRow()+" "+prevRow+" "+files);
    
    ArrayList<SortedKeyValueIterator<Key,Value>> tabletIters = new ArrayList<SortedKeyValueIterator<Key,Value>>();
    for (String file : files) {
      String path = ServerConstants.getMetadataTableDir() + converFileName(file);
      FileSKVIterator in = FileOperations.getInstance().openReader(path, true, fs, conf, AccumuloConfiguration.getDefaultConfiguration());
      tabletIters.add(in);
    }
    
    MultiIterator multiIter = new MultiIterator(tabletIters, ke);
    DeletingIterator delIter = new DeletingIterator(multiIter, false);
    VersioningIterator tabletIter = new VersioningIterator(delIter, 1);
    tabletIter.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);
    
    TreeMap<Key,Value> tm = new TreeMap<Key,Value>();
    
    while (tabletIter.hasTop()) {
      
      Key key = tabletIter.getTopKey();
      Value val = tabletIter.getTopValue();
      
      // System.out.println("ORIG "+key+" "+val);
      
      try {
        if (key.getRow().getLength() > 0 && key.getRow().getBytes()[0] == '~') {
          flushRow(out, tm, maxTime);
          if (key.getRow().toString().startsWith("~del")) {
            if (!key.getRow().toString().startsWith("~del/!METADATA")) {
              String path = key.getRow().toString().substring(4);
              
              path = converFileName(path).toString();
              String[] tokens = path.split("/");
              tokens[1] = tableIds.get(tokens[1]);
              path = StringUtil.join(Arrays.asList(tokens), "/");
              
              Key newKey = new Key(new Text("~del" + path));
              newKey.setTimestamp(key.getTimestamp());
              out.append(newKey, val);
            }
          } else {
            out.append(key, val);
          }
        } else {
          Key newKey = translateKey(key, colQualMap, tableIds);
          
          if (newKey.getColumnFamily().equals(Constants.METADATA_DATAFILE_COLUMN_FAMILY) && !newKey.isDeleted()) {
            val = rencodeDatafileValue(val);
          } else if (Constants.METADATA_PREV_ROW_COLUMN.equals(newKey.getColumnFamily(), newKey.getColumnQualifier())) {
            Text per = KeyExtent.decodePrevEndRow(val);
            if (per == null)
              val = new Value(val.get(), true);
            else if (new KeyExtent(newKey.getRow(), (Text) null).getTableId().toString().equals(Constants.METADATA_TABLE_ID) && per.getBytes()[0] != '~')
              val = KeyExtent.encodePrevEndRow(translateRow(per, tableIds));
            else
              val = new Value(val.get(), true);
            
          } else {
            val = new Value(val.get(), true);
          }
          
          if (tm.size() > 0 && !tm.lastKey().getRow().equals(newKey.getRow())) {
            flushRow(out, tm, maxTime);
          }
          
          tm.put(newKey, val);
        }
      } catch (Exception e) {
        System.err.println("Failed to convert !METADATA entry " + key + " " + val);
        throw e;
      }
      
      tabletIter.next();
    }
    
    flushRow(out, tm, maxTime);
    
  }
  
  private static long determineMaxTime(Configuration conf, FileSystem fs) throws Exception {
    String inputDir = ServerConstants.getMetadataTableDir();
    
    FileStatus[] tablets = fs.listStatus(new Path(inputDir));
    
    long maxTime = 0;
    
    for (FileStatus tabletStatus : tablets) {
      FileStatus[] mapFiles = fs.listStatus(tabletStatus.getPath());
      for (FileStatus fileStatus : mapFiles) {
        
        if (!fileStatus.getPath().getName().matches("\\d+_\\d+\\.map")) {
          continue;
        }
        
        String input = fileStatus.getPath().toString();
        
        FileSKVIterator in = null;
        try {
          in = FileOperations.getInstance().openReader(input, true, fs, conf, AccumuloConfiguration.getDefaultConfiguration());
        } catch (EOFException ex) {
          continue;
        }
        
        while (in.hasTop()) {
          maxTime = Math.max(maxTime, in.getTopKey().getTimestamp());
          in.next();
          
        }
        
        in.close();
      }
    }
    
    return maxTime + 1;
  }
  
  public static void main(String[] args) throws Exception {
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    SortedMap<String,String> tableIds = Tables.getNameToIdMap(HdfsZooInstance.getInstance());
    
    long maxTime = determineMaxTime(conf, fs);
    
    String rootDir = ServerConstants.getMetadataTableDir() + "/root_tablet";
    FileStatus[] rootFiles = fs.listStatus(new Path(rootDir), new PathFilter() {
      public boolean accept(Path path) {
        return path.getName().matches("\\d+_\\d+\\.map");
      }
    });
    
    ArrayList<SortedKeyValueIterator<Key,Value>> rootFileIters = new ArrayList<SortedKeyValueIterator<Key,Value>>();
    for (FileStatus fileStatus : rootFiles) {
      FileSKVIterator in = FileOperations.getInstance().openReader(fileStatus.getPath().toString(), true, fs, conf,
          AccumuloConfiguration.getDefaultConfiguration());
      rootFileIters.add(in);
    }
    
    MultiIterator multiIter = new MultiIterator(rootFileIters, true);
    DeletingIterator delIter = new DeletingIterator(multiIter, false);
    VersioningIterator rootIter = new VersioningIterator(delIter, 1);
    
    TreeMap<Key,Value> files = new TreeMap<Key,Value>();
    
    fs.mkdirs(new Path(ServerConstants.getMetadataTableDir() + "_new" + "/default_tablet"));
    FileSKVWriter defaultTabletFile = FileOperations.getInstance().openWriter(
        ServerConstants.getMetadataTableDir() + "_new" + "/default_tablet/00000_00000.rf", fs, conf, AccumuloConfiguration.getDefaultConfiguration());
    defaultTabletFile.startDefaultLocalityGroup();
    
    while (rootIter.hasTop()) {
      Key key = rootIter.getTopKey();
      Value val = rootIter.getTopValue();
      
      // System.out.println("ROOT "+key+" "+val);
      
      if (key.getRow().toString().equals("!METADATA;!METADATA<")) {
        rootIter.next();
        continue;
      }
      
      if (files.size() > 0 && !files.lastKey().getRow().equals(key.getRow())) {
        processTabletFiles(conf, fs, maxTime, tableIds, files, defaultTabletFile);
        files.clear();
      }
      
      files.put(new Key(key), new Value(val));
      rootIter.next();
    }
    
    processTabletFiles(conf, fs, maxTime, tableIds, files, defaultTabletFile);
    
    defaultTabletFile.close();
    
    fs.mkdirs(new Path(ServerConstants.getMetadataTableDir() + "_new" + "/root_tablet"));
    FileSKVWriter rootTabletFile = FileOperations.getInstance().openWriter(ServerConstants.getMetadataTableDir() + "_new" + "/root_tablet/00000_00000.rf", fs,
        conf, AccumuloConfiguration.getDefaultConfiguration());
    rootTabletFile.startDefaultLocalityGroup();
    
    Text rootExtent = Constants.ROOT_TABLET_EXTENT.getMetadataEntry();
    
    // root's directory
    Key rootDirKey = new Key(rootExtent, Constants.METADATA_DIRECTORY_COLUMN.getColumnFamily(), Constants.METADATA_DIRECTORY_COLUMN.getColumnQualifier(), 0);
    rootTabletFile.append(rootDirKey, new Value("/root_tablet".getBytes()));
    
    // root's prev row
    Key rootPrevRowKey = new Key(rootExtent, Constants.METADATA_PREV_ROW_COLUMN.getColumnFamily(), Constants.METADATA_PREV_ROW_COLUMN.getColumnQualifier(), 0);
    rootTabletFile.append(rootPrevRowKey, new Value(new byte[] {0}));
    
    Text defaultExtent = new Text(KeyExtent.getMetadataEntry(new Text(Constants.METADATA_TABLE_ID), null));
    
    // default's file
    Key defaultFileKey = new Key(defaultExtent, Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text("/default_tablet/00000_00000.rf"), 0);
    rootTabletFile.append(defaultFileKey, new Value(new DataFileValue(0, 0).encode()));
    
    // default's directory
    Key defaultDirKey = new Key(defaultExtent, Constants.METADATA_DIRECTORY_COLUMN.getColumnFamily(), Constants.METADATA_DIRECTORY_COLUMN.getColumnQualifier(),
        0);
    rootTabletFile.append(defaultDirKey, new Value(Constants.DEFAULT_TABLET_LOCATION.getBytes()));
    
    // default's time
    Key defaultTimeKey = new Key(defaultExtent, Constants.METADATA_TIME_COLUMN.getColumnFamily(), Constants.METADATA_TIME_COLUMN.getColumnQualifier(), 0);
    rootTabletFile.append(defaultTimeKey, new Value((TabletTime.LOGICAL_TIME_ID + "" + maxTime).getBytes()));
    
    // default's prevrow
    Key defaultPrevRowKey = new Key(defaultExtent, Constants.METADATA_PREV_ROW_COLUMN.getColumnFamily(),
        Constants.METADATA_PREV_ROW_COLUMN.getColumnQualifier(), 0);
    rootTabletFile.append(defaultPrevRowKey, KeyExtent.encodePrevEndRow(new Text(KeyExtent.getMetadataEntry(new Text(Constants.METADATA_TABLE_ID), null))));
    
    rootTabletFile.close();
    
    fs.rename(new Path(ServerConstants.getMetadataTableDir()), new Path(ServerConstants.getMetadataTableDir() + "_old"));
    fs.rename(new Path(ServerConstants.getMetadataTableDir() + "_new"), new Path(ServerConstants.getMetadataTableDir()));
  }
  
  private static void flushRow(FileSKVWriter out, TreeMap<Key,Value> tm, long maxTime) throws IOException {
    if (tm.size() > 0) {
      Key logicalTimeKey = new Key(tm.lastKey().getRow(), Constants.METADATA_TIME_COLUMN.getColumnFamily(),
          Constants.METADATA_TIME_COLUMN.getColumnQualifier(), System.currentTimeMillis());
      Value logicalTimeValue;
      if (tm.lastKey().getRow().toString().startsWith("!0"))
        logicalTimeValue = new Value(("L" + maxTime).getBytes());
      else
        logicalTimeValue = new Value(("M" + System.currentTimeMillis()).getBytes());
      tm.put(logicalTimeKey, logicalTimeValue);
      
      for (Entry<Key,Value> entry : tm.entrySet()) {
        // System.out.println(entry.getKey()+" "+entry.getValue());
        out.append(entry.getKey(), entry.getValue());
      }
      
      tm.clear();
    }
  }
  
  private static Value rencodeDatafileValue(Value val) throws Exception {
    ByteArrayInputStream bais = new ByteArrayInputStream(val.get());
    DataInputStream dis = new DataInputStream(bais);
    
    long size = Long.parseLong(dis.readUTF());
    long numEntries = Long.parseLong(dis.readUTF());
    
    MetadataTable.DataFileValue dfv = new MetadataTable.DataFileValue(size, numEntries);
    return new Value(dfv.encode());
  }
  
  private static Key translateKey(Key key, HashMap<Text,ColumnFQ> colQualMap, SortedMap<String,String> tableIds) throws Exception {
    
    Text row = translateRow(key.getRow(), tableIds);
    
    if (key.getColumnFamily().equals(Constants.METADATA_LOG_COLUMN_FAMILY)) {
      Key newKey = new Key(row, key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp());
      newKey.setDeleted(key.isDeleted());
      return newKey;
    } else if (key.getColumnFamily().equals(Constants.METADATA_DATAFILE_COLUMN_FAMILY)) {
      
      Text cq = key.getColumnQualifier();
      Path p = converFileName(cq.toString());
      
      Key newKey = new Key(row, key.getColumnFamily(), new Text(p.toString()), key.getColumnVisibility(), key.getTimestamp());
      newKey.setDeleted(key.isDeleted());
      return newKey;
    } else if (key.getColumnFamily().equals(Constants.METADATA_TABLET_COLUMN_FAMILY) && colQualMap.containsKey(key.getColumnQualifier())) {
      ColumnFQ cfq = colQualMap.get(key.getColumnQualifier());
      Key newKey = new Key(row, cfq.getColumnFamily(), cfq.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp());
      newKey.setDeleted(key.isDeleted());
      return newKey;
    } else {
      throw new Exception("Unknown column " + key);
    }
  }
  
  private static Path converFileName(String cq) throws Exception {
    Path p = new Path(cq);
    String name = p.getName();
    
    if (!name.matches("map_\\d+_\\d+")) {
      throw new Exception("Unrecognized file name format " + name);
    }
    
    name = name.substring(4) + ".map";
    p = new Path(p.getParent(), name);
    return p;
  }
  
  private static Text translateRow(Text row, SortedMap<String,String> tableIds) {
    KeyExtent ke = new KeyExtent(row, (Text) null);
    Text er = ke.getEndRow();
    if (ke.getTableId().equals(new Text("!METADATA")) && er != null && er.getBytes()[0] != '~') {
      KeyExtent ke2 = new KeyExtent(er, (Text) null);
      String erTid = tableIds.get(ke2.getTableId().toString());
      er = new KeyExtent(new Text(erTid), ke2.getEndRow(), null).getMetadataEntry();
    }
    
    return new KeyExtent(new Text(tableIds.get(ke.getTableId().toString())), er, null).getMetadataEntry();
  }
}

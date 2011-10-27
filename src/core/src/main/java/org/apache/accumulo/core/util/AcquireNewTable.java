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
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * create a new default tablet for the metadata table
 * 
 * This assumes that all tables have only one mapfile - not really generally useful
 * 
 * 
 * 
 */
@SuppressWarnings("deprecation")
public class AcquireNewTable {
  private static final Logger log = Logger.getLogger(AcquireNewTable.class);
  
  public static void main(String[] args) throws IOException {
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] tables = fs.listStatus(new Path(Constants.getTablesDir()));
    
    Path default_tablet = new Path(Constants.getMetadataTableDir() + Constants.DEFAULT_TABLET_LOCATION);
    fs.delete(default_tablet, true);
    fs.mkdirs(default_tablet);
    
    MyMapFile.Writer mfw = new MyMapFile.Writer(conf, fs, Constants.getMetadataTableDir() + Constants.DEFAULT_TABLET_LOCATION + "/" + MyMapFile.EXTENSION
        + "_00000_00000", Key.class, Value.class);
    
    TreeMap<KeyExtent,Value> data = new TreeMap<KeyExtent,Value>();
    
    log.info("scanning filesystem for tables ...");
    for (FileStatus table : tables) {
      log.info("found " + table.getPath().getName());
      if (table.getPath().getName().equals(Constants.METADATA_TABLE_ID))
        continue;
      FileStatus[] tablets = fs.globStatus(new Path(table.getPath().toString() + "/*"));
      
      for (FileStatus tablet : tablets) {
        log.info("found tablet " + tablet.getPath().toUri().getPath());
        Key endKey = new Key();
        
        FileStatus[] mapfiles = fs.listStatus(tablet.getPath());
        
        // just take the first
        if (mapfiles.length == 0) {
          log.warn("no mapfiles in " + tablet.getPath().toString() + ". skipping ...");
          continue;
        }
        try {
          MyMapFile.Reader mfr = new MyMapFile.Reader(fs, mapfiles[0].getPath().toString(), conf);
          mfr.finalKey(endKey);
          mfr.close();
        } catch (IOException ioe) {
          log.error("exception opening mapfile " + mapfiles[0] + ": " + ioe.getMessage() + ". skipping ...");
          continue;
        }
        
        KeyExtent extent;
        
        if (endKey.getRow().toString().equals("tablet")) {
          extent = new KeyExtent(new Text(table.getPath().getName()), null, null);
        } else {
          extent = new KeyExtent(new Text(table.getPath().getName()), endKey.getRow(), null);
        }
        
        Value value = new Value(tablet.getPath().toUri().getPath().toString().getBytes());
        
        data.put(extent, value);
      }
    }
    
    log.info("Found " + data.size() + " non-metadata tablets.\n Writing metadata table");
    
    // note: don't use the prev end row from the extent
    Text prevEndRow = null;
    for (Entry<KeyExtent,Value> entry : data.entrySet()) {
      Text endRow = entry.getKey().getEndRow();
      
      Key key = new Key(new Text(KeyExtent.getMetadataEntry(entry.getKey().getTableId(), endRow)), Constants.METADATA_PREV_ROW_COLUMN.getColumnFamily(),
          Constants.METADATA_PREV_ROW_COLUMN.getColumnQualifier());
      
      byte[] b;
      if (prevEndRow == null) {
        b = new byte[1];
        b[0] = 0;
      } else {
        b = new byte[prevEndRow.toString().getBytes().length + 1];
        b[0] = 1;
        for (int i = 1; i < b.length; i++) {
          b[i] = prevEndRow.toString().getBytes()[i - 1];
        }
      }
      
      mfw.append(key, new Value(b));
      
      // write out the directory info
      key = new Key(new Text(KeyExtent.getMetadataEntry(entry.getKey().getTableId(), endRow)), Constants.METADATA_DIRECTORY_COLUMN.getColumnFamily(),
          Constants.METADATA_DIRECTORY_COLUMN.getColumnQualifier());
      mfw.append(key, entry.getValue());
      
      if (endRow == null)
        prevEndRow = null;
      else
        prevEndRow = new Text(endRow);
    }
    
    mfw.close();
    log.info("done.");
  }
}

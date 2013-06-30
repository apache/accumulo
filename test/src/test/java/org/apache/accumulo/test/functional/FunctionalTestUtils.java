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
package org.apache.accumulo.test.functional;

import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class FunctionalTestUtils {
  static void checkRFiles(Connector c, String tableName, int minTablets, int maxTablets, int minRFiles, int maxRFiles) throws Exception {
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    String tableId = c.tableOperations().tableIdMap().get(tableName);
    scanner.setRange(new Range(new Text(tableId + ";"), true, new Text(tableId + "<"), true));
    scanner.fetchColumnFamily(MetadataTable.DATAFILE_COLUMN_FAMILY);
    MetadataTable.PREV_ROW_COLUMN.fetch(scanner);
    
    HashMap<Text,Integer> tabletFileCounts = new HashMap<Text,Integer>();
    
    for (Entry<Key,Value> entry : scanner) {
      
      Text row = entry.getKey().getRow();
      
      Integer count = tabletFileCounts.get(row);
      if (count == null)
        count = 0;
      if (entry.getKey().getColumnFamily().equals(MetadataTable.DATAFILE_COLUMN_FAMILY)) {
        count = count + 1;
      }
      
      tabletFileCounts.put(row, count);
    }
    
    if (tabletFileCounts.size() < minTablets || tabletFileCounts.size() > maxTablets) {
      throw new Exception("Did not find expected number of tablets " + tabletFileCounts.size());
    }
    
    Set<Entry<Text,Integer>> es = tabletFileCounts.entrySet();
    for (Entry<Text,Integer> entry : es) {
      if (entry.getValue() > maxRFiles || entry.getValue() < minRFiles) {
        throw new Exception("tablet " + entry.getKey() + " has " + entry.getValue() + " map files");
      }
    }
  }
  
  static public void bulkImport(Connector c, FileSystem fs, String table, String dir) throws Exception {
    String failDir = dir + "_failures";
    Path failPath = new Path(failDir);
    fs.delete(failPath, true);
    fs.mkdirs(failPath);
    
   c.tableOperations().importDirectory(table, dir, failDir, false);
    
    if (fs.listStatus(failPath).length > 0) {
      throw new Exception("Some files failed to bulk import");
    }
    
  }
  

  
}

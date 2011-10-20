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

import java.io.EOFException;
import java.util.ArrayList;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class RunPreUpgradeCheck {
  
  static final String ZROOT_PATH = "/accumulo";
  static final String ZLOGS_PATH = "/root_tablet_logs";
  
  public static void main(String[] args) throws Exception {
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    Path dvLocation = ServerConstants.getDataVersionLocation();
    
    if (!fs.exists(new Path(dvLocation, "2"))) {
      System.err.println("Did not see expected accumulo data version");
      System.exit(-1);
    }
    
    String rootTabletWALOGS = ZROOT_PATH + "/" + HdfsZooInstance.getInstance().getInstanceID() + ZLOGS_PATH;
    ZooReaderWriter session = ZooReaderWriter.getInstance();
    
    if (session.exists(rootTabletWALOGS) && session.getChildren(rootTabletWALOGS).size() != 0) {
      System.err.println("The root tablet has write ahead logs");
      System.exit(-1);
    }
    
    String rootTabletDir = ServerConstants.getTablesDir() + "/!METADATA/root_tablet";
    
    FileStatus[] mapFiles = fs.listStatus(new Path(rootTabletDir));
    ArrayList<SortedKeyValueIterator<Key,Value>> inputs = new ArrayList<SortedKeyValueIterator<Key,Value>>();
    for (FileStatus fileStatus : mapFiles) {
      FileSKVIterator in = null;
      try {
        in = FileOperations.getInstance().openReader(fileStatus.getPath().toString(), true, fs, conf, AccumuloConfiguration.getDefaultConfiguration());
        inputs.add(in);
      } catch (EOFException ex) {
        System.out.println("Problem opening map file " + fileStatus.getPath().toString() + ", probably empty tmp file, skipping... ");
        continue;
      }
    }
    
    MultiIterator mi = new MultiIterator(inputs, true);
    DeletingIterator di = new DeletingIterator(mi, false);
    
    int count = 0, logCount = 0;
    
    while (di.hasTop()) {
      if (di.getTopKey().getColumnFamily().equals(new Text("log"))) {
        logCount++;
      }
      count++;
      di.next();
    }
    
    for (SortedKeyValueIterator<Key,Value> in : inputs) {
      ((FileSKVIterator) in).close();
    }
    
    if (count == 0) {
      System.err.println("Did not find any metadata entries");
      System.exit(-1);
    }
    
    if (logCount > 0) {
      System.err.println("The metadata table has write ahead logs");
      System.exit(-1);
    }
    
  }
}

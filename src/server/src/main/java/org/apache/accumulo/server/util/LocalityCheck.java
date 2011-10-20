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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalityCheck {
  
  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: " + LocalityCheck.class.getName() + " instance zookeepers username password");
      System.exit(1);
    }
    ZooKeeperInstance instance = new ZooKeeperInstance(args[0], args[1]);
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    Connector connector = instance.getConnector(args[2], args[3].getBytes());
    Scanner scanner = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
    scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
    scanner.setRange(Constants.METADATA_KEYSPACE);
    
    Map<String,Long> totalBlocks = new HashMap<String,Long>();
    Map<String,Long> localBlocks = new HashMap<String,Long>();
    ArrayList<String> files = new ArrayList<String>();
    
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      if (key.compareColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY) == 0) {
        String location = entry.getValue().toString();
        String[] parts = location.split(":");
        String host = parts[0];
        addBlocks(fs, host, files, totalBlocks, localBlocks);
        files.clear();
      } else if (key.compareColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY) == 0) {
        files.add(new String(KeyExtent.tableOfMetadataRow(key.getRow())) + key.getColumnQualifier().toString());
      }
    }
    System.out.println(" Server         %local  total blocks");
    for (String host : totalBlocks.keySet()) {
      System.out.println(String.format("%15s %5.1f %8d", host, (localBlocks.get(host) * 100.) / totalBlocks.get(host), totalBlocks.get(host)));
    }
    return 0;
  }
  
  private void addBlocks(FileSystem fs, String host, ArrayList<String> files, Map<String,Long> totalBlocks, Map<String,Long> localBlocks) throws Exception {
    long allBlocks = 0;
    long matchingBlocks = 0;
    if (!totalBlocks.containsKey(host)) {
      totalBlocks.put(host, 0L);
      localBlocks.put(host, 0L);
    }
    for (String file : files) {
      Path filePath = new Path(ServerConstants.getTablesDir() + "/" + file);
      FileStatus fileStatus = fs.getFileStatus(filePath);
      BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      for (BlockLocation blockLocation : fileBlockLocations) {
        allBlocks++;
        for (String location : blockLocation.getHosts()) {
          InetSocketAddress inetSocketAddress = new InetSocketAddress(location, 0);
          if (inetSocketAddress.getAddress().getHostAddress().equals(host)) {
            matchingBlocks++;
            break;
          }
        }
      }
    }
    totalBlocks.put(host, allBlocks + totalBlocks.get(host));
    localBlocks.put(host, matchingBlocks + localBlocks.get(host));
  }
  
  public static void main(String[] args) throws Exception {
    LocalityCheck check = new LocalityCheck();
    System.exit(check.run(args));
  }
}

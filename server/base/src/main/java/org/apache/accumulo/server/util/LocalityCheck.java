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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.net.HostAndPort;

public class LocalityCheck {

  public int run(String[] args) throws Exception {
    ClientOpts opts = new ClientOpts();
    opts.parseArgs(LocalityCheck.class.getName(), args);

    VolumeManager fs = VolumeManagerImpl.get();
    Connector connector = opts.getConnector();
    Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    scanner.setRange(MetadataSchema.TabletsSection.getRange());

    Map<String,Long> totalBlocks = new HashMap<String,Long>();
    Map<String,Long> localBlocks = new HashMap<String,Long>();
    ArrayList<String> files = new ArrayList<String>();

    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      if (key.compareColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME) == 0) {
        String location = entry.getValue().toString();
        String[] parts = location.split(":");
        String host = parts[0];
        addBlocks(fs, host, files, totalBlocks, localBlocks);
        files.clear();
      } else if (key.compareColumnFamily(DataFileColumnFamily.NAME) == 0) {

        files.add(fs.getFullPath(key).toString());
      }
    }
    System.out.println(" Server         %local  total blocks");
    for (Entry<String,Long> entry : totalBlocks.entrySet()) {
      final String host = entry.getKey();
      final Long blocksForHost = entry.getValue();
      System.out.println(String.format("%15s %5.1f %8d", host, (localBlocks.get(host) * 100.) / blocksForHost, blocksForHost));
    }
    return 0;
  }

  private void addBlocks(VolumeManager fs, String host, ArrayList<String> files, Map<String,Long> totalBlocks, Map<String,Long> localBlocks) throws Exception {
    long allBlocks = 0;
    long matchingBlocks = 0;
    if (!totalBlocks.containsKey(host)) {
      totalBlocks.put(host, 0L);
      localBlocks.put(host, 0L);
    }
    for (String file : files) {
      Path filePath = new Path(file);
      FileSystem ns = fs.getVolumeByPath(filePath).getFileSystem();
      FileStatus fileStatus = ns.getFileStatus(filePath);
      BlockLocation[] fileBlockLocations = ns.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      for (BlockLocation blockLocation : fileBlockLocations) {
        allBlocks++;
        for (String location : blockLocation.getHosts()) {
          HostAndPort hap = HostAndPort.fromParts(location, 0);
          if (hap.getHostText().equals(host)) {
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

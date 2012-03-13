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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class AddFilesWithMissingEntries {
  
  static final Logger log = Logger.getLogger(AddFilesWithMissingEntries.class);
  static boolean update = false;
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length > 1 || new HashSet<String>(Arrays.asList(args)).contains("-?")) {
      System.err.println("Usage: bin/accumulo " + AddFilesWithMissingEntries.class.getName() + " [update]");
      System.exit(1);
    }
    update = args.length > 0;
    final AuthInfo creds = SecurityConstants.getSystemCredentials();
    final Connector connector = HdfsZooInstance.getInstance().getConnector(creds.getUser(), creds.getPassword());
    final Key rootTableEnd = new Key(Constants.ROOT_TABLET_EXTENT.getEndRow());
    final Range range = new Range(rootTableEnd.followingKey(PartialKey.ROW), true, Constants.METADATA_RESERVED_KEYSPACE_START_KEY, false);
    final Scanner scanner = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    scanner.setRange(range);
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    
    KeyExtent last = new KeyExtent();
    String directory = null;
    Set<String> knownFiles = new HashSet<String>();
    
    int count = 0;
    final MultiTableBatchWriter writer = connector.createMultiTableBatchWriter(100000, 1000, 4);
    
    // collect the list of known files and the directory for each extent
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      KeyExtent ke = new KeyExtent(key.getRow(), (Text) null);
      // when the key extent changes
      if (!ke.equals(last)) {
        if (directory != null) {
          // add any files in the directory unknown to the key extent
          count += addUnknownFiles(fs, directory, knownFiles, last, writer);
        }
        directory = null;
        knownFiles.clear();
        last = ke;
      }
      if (Constants.METADATA_DIRECTORY_COLUMN.hasColumns(key)) {
        directory = entry.getValue().toString();
        log.debug("Found directory " + directory + " for row " + key.getRow().toString());
      } else if (key.compareColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY) == 0) {
        String filename = key.getColumnQualifier().toString();
        knownFiles.add(filename);
        log.debug("METADATA file found: " + filename);
      }
    }
    if (directory != null) {
      // catch the last key extent
      count += addUnknownFiles(fs, directory, knownFiles, last, writer);
    }
    log.info("There were " + count + " files that are unknown to the metadata table");
    writer.close();
  }
  
  private static int addUnknownFiles(FileSystem fs, String directory, Set<String> knownFiles, KeyExtent ke, MultiTableBatchWriter writer) throws Exception {
    int count = 0;
    final String tableId = ke.getTableId().toString();
    final Text row = ke.getMetadataEntry();
    log.info(row.toString());
    final Path path = new Path(ServerConstants.getTablesDir() + "/" + tableId + directory);
    for (FileStatus file : fs.listStatus(path)) {
      final String filename = directory + "/" + file.getPath().getName();
      if (!knownFiles.contains(filename)) {
        count++;
        final Mutation m = new Mutation(row);
        String size = Long.toString(file.getLen());
        String entries = "1"; // lie
        String value = size + "," + entries;
        m.put(Constants.METADATA_DATAFILE_COLUMN_FAMILY, new Text(filename), new Value(value.getBytes()));
        if (update) {
          writer.getBatchWriter(Constants.METADATA_TABLE_NAME).addMutation(m);
        }
      }
    }
    return count;
  }
  
}

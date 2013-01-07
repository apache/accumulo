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

import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

/**
 * Remove file entries for map files that don't exist.
 * 
 */
public class RemoveEntriesForMissingFiles {
  private static Logger log = Logger.getLogger(RemoveEntriesForMissingFiles.class);
  
  static class Opts extends ClientOpts {
    @Parameter(names="--fix")
    boolean fix = false;
  }
  
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(RemoveEntriesForMissingFiles.class.getName(), args, scanOpts, bwOpts);
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    Connector connector = opts.getConnector();
    Scanner metadata = connector.createScanner(Constants.METADATA_TABLE_NAME, opts.auths);
    metadata.setBatchSize(scanOpts.scanBatchSize);
    metadata.setRange(Constants.METADATA_KEYSPACE);
    metadata.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
    int count = 0;
    int missing = 0;
    BatchWriter writer = null; 
    if (opts.fix)
      writer = connector.createBatchWriter(Constants.METADATA_TABLE_NAME, bwOpts.getBatchWriterConfig());
    for (Entry<Key,Value> entry : metadata) {
      count++;
      Key key = entry.getKey();
      String table = new String(KeyExtent.tableOfMetadataRow(entry.getKey().getRow()));
      String file = key.getColumnQualifier().toString();
      if (!file.startsWith("/"))
        file = "/" + file;
      Path map = new Path(ServerConstants.getTablesDir() + "/" + table + file);
      if (!fs.exists(map)) {
        missing++;
        log.info("File " + map + " is missing");
        Mutation m = new Mutation(key.getRow());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        if (writer != null) {
          writer.addMutation(m);
          log.info("entry removed from metadata table: " + m);
        }
      }
    }
    if (writer != null && missing > 0)
      writer.close();
    log.info(String.format("%d files of %d missing", missing, count));
  }
}

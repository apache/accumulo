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

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class AddFilesWithMissingEntries {
  
  static final Logger log = Logger.getLogger(AddFilesWithMissingEntries.class);
  
  public static class Opts extends ClientOpts {
    @Parameter(names = "-update", description = "Make changes to the " + MetadataTable.NAME + " table to include missing files")
    boolean update = false;
  }
  
  /**
   * A utility to add files to the {@value MetadataTable#NAME} table that are not listed in the root tablet. This is a recovery tool for someone who knows what
   * they are doing. It might be better to save off files, and recover your instance by re-initializing and importing the existing files.
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(AddFilesWithMissingEntries.class.getName(), args, bwOpts);
    
    final Scanner scanner = opts.getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(MetadataSchema.TabletsSection.getRange());
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    
    KeyExtent last = new KeyExtent();
    String directory = null;
    Set<String> knownFiles = new HashSet<String>();
    
    int count = 0;
    final MultiTableBatchWriter writer = opts.getConnector().createMultiTableBatchWriter(bwOpts.getBatchWriterConfig());
    
    // collect the list of known files and the directory for each extent
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      KeyExtent ke = new KeyExtent(key.getRow(), (Text) null);
      // when the key extent changes
      if (!ke.equals(last)) {
        if (directory != null) {
          // add any files in the directory unknown to the key extent
          count += addUnknownFiles(fs, directory, knownFiles, last, writer, opts.update);
        }
        directory = null;
        knownFiles.clear();
        last = ke;
      }
      if (TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
        directory = entry.getValue().toString();
        log.debug("Found directory " + directory + " for row " + key.getRow().toString());
      } else if (key.compareColumnFamily(DataFileColumnFamily.NAME) == 0) {
        String filename = key.getColumnQualifier().toString();
        knownFiles.add(filename);
        log.debug("METADATA file found: " + filename);
      }
    }
    if (directory != null) {
      // catch the last key extent
      count += addUnknownFiles(fs, directory, knownFiles, last, writer, opts.update);
    }
    log.info("There were " + count + " files that are unknown to the metadata table");
    writer.close();
  }
  
  private static int addUnknownFiles(FileSystem fs, String directory, Set<String> knownFiles, KeyExtent ke, MultiTableBatchWriter writer, boolean update)
      throws Exception {
    int count = 0;
    final String tableId = ke.getTableId().toString();
    final Text row = ke.getMetadataEntry();
    log.info(row.toString());
    for (String dir : ServerConstants.getTablesDirs()) {
      final Path path = new Path(dir + "/" + tableId + directory);
      for (FileStatus file : fs.listStatus(path)) {
        if (file.getPath().getName().endsWith("_tmp") || file.getPath().getName().endsWith("_tmp.rf"))
          continue;
        final String filename = directory + "/" + file.getPath().getName();
        if (!knownFiles.contains(filename)) {
          count++;
          final Mutation m = new Mutation(row);
          String size = Long.toString(file.getLen());
          String entries = "1"; // lie
          String value = size + "," + entries;
          m.put(DataFileColumnFamily.NAME, new Text(filename), new Value(value.getBytes()));
          if (update) {
            writer.getBatchWriter(MetadataTable.NAME).addMutation(m);
          }
        }
      }
    }
    return count;
  }
  
}

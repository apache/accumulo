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

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * @deprecated since 1.4
 */
public class CountDiskRows {
  
  private static final Logger log = Logger.getLogger(CountDiskRows.class);
  
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      log.error("usage: CountDiskRows tablename");
      return;
    }
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    Key key = new Key();
    Value value = new Value();
    int numrows = 0;
    Text prevRow = new Text("");
    Text row = null;
    
    FileStatus[] tablets = fs.listStatus(new Path(ServerConstants.getTablesDir() + "/" + args[0]));
    for (FileStatus tablet : tablets) {
      FileStatus[] mapfiles = fs.listStatus(tablet.getPath());
      for (FileStatus mapfile : mapfiles) {
        MyMapFile.Reader mfr = new MyMapFile.Reader(fs, mapfile.getPath().toString(), conf);
        while (mfr.next(key, value)) {
          row = key.getRow();
          if (!row.equals(prevRow)) {
            prevRow = new Text(row);
            numrows++;
          }
        }
      }
    }
    
    log.info("files in directory " + args[0] + " have " + numrows);
  }
  
}

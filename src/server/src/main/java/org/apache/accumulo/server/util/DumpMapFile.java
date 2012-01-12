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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 * @deprecated
 */
public class DumpMapFile {
  private static final Logger log = Logger.getLogger(DumpMapFile.class);
  
  public static void main(String[] args) throws IOException {
    
    if (args.length < 1) {
      log.error("usage: DumpMapFile [-s] <map file>");
      return;
    }
    
    boolean summarize = false;
    int filenameIndex = 0;
    
    if (args[0].equals("-s")) {
      summarize = true;
      filenameIndex++;
    }
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    
    MyMapFile.Reader mr = new MyMapFile.Reader(fs, args[filenameIndex], conf);
    Key key = new Key();
    Value value = new Value();
    
    long count = 0;
    
    if (summarize) {
      if (mr.next(key, value)) {
        count++;
        log.info("first key : " + key);
      }
    }
    
    long start = System.currentTimeMillis();
    while (mr.next(key, value)) {
      if (!summarize) {
        log.info(key + " -> " + value);
      }
      
      count++;
    }
    
    if (summarize && count > 0) {
      log.info("last  key : " + key);
    }
    
    long stop = System.currentTimeMillis();
    
    if (summarize) {
      System.out.printf("count     : %,d\n", count);
    }
    
    log.info("\nsecs      : " + (stop - start) / 1000);
  }
}

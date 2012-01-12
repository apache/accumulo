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
package org.apache.accumulo.core.file.map;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class MapFileUtil {
  private static final Logger log = Logger.getLogger(MapFileUtil.class);
  
  /**
   * @deprecated since 1.4
   * @param conf
   * @param fs
   * @param dirName
   * @return
   */
  public static boolean attemptToFixMapFile(Configuration conf, FileSystem fs, String dirName) {
    boolean fixed = true;
    try {
      log.info("Attempting to fix mapfile " + dirName);
      Path indexFile = new Path(dirName + "/" + MyMapFile.INDEX_FILE_NAME);
      if (fs.exists(indexFile) && fs.getFileStatus(indexFile).getLen() == 0) {
        log.info("Deleting 0 length index file " + indexFile);
        fs.delete(indexFile, false);
      }
      
      MyMapFile.fix(fs, new Path(dirName), Key.class, Value.class, false, conf);
    } catch (Exception e) {
      log.error("Failed to fix mapfile " + dirName, e);
      fixed = false;
    }
    
    return fixed;
  }
  
  @SuppressWarnings("deprecation")
  public static MyMapFile.Reader openMapFile(AccumuloConfiguration acuconf, FileSystem fs, String dirName, Configuration conf) throws IOException {
    MyMapFile.Reader mfr = null;
    try {
      mfr = new MyMapFile.Reader(fs, dirName, conf);
      return mfr;
    } catch (IOException e) {
      if (attemptToFixMapFile(conf, fs, dirName)) {
        log.info("Fixed mapfile " + dirName);
        mfr = new MyMapFile.Reader(fs, dirName, conf);
        return mfr;
      }
      throw e;
    }
  }
  
  @SuppressWarnings("deprecation")
  public static MySequenceFile.Reader openIndex(Configuration conf, FileSystem fs, Path mapFile) throws IOException {
    Path indexPath = new Path(mapFile, MyMapFile.INDEX_FILE_NAME);
    MySequenceFile.Reader index = null;
    try {
      index = new MySequenceFile.Reader(fs, indexPath, conf);
      return index;
    } catch (IOException e) {
      if (attemptToFixMapFile(conf, fs, mapFile.toString())) {
        log.info("Fixed mapfile " + mapFile);
        index = new MySequenceFile.Reader(fs, indexPath, conf);
        return index;
      }
      throw e;
    }
  }
  
  /**
   * @deprecated sicne 1.4
   * 
   * @param acuTableConf
   * @param conf
   * @param fs
   * @param dirname
   * @return
   * @throws IOException
   */
  public static MyMapFile.Writer openMapFileWriter(AccumuloConfiguration acuTableConf, Configuration conf, FileSystem fs, String dirname) throws IOException {
    MyMapFile.Writer mfw = null;
    int hbs = conf.getInt("io.seqfile.compress.blocksize", -1);
    int hrep = conf.getInt("dfs.replication", -1);
    
    // dfs.replication
    
    Configuration newConf = null;
    
    int tbs = (int) acuTableConf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE);
    int trep = acuTableConf.getCount(Property.TABLE_FILE_REPLICATION);
    
    if (hbs != tbs) {
      newConf = new Configuration(conf);
      newConf.setInt("io.seqfile.compress.blocksize", tbs);
    }
    
    if (fs.exists(new Path(dirname)))
      log.error("Map file " + dirname + " already exists", new Exception());
    
    if (newConf != null)
      conf = newConf;
    
    mfw = new MyMapFile.Writer(conf, fs, dirname, Key.class, Value.class, MySequenceFile.CompressionType.BLOCK);
    
    if (trep > 0 && trep != hrep) {
      // tried to set dfs.replication property on conf obj, however this was ignored, so have to manually set the prop
      fs.setReplication(new Path(dirname + "/" + MyMapFile.DATA_FILE_NAME), (short) trep);
      fs.setReplication(new Path(dirname + "/" + MyMapFile.INDEX_FILE_NAME), (short) trep);
    }
    
    return mfw;
  }
}

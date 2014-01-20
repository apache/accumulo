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
package org.apache.accumulo.core.file;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class FileUtil {
  
  public static class FileInfo {
    Key firstKey = new Key();
    Key lastKey = new Key();
    
    public FileInfo(Key firstKey, Key lastKey) {
      this.firstKey = firstKey;
      this.lastKey = lastKey;
    }
    
    public Text getFirstRow() {
      return firstKey.getRow();
    }
    
    public Text getLastRow() {
      return lastKey.getRow();
    }
  }
  
  private static final Logger log = Logger.getLogger(FileUtil.class);
  
  private static class MLong {
    public MLong(long i) {
      l = i;
    }
    
    long l;
  }
  
  public static Map<KeyExtent,Long> estimateSizes(AccumuloConfiguration acuConf, Path mapFile, long fileSize, List<KeyExtent> extents, Configuration conf,
      FileSystem fs) throws IOException {
    
    long totalIndexEntries = 0;
    Map<KeyExtent,MLong> counts = new TreeMap<KeyExtent,MLong>();
    for (KeyExtent keyExtent : extents)
      counts.put(keyExtent, new MLong(0));
    
    Text row = new Text();
    
    FileSKVIterator index = FileOperations.getInstance().openIndex(mapFile.toString(), fs, conf, acuConf);
    
    try {
      while (index.hasTop()) {
        Key key = index.getTopKey();
        totalIndexEntries++;
        key.getRow(row);
        
        for (Entry<KeyExtent,MLong> entry : counts.entrySet())
          if (entry.getKey().contains(row))
            entry.getValue().l++;
        
        index.next();
      }
    } finally {
      try {
        if (index != null)
          index.close();
      } catch (IOException e) {
        // continue with next file
        log.error(e, e);
      }
    }
    
    Map<KeyExtent,Long> results = new TreeMap<KeyExtent,Long>();
    for (KeyExtent keyExtent : extents) {
      double numEntries = counts.get(keyExtent).l;
      if (numEntries == 0)
        numEntries = 1;
      long estSize = (long) ((numEntries / totalIndexEntries) * fileSize);
      results.put(keyExtent, estSize);
    }
    return results;
  }
  
  public static FileSystem getFileSystem(String path, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    if (path.contains(":"))
      return new Path(path).getFileSystem(conf);
    else
      return getFileSystem(conf, acuconf);
  }

  public static FileSystem getFileSystem(Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    String uri = acuconf.get(Property.INSTANCE_DFS_URI);
    if ("".equals(uri))
      return FileSystem.get(conf);
    else
      try {
        return FileSystem.get(new URI(uri), conf);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
  }
}

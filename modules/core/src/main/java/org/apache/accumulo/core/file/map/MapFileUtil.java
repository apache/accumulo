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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

public class MapFileUtil {
  public static MapFile.Reader openMapFile(AccumuloConfiguration acuconf, FileSystem fs, String dirName, Configuration conf) throws IOException {
    MapFile.Reader mfr = null;
    try {
      mfr = new MapFile.Reader(fs.makeQualified(new Path(dirName)), conf);
      return mfr;
    } catch (IOException e) {
      throw e;
    }
  }

  public static SequenceFile.Reader openIndex(Configuration conf, FileSystem fs, Path mapFile) throws IOException {
    Path indexPath = new Path(mapFile, MapFile.INDEX_FILE_NAME);
    SequenceFile.Reader index = null;
    try {
      index = new SequenceFile.Reader(conf, SequenceFile.Reader.file(fs.makeQualified(indexPath)));
      return index;
    } catch (IOException e) {
      throw e;
    }
  }
}

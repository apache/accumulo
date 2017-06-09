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
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.MetaIndexEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PrintInfo {
  public static void printMetaBlockInfo(Configuration conf, FileSystem fs, Path path) throws IOException {
    FSDataInputStream fsin = fs.open(path);
    BCFile.Reader bcfr = null;
    try {
      bcfr = new BCFile.Reader(fsin, fs.getFileStatus(path).getLen(), conf, SiteConfiguration.getInstance());

      Set<Entry<String,MetaIndexEntry>> es = bcfr.metaIndex.index.entrySet();

      for (Entry<String,MetaIndexEntry> entry : es) {
        PrintStream out = System.out;
        out.println("Meta block     : " + entry.getKey());
        out.println("      Raw size             : " + String.format("%,d", entry.getValue().getRegion().getRawSize()) + " bytes");
        out.println("      Compressed size      : " + String.format("%,d", entry.getValue().getRegion().getCompressedSize()) + " bytes");
        out.println("      Compression type     : " + entry.getValue().getCompressionAlgorithm().getName());
        out.println();
      }
    } finally {
      if (bcfr != null) {
        bcfr.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem hadoopFs = FileSystem.get(conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    Path path = new Path(args[0]);
    FileSystem fs;
    if (args[0].contains(":"))
      fs = path.getFileSystem(conf);
    else
      fs = hadoopFs.exists(path) ? hadoopFs : localFs; // fall back to local
    printMetaBlockInfo(conf, fs, path);
  }
}

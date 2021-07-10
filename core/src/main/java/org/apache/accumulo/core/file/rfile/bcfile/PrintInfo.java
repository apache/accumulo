/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.crypto.CryptoServiceFactory.ClassloaderType;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.MetaIndexEntry;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

public class PrintInfo {
  public static void printMetaBlockInfo(SiteConfiguration siteConfig, Configuration conf,
      FileSystem fs, Path path) throws IOException {
    FSDataInputStream fsin = fs.open(path);
    try (BCFile.Reader bcfr =
        new BCFile.Reader(fsin, fs.getFileStatus(path).getLen(), conf, CryptoServiceFactory
            .newInstance(siteConfig, ClassloaderType.ACCUMULO, CryptoEnvironment.Scope.TABLE))) {

      Set<Entry<String,MetaIndexEntry>> es = bcfr.metaIndex.index.entrySet();

      for (Entry<String,MetaIndexEntry> entry : es) {
        PrintStream out = System.out;
        out.println("Meta block     : " + entry.getKey());
        out.println("      Raw size             : "
            + String.format("%,d", entry.getValue().getRegion().getRawSize()) + " bytes");
        out.println("      Compressed size      : "
            + String.format("%,d", entry.getValue().getRegion().getCompressedSize()) + " bytes");
        out.println(
            "      Compression type     : " + entry.getValue().getCompressionAlgorithm().getName());
        out.println();
      }
    }
  }

  static class Opts extends ConfigOpts {

    @Parameter(description = " <file>")
    String file;

  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs("PrintInfo", args);
    if (opts.file.isEmpty()) {
      System.err.println("No files were given");
      System.exit(-1);
    }
    var siteConfig = opts.getSiteConfiguration();
    Configuration conf = new Configuration();
    FileSystem hadoopFs = FileSystem.get(conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    Path path = new Path(opts.file);
    FileSystem fs;
    if (opts.file.contains(":")) {
      fs = path.getFileSystem(conf);
    } else {
      fs = hadoopFs.exists(path) ? hadoopFs : localFs; // fall back to local
    }
    printMetaBlockInfo(siteConfig, conf, fs, path);
  }
}

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
package org.apache.accumulo.core.file.rfile;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class PrintInfo {
  private static final Logger log = Logger.getLogger(PrintInfo.class);

  static class Opts extends Help {
    @Parameter(names = {"-d", "--dump"}, description = "dump the key/value pairs")
    boolean dump = false;
    @Parameter(names = {"--histogram"}, description = "print a histogram of the key-value sizes")
    boolean histogram = false;
    @Parameter(description = " <file> { <file> ... }")
    List<String> files = new ArrayList<String>();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    AccumuloConfiguration aconf = SiteConfiguration.getInstance(DefaultConfiguration.getInstance());
    // TODO ACCUMULO-2462 This will only work for RFiles (path only, not URI) in HDFS when the correct filesystem for the given file
    // is on Property.INSTANCE_DFS_DIR or, when INSTANCE_DFS_DIR is not defined, is on the default filesystem
    // defined in the Hadoop's core-site.xml
    //
    // A workaround is to always provide a URI to this class
    FileSystem hadoopFs = VolumeConfiguration.getDefaultVolume(conf, aconf).getFileSystem();
    FileSystem localFs = FileSystem.getLocal(conf);
    Opts opts = new Opts();
    opts.parseArgs(PrintInfo.class.getName(), args);
    if (opts.files.isEmpty()) {
      System.err.println("No files were given");
      System.exit(-1);
    }

    long countBuckets[] = new long[11];
    long sizeBuckets[] = new long[countBuckets.length];
    long totalSize = 0;

    for (String arg : opts.files) {
      Path path = new Path(arg);
      FileSystem fs;
      if (arg.contains(":"))
        fs = path.getFileSystem(conf);
      else {
        // Recommend a URI is given for the above todo reason
        log.warn("Attempting to find file across filesystems. Consider providing URI instead of path");
        fs = hadoopFs.exists(path) ? hadoopFs : localFs; // fall back to local
      }

      CachableBlockFile.Reader _rdr = new CachableBlockFile.Reader(fs, path, conf, null, null, aconf);
      Reader iter = new RFile.Reader(_rdr);

      iter.printInfo();
      System.out.println();
      org.apache.accumulo.core.file.rfile.bcfile.PrintInfo.main(new String[] {arg});

      if (opts.histogram || opts.dump) {
        iter.seek(new Range((Key) null, (Key) null), new ArrayList<ByteSequence>(), false);
        while (iter.hasTop()) {
          Key key = iter.getTopKey();
          Value value = iter.getTopValue();
          if (opts.dump)
            System.out.println(key + " -> " + value);
          if (opts.histogram) {
            long size = key.getSize() + value.getSize();
            int bucket = (int) Math.log10(size);
            countBuckets[bucket]++;
            sizeBuckets[bucket] += size;
            totalSize += size;
          }
          iter.next();
        }
      }
      iter.close();
      if (opts.histogram) {
        System.out.println("Up to size      count      %-age");
        for (int i = 1; i < countBuckets.length; i++) {
          System.out.println(String.format("%11.0f : %10d %6.2f%%", Math.pow(10, i), countBuckets[i], sizeBuckets[i] * 100. / totalSize));
        }
      }

      // If the output stream has closed, there is no reason to keep going.
      if (System.out.checkError())
        return;
    }
  }
}

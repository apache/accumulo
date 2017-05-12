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
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.RFile.Writer;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

/**
 * Split an RFile into large and small key/value files.
 *
 */
public class SplitLarge {

  static class Opts extends Help {
    @Parameter(names = "-m", description = "the maximum size of the key/value pair to shunt to the small file")
    long maxSize = 10 * 1024 * 1024;
    @Parameter(description = "<file.rf> { <file.rf> ... }")
    List<String> files = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.get(conf);
    Opts opts = new Opts();
    opts.parseArgs(SplitLarge.class.getName(), args);

    for (String file : opts.files) {
      AccumuloConfiguration aconf = DefaultConfiguration.getInstance();
      Path path = new Path(file);
      CachableBlockFile.Reader rdr = new CachableBlockFile.Reader(fs, path, conf, null, null, aconf);
      try (Reader iter = new RFile.Reader(rdr)) {

        if (!file.endsWith(".rf")) {
          throw new IllegalArgumentException("File must end with .rf");
        }
        String smallName = file.substring(0, file.length() - 3) + "_small.rf";
        String largeName = file.substring(0, file.length() - 3) + "_large.rf";

        int blockSize = (int) aconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE);
        try (Writer small = new RFile.Writer(new CachableBlockFile.Writer(fs, new Path(smallName), "gz", null, conf, aconf), blockSize);
            Writer large = new RFile.Writer(new CachableBlockFile.Writer(fs, new Path(largeName), "gz", null, conf, aconf), blockSize)) {
          small.startDefaultLocalityGroup();
          large.startDefaultLocalityGroup();

          iter.seek(new Range(), new ArrayList<ByteSequence>(), false);
          while (iter.hasTop()) {
            Key key = iter.getTopKey();
            Value value = iter.getTopValue();
            if (key.getSize() + value.getSize() < opts.maxSize) {
              small.append(key, value);
            } else {
              large.append(key, value);
            }
            iter.next();
          }

        }
      }
    }
  }

}

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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

/**
 * Simple tool to reproduce the file that caused problems in ACCUMULO-3967
 */
public class GenerateSequentialRFile implements Runnable {
  private static final Text CF = new Text("CF");
  private static final Text CQ = new Text("CQ");

  private final Opts opts;

  public GenerateSequentialRFile(Opts opts) {
    this.opts = opts;
  }

  static class Opts extends Help {
    @Parameter(names = {"-f", "--file"}, description = "Path to the file to create")
    String filePath;
    @Parameter(names = {"-nr"}, description = "Number of rows")
    long rows = 24;
    @Parameter(names = {"-nv"}, description = "Number of values per row")
    long valuesPerRow = 42000;
  }

  public void run() {
    try {
      final Configuration conf = new Configuration();
      Path p = new Path(opts.filePath);
      final FileSystem fs = p.getFileSystem(conf);
      FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder().forFile(opts.filePath, fs, conf)
          .withTableConfiguration(DefaultConfiguration.getInstance()).build();

      writer.startDefaultLocalityGroup();

      for (int x = 0; x < opts.rows; x++) {
        final Text row = new Text(String.format("%03d", x));
        for (int y = 0; y < opts.valuesPerRow; y++) {
          final String suffix = String.format("%05d", y);
          writer.append(new Key(new Text(row + ":" + suffix), CF, CQ), new Value(suffix.getBytes(UTF_8)));
        }
      }

      writer.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(GenerateSequentialRFile.class.getName(), args);
    new GenerateSequentialRFile(opts).run();
  }
}

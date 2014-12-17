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
package org.apache.accumulo.examples.simple.mapreduce.bulk;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

public class GenerateTestData {

  static class Opts extends org.apache.accumulo.core.cli.Help {
    @Parameter(names = "--start-row", required = true)
    int startRow = 0;
    @Parameter(names = "--count", required = true)
    int numRows = 0;
    @Parameter(names = "--output", required = true)
    String outputFile;
  }

  public static void main(String[] args) throws IOException {
    Opts opts = new Opts();
    opts.parseArgs(GenerateTestData.class.getName(), args);

    FileSystem fs = FileSystem.get(new Configuration());
    PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(opts.outputFile))));

    for (int i = 0; i < opts.numRows; i++) {
      out.println(String.format("row_%010d\tvalue_%010d", i + opts.startRow, i + opts.startRow));
    }
    out.close();
  }

}

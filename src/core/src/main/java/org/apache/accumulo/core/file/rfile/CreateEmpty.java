/**
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

import java.util.Arrays;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.bcfile.TFile;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Create an empty RFile for use in recovering from data loss where Accumulo still refers internally to a path.
 */
public class CreateEmpty {

  public static void main(String[] args) throws Exception {
    Configuration conf = CachedConfiguration.getInstance();

    Options opts = new Options();
    Option codecOption = new Option("c", "codec", true, "the compression codec to use. one of " + Arrays.toString(TFile.getSupportedCompressionAlgorithms()) + ". defaults to none.");
    opts.addOption(codecOption);
    Option help = new Option( "?", "help", false, "print this message" );
    opts.addOption(help);

    CommandLine commandLine = new BasicParser().parse(opts, args);
    if (commandLine.hasOption(help.getOpt()) || 0 == commandLine.getArgs().length) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(120, "$ACCUMULO_HOME/bin/accumulo " + CreateEmpty.class.getName() + "[options] path [path ...]",
          "", opts,
          "each path given is a filesystem URL. Relative paths are resolved according to the default filesytem defined in your Hadoop configuration, which is usually an HDFS instance.");
    }
    String codec = commandLine.getOptionValue(codecOption.getOpt(), TFile.COMPRESSION_NONE);

    for (String arg : commandLine.getArgs()) {
      if (!arg.endsWith(".rf")) {
        throw new IllegalArgumentException("File must end with .rf and '" + arg + "' does not.");
      }
    }

    for (String arg : commandLine.getArgs()) {
      Path path = new Path(arg);
      FileSKVWriter writer = (new RFileOperations()).openWriter(arg, path.getFileSystem(conf), conf, DefaultConfiguration.getDefaultConfiguration(), codec);
      writer.close();
    }
  }

}

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
package org.apache.accumulo.server.logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.tabletserver.log.MultiReader;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class LogReader {
  public static void usage() {
    System.err.println("Usage : " + LogReader.class.getName() + " [-r <row>] [-m <maxColumns] [-t <key extent>] [-p <row regex>] <log file>");
  }
  
  /**
   * Dump a Log File (Map or Sequence) to stdout. Will read from HDFS or local file system.
   * 
   * @param args
   *          - first argument is the file to print
   * @throws IOException
   * @throws ParseException
   */
  public static void main(String[] args) throws IOException {
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = TraceFileSystem.wrap(FileUtil.getFileSystem(conf, ServerConfiguration.getSiteConfiguration()));
    FileSystem local = TraceFileSystem.wrap(FileSystem.getLocal(conf));
    Option rowOpt = new Option("r", "row", true, "search for a specific row");
    Option maxOpt = new Option("m", "max-mutations", true, "the maximum number of mutations to print per log entry");
    Option tabletOpt = new Option("t", "tablet", true, "key extent");
    Option rowPatternOpt = new Option("p", "row-pattern", true, "search for a row that matches the given regex");
    Options options = new Options();
    options.addOption(rowOpt);
    options.addOption(maxOpt);
    options.addOption(tabletOpt);
    options.addOption(rowPatternOpt);
    CommandLine cl;
    try {
      cl = new BasicParser().parse(options, args);
    } catch (ParseException ex) {
      usage();
      return;
    }
    
    Matcher rowMatcher = null;
    KeyExtent ke = null;
    Text row = null;
    int max = 5;
    String[] files = cl.getArgs();
    if (files.length == 0) {
      usage();
      return;
    }
    if (cl.hasOption(rowOpt.getOpt()))
      row = new Text(cl.getOptionValue(rowOpt.getOpt()));
    if (cl.hasOption(maxOpt.getOpt()))
      max = Integer.parseInt(cl.getOptionValue(maxOpt.getOpt()));
    if (cl.hasOption(tabletOpt.getOpt())) {
      String extent = cl.getOptionValue(tabletOpt.getOpt());
      String sa[] = extent.split(";");
      ke = new KeyExtent(new Text(sa[0]), new Text(sa[1]), new Text(sa[2]));
    }
    if (cl.hasOption(rowPatternOpt.getOpt())) {
      Pattern pattern = Pattern.compile(cl.getOptionValue(rowPatternOpt.getOpt()));
      rowMatcher = pattern.matcher("");
    }
    
    Set<Integer> tabletIds = new HashSet<Integer>();

    for (String file : files) {
      
      Path path = new Path(file);
      LogFileKey key = new LogFileKey();
      LogFileValue value = new LogFileValue();
      
      if (fs.isFile(path)) {
        // read log entries from a simple hdfs file
        org.apache.hadoop.io.SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file), conf);
        while (reader.next(key, value)) {
          printLogEvent(key, value, row, rowMatcher, ke, tabletIds, max);
        }
      } else if (local.isFile(path)) {
        // read log entries from a simple file
        org.apache.hadoop.io.SequenceFile.Reader reader = new SequenceFile.Reader(local, new Path(file), conf);
        while (reader.next(key, value)) {
          printLogEvent(key, value, row, rowMatcher, ke, tabletIds, max);
        }
      } else {
        try {
          // read the log entries sorted in a map file
          MultiReader input = new MultiReader(fs, conf, file);
          while (input.next(key, value)) {
            printLogEvent(key, value, row, rowMatcher, ke, tabletIds, max);
          }
        } catch (FileNotFoundException ex) {
          SequenceFile.Reader input = new SequenceFile.Reader(local, new Path(file), conf);
          while (input.next(key, value)) {
            printLogEvent(key, value, row, rowMatcher, ke, tabletIds, max);
          }
        }
      }
    }
  }
  
  public static void printLogEvent(LogFileKey key, LogFileValue value, Text row, Matcher rowMatcher, KeyExtent ke, Set<Integer> tabletIds, int maxMutations) {
    
    if (ke != null) {
      if (key.event == LogEvents.DEFINE_TABLET) {
        if (key.tablet.equals(ke)) {
          tabletIds.add(key.tid);
        } else {
          return;
        }
      } else if (!tabletIds.contains(key.tid)) {
        return;
      }
    }

    if (row != null || rowMatcher != null) {
      if (key.event == LogEvents.MUTATION || key.event == LogEvents.MANY_MUTATIONS) {
        boolean found = false;
        for (Mutation m : value.mutations) {
          if (row != null && new Text(m.getRow()).equals(row)) {
            found = true;
            break;
          }
          
          if (rowMatcher != null) {
            rowMatcher.reset(new String(m.getRow()));
            if (rowMatcher.matches()) {
              found = true;
              break;
            }
          }
        }
        
        if (!found)
          return;
      } else {
        return;
      }
      
    }
    
    System.out.println(key);
    System.out.println(LogFileValue.format(value, maxMutations));
  }
  
}

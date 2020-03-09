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
package org.apache.accumulo.tserver.logger;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.DfsLogger.DFSLoggerInputStreams;
import org.apache.accumulo.tserver.log.DfsLogger.LogHeaderIncompleteException;
import org.apache.accumulo.tserver.log.RecoveryLogReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class LogReader {
  private static final Logger log = LoggerFactory.getLogger(LogReader.class);

  static class Opts extends Help {
    @Parameter(names = "-r", description = "print only mutations associated with the given row")
    String row;
    @Parameter(names = "-m", description = "limit the number of mutations printed per row")
    int maxMutations = 5;
    @Parameter(names = "-t",
        description = "print only mutations that fall within the given key extent")
    String extent;
    @Parameter(names = "-p", description = "search for a row that matches the given regex")
    String regexp;
    @Parameter(description = "<logfile> { <logfile> ... }")
    List<String> files = new ArrayList<>();
  }

  /**
   * Dump a Log File (Map or Sequence) to stdout. Will read from HDFS or local file system.
   *
   * @param args
   *          - first argument is the file to print
   */
  public static void main(String[] args) throws IOException {
    Opts opts = new Opts();
    opts.parseArgs(LogReader.class.getName(), args);
    var siteConfig = SiteConfiguration.auto();
    try (var fs = VolumeManagerImpl.get(siteConfig, new Configuration())) {

      Matcher rowMatcher = null;
      KeyExtent ke = null;
      Text row = null;
      if (opts.files.isEmpty()) {
        new JCommander(opts).usage();
        return;
      }
      if (opts.row != null) {
        row = new Text(opts.row);
      }
      if (opts.extent != null) {
        String[] sa = opts.extent.split(";");
        ke = new KeyExtent(TableId.of(sa[0]), new Text(sa[1]), new Text(sa[2]));
      }
      if (opts.regexp != null) {
        Pattern pattern = Pattern.compile(opts.regexp);
        rowMatcher = pattern.matcher("");
      }

      Set<Integer> tabletIds = new HashSet<>();

      for (String file : opts.files) {

        Path path = new Path(file);
        LogFileKey key = new LogFileKey();
        LogFileValue value = new LogFileValue();

        if (fs.getFileStatus(path).isFile()) {
          try (final FSDataInputStream fsinput = fs.open(path)) {
            // read log entries from a simple hdfs file
            DFSLoggerInputStreams streams;
            try {
              streams = DfsLogger.readHeaderAndReturnStream(fsinput, siteConfig);
            } catch (LogHeaderIncompleteException e) {
              log.warn("Could not read header for {} . Ignoring...", path);
              continue;
            }

            try (DataInputStream input = streams.getDecryptingInputStream()) {
              while (true) {
                try {
                  key.readFields(input);
                  value.readFields(input);
                } catch (EOFException ex) {
                  break;
                }
                printLogEvent(key, value, row, rowMatcher, ke, tabletIds, opts.maxMutations);
              }
            }
          }
        } else {
          // read the log entries sorted in a map file
          try (RecoveryLogReader input = new RecoveryLogReader(fs, path)) {
            while (input.hasNext()) {
              Entry<LogFileKey,LogFileValue> entry = input.next();
              printLogEvent(entry.getKey(), entry.getValue(), row, rowMatcher, ke, tabletIds,
                  opts.maxMutations);
            }
          }
        }
      }
    }
  }

  public static void printLogEvent(LogFileKey key, LogFileValue value, Text row, Matcher rowMatcher,
      KeyExtent ke, Set<Integer> tabletIds, int maxMutations) {

    if (ke != null) {
      if (key.event == LogEvents.DEFINE_TABLET) {
        if (key.tablet.equals(ke)) {
          tabletIds.add(key.tabletId);
        } else {
          return;
        }
      } else if (!tabletIds.contains(key.tabletId)) {
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
            rowMatcher.reset(new String(m.getRow(), UTF_8));
            if (rowMatcher.matches()) {
              found = true;
              break;
            }
          }
        }

        if (!found) {
          return;
        }
      } else {
        return;
      }

    }

    System.out.println(key);
    System.out.println(LogFileValue.format(value, maxMutations));
  }

}

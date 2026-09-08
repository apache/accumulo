/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.util.adminCommand;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;

@AutoService(KeywordExecutable.class)
public class PreSplitUtil extends ServerKeywordExecutable<PreSplitUtil.PreSplitOpts> {

  @Override
  public String keyword() {
    return "pre-split";
  }

  @Override
  public String description() {
    return "Generates UUID-based split points for any Accumulo table whose row keys are UUIDs.";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.INSTANCE;
  }

  @Override
  public void execute(JCommander cl, PreSplitOpts options) throws Exception {
    validateOptions(options);
    List<String> splits = generateSplits(options.numSplits);
    String tableName = options.tableName != null ? options.tableName : "<unspecified>";
    System.out.println("Generating " + splits.size() + " split points for table: " + tableName);

    if (options.splitsFile != null) {
      Path splitsPath = Path.of(options.splitsFile);
      try (var out = Files.newOutputStream(splitsPath);
          var osw = new OutputStreamWriter(out, UTF_8); var bw = new BufferedWriter(osw);
          var writer = new PrintWriter(bw)) {
        splits.forEach(writer::println);
      }
      System.out.println("Wrote " + splits.size() + " split point(s) to " + options.splitsFile);
    } else {
      splits.forEach(System.out::println);
    }
  }

  static class PreSplitOpts extends ServerOpts {
    @Parameter(names = {"-t", "--table"},
        description = "The names of the table for which to generate splits points.")
    String tableName = null;

    @Parameter(names = {"-n", "--num-splits"},
        description = "Generate N split points for the fate table and print to stdout. N must be >= 1.")
    int numSplits = -1;

    @Parameter(names = {"-sf", "--splitsFile"},
        description = "Write split points to a file. Used with -n or --num-splits.")
    String splitsFile = null;
  }

  public PreSplitUtil() {
    super(new PreSplitOpts());
  }

  public static void main(String[] args) throws Exception {
    new PreSplitUtil().execute(args);
  }

  static List<String> generateSplits(int numSplits) {
    Preconditions.checkArgument(numSplits >= 1,
        "Number of splits must be greater than 1. Specifying 0 would generate no splits and leave the table unchanged.");

    // Same logic as in FateManager.getDesiredPartitions()
    // Work w/ 60 bit unsigned integers to partition the space and then shift over by 4. Used 60
    // bits instead of 63 so it nicely aligns w/ hex in the uuid.
    long jump = (1L << 60) / (numSplits + 1);
    List<String> splits = new ArrayList<>(numSplits);
    for (int i = 1; i <= numSplits; i++) {
      long start = (i * jump) << 4;
      splits.add(new UUID(start, 0).toString());
    }

    return Collections.unmodifiableList(splits);
  }

  private void validateOptions(PreSplitOpts opts) {
    if (opts.numSplits == 0) {
      throw new IllegalArgumentException(
          "-n / --num-splits must be >= 1. Specifying 0 generates no splits and leaves the table unchanged.");
    }
    if (opts.numSplits < 0) {
      throw new IllegalArgumentException("-n / --num-splits is required and must be >= 1.");
    }
  }
}

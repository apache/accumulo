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
package org.apache.accumulo.shell.commands;

import java.io.IOException;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * "du" command that will compute disk usage for tables and shared usage across tables by scanning
 * the metadata table for file size information.
 *
 * Because the metadata table is used for computing usage and not the actual files in HDFS the
 * results will be an estimate. Older entries may exist with no file metadata (resulting in size 0)
 * and other actions in the cluster can impact the estimated size such as flushes, tablet splits,
 * compactions, etc.
 *
 * For more accurate information a compaction should first be run on the set of tables being
 * computed.
 */
public class DUCommand extends Command {

  private Option optTablePattern, optHumanReadble, optNamespace;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException, TableNotFoundException, NamespaceNotFoundException {

    final SortedSet<String> tables = new TreeSet<>(Arrays.asList(cl.getArgs()));

    if (cl.hasOption(ShellOptions.tableOption)) {
      tables.add(cl.getOptionValue(ShellOptions.tableOption));
    }

    if (cl.hasOption(optNamespace.getOpt())) {
      NamespaceId namespaceId = Namespaces.getNamespaceId(shellState.getContext(),
          cl.getOptionValue(optNamespace.getOpt()));
      tables.addAll(Namespaces.getTableNames(shellState.getContext(), namespaceId));
    }

    boolean prettyPrint = cl.hasOption(optHumanReadble.getOpt());

    // Add any patterns
    if (cl.hasOption(optTablePattern.getOpt())) {
      shellState.getAccumuloClient().tableOperations().list().stream()
          .filter(Pattern.compile(cl.getOptionValue(optTablePattern.getOpt())).asMatchPredicate())
          .forEach(tables::add);
    }

    // If we didn't get any tables, and we have a table selected, add the current table
    if (tables.isEmpty() && !shellState.getTableName().isEmpty()) {
      tables.add(shellState.getTableName());
    }

    // sanity check...make sure the user-specified tables exist
    for (String tableName : tables) {
      if (!shellState.getAccumuloClient().tableOperations().exists(tableName)) {
        throw new TableNotFoundException(tableName, tableName,
            "specified table that doesn't exist");
      }
    }

    try {
      String valueFormat = prettyPrint ? "%9s" : "%,24d";
      for (DiskUsage usage : shellState.getAccumuloClient().tableOperations()
          .getDiskUsage(tables)) {
        Object value = prettyPrint ? NumUtil.bigNumberForSize(usage.getUsage()) : usage.getUsage();
        shellState.getWriter()
            .println(String.format(valueFormat + " %s", value, usage.getTables()));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return 0;
  }

  @Override
  public String description() {
    return "Prints estimated space, in bytes, used by files referenced by a "
        + "table or tables.  When multiple tables are specified it prints how much space, in "
        + "bytes, are used by files shared between tables, if any. Because the metadata table "
        + "is used for the file size information and not the actual files in HDFS the results "
        + "will be an estimate. Older entries may exist with no file metadata (resulting in size 0) and "
        + "other actions in the cluster can impact the estimated size such as flushes, tablet splits, "
        + "compactions, etc. For more accurate information a compaction should first be run on all of the files for the "
        + "set of tables being computed.";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    optTablePattern = new Option("p", "pattern", true, "regex pattern of table names");
    optTablePattern.setArgName("pattern");

    optHumanReadble =
        new Option("h", "human-readable", false, "format large sizes to human readable units");
    optHumanReadble.setArgName("human readable output");

    optNamespace =
        new Option(ShellOptions.namespaceOption, "namespace", true, "name of a namespace");
    optNamespace.setArgName("namespace");

    o.addOption(OptUtil.tableOpt("table to examine"));

    o.addOption(optTablePattern);
    o.addOption(optHumanReadble);
    o.addOption(optNamespace);

    return o;
  }

  @Override
  public String usage() {
    return getName() + " <table>{ <table>}";
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }
}

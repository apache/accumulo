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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.TableDiskUsageResult;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * New "du" command that will compute disk usage by scanning the metadata table for files instead of
 * using the HDFS iterator on the server side to scan files.
 */
public class DuMetaCommand extends Command {

  private Option optTablePattern, optHumanReadable, optNamespace, optVerbose;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException, TableNotFoundException, NamespaceNotFoundException, AccumuloException,
      AccumuloSecurityException {

    final String user = shellState.getAccumuloClient().whoami();
    final Authorizations auths =
        shellState.getAccumuloClient().securityOperations().getUserAuthorizations(user);

    final SortedSet<String> tables = new TreeSet<>(Arrays.asList(cl.getArgs()));

    if (cl.hasOption(ShellOptions.tableOption)) {
      tables.add(cl.getOptionValue(ShellOptions.tableOption));
    }

    // If a namespace is specified then grab all matching tables
    if (cl.hasOption(optNamespace.getOpt())) {
      NamespaceId namespaceId = Namespaces.getNamespaceId(shellState.getContext(),
          cl.getOptionValue(optNamespace.getOpt()));
      tables.addAll(Namespaces.getTableNames(shellState.getContext(), namespaceId));
    }

    boolean prettyPrint = cl.hasOption(optHumanReadable.getOpt());
    boolean verbose = cl.hasOption(optVerbose.getOpt());

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
      final String valueFormat = prettyPrint ? "%s" : "%,d";
      final TableDiskUsageResult usageResult = shellState.getAccumuloClient().tableOperations()
          .getDiskUsageFromMetadata(tables, verbose, auths);

      // Print results for each table queried
      for (Map.Entry<TableId,AtomicLong> entry : usageResult.getTableUsages().entrySet()) {
        final Object usage =
            prettyPrint ? NumUtil.bigNumberForSize(entry.getValue().get()) : entry.getValue().get();

        // Print the usage for the table
        shellState.getWriter().print(String.format("%s  %s  used total: " + valueFormat,
            shellState.getContext().getTableName(entry.getKey()), entry.getKey(), usage));

        // Print usage for all volume(s) that contain files for this table
        Optional.ofNullable(usageResult.getVolumeUsages().get(entry.getKey()))
            .ifPresent(tableVolUsage -> tableVolUsage.entrySet()
                .forEach(vuEntry -> shellState.getWriter()
                    .print(String.format("  %s: " + valueFormat, vuEntry.getKey(),
                        prettyPrint ? NumUtil.bigNumberForSize(vuEntry.getValue().get())
                            : vuEntry.getValue().get()))));

        // Check/print if this table has any shared files
        final Optional<Set<TableId>> sharedTables =
            Optional.ofNullable(usageResult.getSharedTables().get(entry.getKey()));
        final boolean hasShared = sharedTables.map(st -> st.size() > 0).orElse(false);

        shellState.getWriter().print(String.format("  has_shared: %s", hasShared));
        if (hasShared) {
          shellState.getWriter().print(String.format("  shared with:  %s", sharedTables.get()));
        }
        shellState.getWriter().println();
      }

      // If the verbose flag is specified then also print the statistics on size that is shared
      // between the tables that were provided to the du command.
      if (verbose) {
        shellState.getWriter().println("\nShared usage between tables:");

        for (DiskUsage usage : usageResult.getSharedDiskUsages()) {
          Object value =
              prettyPrint ? NumUtil.bigNumberForSize(usage.getUsage()) : usage.getUsage();
          shellState.getWriter()
              .println(String.format(valueFormat + " %s", value, usage.getTables()));
        }
      }

      shellState.getWriter().println();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return 0;
  }

  @Override
  public String description() {
    return "prints how much space, in bytes, is used by files referenced by a"
        + " table or tables. If the verbose flag is provided this prints how much space, in"
        + " bytes, is used by files shared between the provided tables, if any.";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    optTablePattern = new Option("p", "pattern", true, "regex pattern of table names");
    optTablePattern.setArgName("pattern");

    optHumanReadable =
        new Option("h", "human-readable", false, "format large sizes to human readable units");
    optHumanReadable.setArgName("human readable output");

    optNamespace =
        new Option(ShellOptions.namespaceOption, "namespace", true, "name of a namespace");
    optNamespace.setArgName("namespace");

    optVerbose = new Option("v", "verbose", false, "display detailed usage information");

    o.addOption(OptUtil.tableOpt("table to examine"));

    o.addOption(optTablePattern);
    o.addOption(optHumanReadable);
    o.addOption(optNamespace);
    o.addOption(optVerbose);

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

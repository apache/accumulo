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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility that generates single line tablet info. The output of this could be fed to sort, awk,
 * grep, etc. in order to answer questions like which tablets have the most files.
 */
public class ListTabletsCommand extends Command {

  private static final Logger log = LoggerFactory.getLogger(ListTabletsCommand.class);

  private Option outputFileOpt;
  private Option optTablePattern;
  private Option optHumanReadable;
  private Option optNamespace;
  private Option disablePaginationOpt;

  static final String header =
      String.format("%-4s %-15s %-5s %-5s %-9s %-9s %-10s %-30s %-5s %-20s %-20s %-10s", "NUM",
          "TABLET_DIR", "FILES", "WALS", "ENTRIES", "SIZE", "STATUS", "LOCATION", "ID",
          "START (Exclusive)", "END", "GOAL");

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    final Set<TableInfo> tableInfoSet = populateTables(cl, shellState);
    if (tableInfoSet.isEmpty()) {
      log.warn("No tables found that match your criteria");
      return 0;
    }

    boolean humanReadable = cl.hasOption(optHumanReadable.getOpt());

    List<String> lines = new LinkedList<>();
    lines.add(header);
    for (TableInfo tableInfo : tableInfoSet) {
      String name = tableInfo.name;
      lines.add("TABLE: " + name);

      try (Stream<TabletInformation> tabletInfoStream =
          shellState.getContext().tableOperations().getTabletInformation(name, new Range())) {
        final AtomicInteger counter = new AtomicInteger(1);
        tabletInfoStream.forEach(tabletInfo -> {
          int i = counter.getAndIncrement();
          lines.add(
              String.format("%-4d %-15s %-5d %-5s %-9s %-9s %-10s %-30s %-5s %-20s %-20s %-10s", i,
                  tabletInfo.getTabletDir(), tabletInfo.getNumFiles(), tabletInfo.getNumWalLogs(),
                  getEstimatedEntries(tabletInfo.getEstimatedEntries(), humanReadable),
                  getEstimatedSize(tabletInfo.getEstimatedSize(), humanReadable),
                  tabletInfo.getTabletState(), tabletInfo.getLocation().orElse("None"),
                  tabletInfo.getTabletId().getTable(),
                  tabletInfo.getTabletId().getPrevEndRow() == null ? "-INF"
                      : tabletInfo.getTabletId().getPrevEndRow().toString(),
                  tabletInfo.getTabletId().getEndRow() == null ? "+INF"
                      : tabletInfo.getTabletId().getEndRow().toString(),
                  tabletInfo.getTabletAvailability()));
        });
      }
    }

    if (lines.size() == 1) {
      lines.add("No data");
    }

    printResults(cl, shellState, lines);
    return 0;
  }

  private String getEstimatedSize(long size, boolean humanReadable) {
    if (humanReadable) {
      return NumUtil.bigNumberForQuantity(size);
    }
    return String.format("%,d", size);
  }

  private String getEstimatedEntries(long numEntries, boolean humanReadable) {
    if (humanReadable) {
      return NumUtil.bigNumberForQuantity(numEntries);
    }
    return String.format("%,d", numEntries);
  }

  @VisibleForTesting
  protected void printResults(CommandLine cl, Shell shellState, List<String> lines)
      throws Exception {
    if (cl.hasOption(outputFileOpt.getOpt())) {
      final String outputFile = cl.getOptionValue(outputFileOpt.getOpt());
      Shell.PrintFile printFile = new Shell.PrintFile(outputFile);
      shellState.printLines(lines.iterator(), false, printFile);
      printFile.close();
    } else {
      boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());
      shellState.printLines(lines.iterator(), paginate);
    }
  }

  /**
   * Process the command line for table names using table option, table name pattern, or default to
   * current table.
   *
   * @param cl command line
   * @param shellState shell state
   * @return set of table names.
   * @throws NamespaceNotFoundException if the namespace option is specified and namespace does not
   *         exist
   */
  private Set<TableInfo> populateTables(final CommandLine cl, final Shell shellState)
      throws NamespaceNotFoundException {

    final TableOperations tableOps = shellState.getAccumuloClient().tableOperations();
    var tableIdMap = tableOps.tableIdMap();

    Set<TableInfo> tableSet = new TreeSet<>();

    if (cl.hasOption(optTablePattern.getOpt())) {
      Pattern tablePattern = Pattern.compile(cl.getOptionValue(optTablePattern.getOpt()));
      for (String table : tableOps.list()) {
        if (tablePattern.matcher(table).matches()) {
          TableId id = TableId.of(tableIdMap.get(table));
          tableSet.add(new TableInfo(table, id));
        }
      }
      return tableSet;
    }

    if (cl.hasOption(optNamespace.getOpt())) {
      String nsName = cl.getOptionValue(optNamespace.getOpt());
      NamespaceId namespaceId = Namespaces.getNamespaceId(shellState.getContext(), nsName);
      List<String> tables = Namespaces.getTableNames(shellState.getContext(), namespaceId);
      tables.forEach(name -> {
        String tableIdString = tableIdMap.get(name);
        if (tableIdString != null) {
          TableId id = TableId.of(tableIdString);
          tableSet.add(new TableInfo(name, id));
        } else {
          log.warn("Table not found: {}", name);
        }
      });
      return tableSet;
    }

    if (cl.hasOption(ShellOptions.tableOption)) {
      String table = cl.getOptionValue(ShellOptions.tableOption);
      String idString = tableIdMap.get(table);
      if (idString != null) {
        TableId id = TableId.of(idString);
        tableSet.add(new TableInfo(table, id));
      } else {
        log.warn("Table not found: {}", table);
      }
      return tableSet;
    }

    // If we didn't get any tables, and we have a table selected, add the current table
    String table = shellState.getTableName();
    if (!table.isEmpty()) {
      TableId id = TableId.of(tableIdMap.get(table));
      tableSet.add(new TableInfo(table, id));
      return tableSet;
    }

    return Collections.emptySet();
  }

  /**
   * Wrapper for tableName and id. Comparisons, equals and hash code use tableName (id is ignored)
   */
  static class TableInfo implements Comparable<TableInfo> {

    public final String name;
    public final TableId id;

    public TableInfo(final String name, final TableId id) {
      this.name = name;
      this.id = id;
    }

    @Override
    public int compareTo(TableInfo other) {
      return name.compareTo(other.name);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TableInfo tableInfo = (TableInfo) o;
      return name.equals(tableInfo.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }

  @Override
  public String description() {
    return "Prints info about every tablet for a table, one tablet per line.";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {

    final Options opts = new Options();
    opts.addOption(OptUtil.tableOpt("table to be scanned"));

    optTablePattern = new Option("p", "pattern", true, "regex pattern of table names");
    optTablePattern.setArgName("pattern");
    opts.addOption(optTablePattern);

    optNamespace =
        new Option(ShellOptions.namespaceOption, "namespace", true, "name of a namespace");
    optNamespace.setArgName("namespace");
    opts.addOption(optNamespace);

    optHumanReadable =
        new Option("h", "human-readable", false, "format large sizes to human readable units");
    optHumanReadable.setArgName("human readable output");
    opts.addOption(optHumanReadable);

    disablePaginationOpt =
        new Option("np", "no-pagination", false, "disables pagination of output");
    opts.addOption(disablePaginationOpt);

    outputFileOpt = new Option("o", "output", true, "local file to write output to");
    outputFileOpt.setArgName("file");
    opts.addOption(outputFileOpt);

    return opts;
  }

}

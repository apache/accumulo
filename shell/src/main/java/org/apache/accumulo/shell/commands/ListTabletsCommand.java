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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility that generates single line tablet info. The output of this could be fed to sort, awk,
 * grep, etc inorder to answer questions like which tablets have the most files.
 */
public class ListTabletsCommand extends Command {

  private static final Logger log = LoggerFactory.getLogger(ListTabletsCommand.class);

  private Option outputFileOpt;
  private Option optTablePattern;
  private Option optHumanReadable;
  private Option optNamespace;
  private Option disablePaginationOpt;

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    final Set<TableInfo> tableInfoSet = populateTables(cl, shellState);
    if (tableInfoSet.isEmpty()) {
      log.warn("No tables found that match your criteria");
      return 0;
    }
    boolean humanReadable = cl.hasOption(optHumanReadable.getOpt());

    List<String> lines = new LinkedList<>();
    lines.add(TabletRowInfo.header);
    for (TableInfo tableInfo : tableInfoSet) {
      String name = tableInfo.name;
      lines.add("TABLE: " + name);

      List<TabletRowInfo> rows = getTabletRowInfo(shellState, tableInfo);
      for (int i = 0; i < rows.size(); i++) {
        TabletRowInfo row = rows.get(i);
        lines.add(row.format(i + 1, humanReadable));
      }
    }

    if (lines.size() == 1) {
      lines.add("No data");
    }

    printResults(cl, shellState, lines);
    return 0;
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
   * Wrapper for tablename and id. Comparisons, equals and hash code use tablename (id is ignored)
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

  private List<TabletRowInfo> getTabletRowInfo(Shell shellState, TableInfo tableInfo)
      throws Exception {
    log.trace("scan metadata for tablet info table name: \'{}\', tableId: \'{}\' ", tableInfo.name,
        tableInfo.id);

    List<TabletRowInfo> tResults = getMetadataInfo(shellState, tableInfo);

    if (log.isTraceEnabled()) {
      for (TabletRowInfo tabletRowInfo : tResults) {
        log.trace("Tablet info: {}", tabletRowInfo);
      }
    }

    return tResults;
  }

  protected List<TabletRowInfo> getMetadataInfo(Shell shellState, TableInfo tableInfo)
      throws Exception {
    List<TabletRowInfo> results = new ArrayList<>();
    final ClientContext context = shellState.getContext();
    Set<TServerInstance> liveTserverSet = TabletMetadata.getLiveTServers(context);

    try (var tabletsMetadata = TabletsMetadata.builder(context).forTable(tableInfo.id).build()) {
      for (var md : tabletsMetadata) {
        TabletRowInfo.Factory factory = new TabletRowInfo.Factory(tableInfo.name, md.getExtent());
        var fileMap = md.getFilesMap();
        factory.numFiles(fileMap.size());
        long entries = 0L;
        long size = 0L;
        for (DataFileValue dfv : fileMap.values()) {
          entries += dfv.getNumEntries();
          size += dfv.getSize();
        }
        factory.numEntries(entries);
        factory.size(size);
        factory.numWalLogs(md.getLogs().size());
        factory.dir(md.getDirName());
        factory.location(md.getLocation());
        factory.status(md.getTabletState(liveTserverSet).toString());
        results.add(factory.build());
      }
    }

    return results;
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

  static class TabletRowInfo {

    public final String tableName;
    public final int numFiles;
    public final int numWalLogs;
    public final long numEntries;
    public final long size;
    public final String status;
    public final String location;
    public final String dir;
    public final TableId tableId;
    public final KeyExtent tablet;
    public final boolean tableExists;

    private TabletRowInfo(String tableName, KeyExtent tablet, int numFiles, int numWalLogs,
        long numEntries, long size, String status, String location, String dir,
        boolean tableExists) {
      this.tableName = tableName;
      this.tableId = tablet.tableId();
      this.tablet = tablet;
      this.numFiles = numFiles;
      this.numWalLogs = numWalLogs;
      this.numEntries = numEntries;
      this.size = size;
      this.status = status;
      this.location = location;
      this.dir = dir;
      this.tableExists = tableExists;
    }

    String getNumEntries(final boolean humanReadable) {
      if (humanReadable) {
        return String.format("%9s", NumUtil.bigNumberForQuantity(numEntries));
      }
      // return String.format("%,24d", numEntries);
      return Long.toString(numEntries);
    }

    String getSize(final boolean humanReadable) {
      if (humanReadable) {
        return String.format("%9s", NumUtil.bigNumberForSize(size));
      }
      // return String.format("%,24d", size);
      return Long.toString(size);
    }

    public String getEndRow() {
      Text t = tablet.endRow();
      if (t == null) {
        return "+INF";
      } else {
        return t.toString();
      }
    }

    public String getStartRow() {
      Text t = tablet.prevEndRow();
      if (t == null) {
        return "-INF";
      } else {
        return t.toString();
      }
    }

    public static final String header = String.format(
        "%-4s %-15s %-5s %-5s %-9s %-9s %-10s %-30s %-5s %-20s %-20s", "NUM", "TABLET_DIR", "FILES",
        "WALS", "ENTRIES", "SIZE", "STATUS", "LOCATION", "ID", "START (Exclusive)", "END");

    String format(int number, boolean prettyPrint) {
      return String.format("%-4d %-15s %-5d %-5s %-9s %-9s %-10s %-30s %-5s %-20s %-20s", number,
          dir, numFiles, numWalLogs, getNumEntries(prettyPrint), getSize(prettyPrint), status,
          location, tableId, getStartRow(), getEndRow());
    }

    public String getTablet() {
      return getStartRow() + " " + getEndRow();
    }

    public static class Factory {
      final String tableName;
      final KeyExtent tablet;
      final TableId tableId;
      int numFiles = 0;
      int numWalLogs = 0;
      long numEntries = 0;
      long size = 0;
      String status = "";
      String location = "";
      String dir = "";
      boolean tableExists = false;

      Factory(final String tableName, KeyExtent tablet) {
        this.tableName = tableName;
        this.tablet = tablet;
        this.tableId = tablet.tableId();
      }

      Factory numFiles(int numFiles) {
        this.numFiles = numFiles;
        return this;
      }

      Factory numWalLogs(int numWalLogs) {
        this.numWalLogs = numWalLogs;
        return this;
      }

      public Factory numEntries(long numEntries) {
        this.numEntries = numEntries;
        return this;
      }

      public Factory size(long size) {
        this.size = size;
        return this;
      }

      public Factory status(String status) {
        this.status = status;
        return this;
      }

      public Factory location(TabletMetadata.Location location) {
        if (location == null) {
          this.location = "None";
        } else {
          String server = location.getHostPort();
          this.location = location.getType() + ":" + server;
        }
        return this;
      }

      public Factory dir(String dirName) {
        this.dir = dirName;
        return this;
      }

      public Factory tableExists(boolean tableExists) {
        this.tableExists = tableExists;
        return this;
      }

      public TabletRowInfo build() {
        return new TabletRowInfo(tableName, tablet, numFiles, numWalLogs, numEntries, size, status,
            location, dir, tableExists);
      }
    }
  }

}

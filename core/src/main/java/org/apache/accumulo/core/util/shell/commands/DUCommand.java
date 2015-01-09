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
package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class DUCommand extends Command {

  private Option optTablePattern, optHumanReadble, optNamespace;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws IOException, TableNotFoundException,
      NamespaceNotFoundException {

    final SortedSet<String> tables = new TreeSet<String>(Arrays.asList(cl.getArgs()));

    if (cl.hasOption(Shell.tableOption)) {
      String tableName = cl.getOptionValue(Shell.tableOption);
      if (!shellState.getConnector().tableOperations().exists(tableName)) {
        throw new TableNotFoundException(tableName, tableName, "specified table that doesn't exist");
      }
      tables.add(tableName);
    }

    if (cl.hasOption(optNamespace.getOpt())) {
      Instance instance = shellState.getInstance();
      String namespaceId = Namespaces.getNamespaceId(instance, cl.getOptionValue(optNamespace.getOpt()));
      tables.addAll(Namespaces.getTableNames(instance, namespaceId));
    }

    boolean prettyPrint = cl.hasOption(optHumanReadble.getOpt()) ? true : false;

    // Add any patterns
    if (cl.hasOption(optTablePattern.getOpt())) {
      for (String table : shellState.getConnector().tableOperations().list()) {
        if (table.matches(cl.getOptionValue(optTablePattern.getOpt()))) {
          tables.add(table);
        }
      }
    }

    // If we didn't get any tables, and we have a table selected, add the current table
    if (tables.isEmpty() && !shellState.getTableName().isEmpty()) {
      tables.add(shellState.getTableName());
    }

    try {
      String valueFormat = prettyPrint ? "%9s" : "%,24d";
      for (DiskUsage usage : shellState.getConnector().tableOperations().getDiskUsage(tables)) {
        Object value = prettyPrint ? NumUtil.bigNumberForSize(usage.getUsage()) : usage.getUsage();
        shellState.getReader().println(String.format(valueFormat + " %s", value, usage.getTables()));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return 0;
  }

  @Override
  public String description() {
    return "prints how much space, in bytes, is used by files referenced by a table.  "
        + "When multiple tables are specified it prints how much space, in bytes, is used by files shared between tables, if any.";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    optTablePattern = new Option("p", "pattern", true, "regex pattern of table names");
    optTablePattern.setArgName("pattern");

    optHumanReadble = new Option("h", "human-readable", false, "format large sizes to human readable units");
    optHumanReadble.setArgName("human readable output");

    optNamespace = new Option(Shell.namespaceOption, "namespace", true, "name of a namespace");
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

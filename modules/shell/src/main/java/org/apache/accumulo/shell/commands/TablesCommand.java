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
package org.apache.accumulo.shell.commands;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.MapUtils;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public class TablesCommand extends Command {
  static final String NAME_AND_ID_FORMAT = "%-20s => %9s%n";

  private Option tableIdOption;
  private Option sortByTableIdOption;
  private Option disablePaginationOpt;

  @SuppressWarnings("unchecked")
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException,
      NamespaceNotFoundException {

    final String namespace = cl.hasOption(OptUtil.namespaceOpt().getOpt()) ? OptUtil.getNamespaceOpt(cl, shellState) : null;
    Map<String,String> tables = shellState.getConnector().tableOperations().tableIdMap();

    // filter only specified namespace
    tables = Maps.filterKeys(tables, tableName -> namespace == null || Tables.qualify(tableName).getFirst().equals(namespace));

    final boolean sortByTableId = cl.hasOption(sortByTableIdOption.getOpt());
    tables = new TreeMap<String,String>((sortByTableId ? MapUtils.invertMap(tables) : tables));

    Iterator<String> it = Iterators.transform(tables.entrySet().iterator(), entry -> {
      String tableName = String.valueOf(sortByTableId ? entry.getValue() : entry.getKey());
      String tableId = String.valueOf(sortByTableId ? entry.getKey() : entry.getValue());
      if (namespace != null)
        tableName = Tables.qualify(tableName).getSecond();
      if (cl.hasOption(tableIdOption.getOpt()))
        return String.format(NAME_AND_ID_FORMAT, tableName, tableId);
      else
        return tableName;
    });

    shellState.printLines(it, !cl.hasOption(disablePaginationOpt.getOpt()));
    return 0;
  }

  @Override
  public String description() {
    return "displays a list of all existing tables";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    tableIdOption = new Option("l", "list-ids", false, "display internal table ids along with the table name");
    o.addOption(tableIdOption);
    sortByTableIdOption = new Option("s", "sort-ids", false, "with -l: sort output by table ids");
    o.addOption(sortByTableIdOption);
    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    o.addOption(disablePaginationOpt);
    o.addOption(OptUtil.namespaceOpt("name of namespace to list only its tables"));
    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }
}

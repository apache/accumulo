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

import java.util.EnumSet;

import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class DeleteIterCommand extends Command {
  private Option allScopeOpt, mincScopeOpt, majcScopeOpt, scanScopeOpt, nameOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {

    boolean tables =
        cl.hasOption(OptUtil.tableOpt().getOpt()) || !shellState.getTableName().isEmpty();
    boolean namespaces = cl.hasOption(OptUtil.namespaceOpt().getOpt());

    final String name = cl.getOptionValue(nameOpt.getOpt());

    if (namespaces) {
      if (!shellState.getAccumuloClient().namespaceOperations()
          .listIterators(OptUtil.getNamespaceOpt(cl, shellState)).containsKey(name)) {
        Shell.log.warn("no iterators found that match your criteria");
        return 0;
      }
    } else if (tables) {
      if (!shellState.getAccumuloClient().tableOperations()
          .listIterators(OptUtil.getTableOpt(cl, shellState)).containsKey(name)) {
        Shell.log.warn("no iterators found that match your criteria");
        return 0;
      }
    } else {
      throw new IllegalArgumentException("No table or namespace specified");
    }

    final EnumSet<IteratorScope> scopes = EnumSet.noneOf(IteratorScope.class);
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(mincScopeOpt.getOpt())) {
      scopes.add(IteratorScope.minc);
    }
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(majcScopeOpt.getOpt())) {
      scopes.add(IteratorScope.majc);
    }
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(scanScopeOpt.getOpt())) {
      scopes.add(IteratorScope.scan);
    }
    if (scopes.isEmpty()) {
      throw new IllegalArgumentException("You must select at least one scope to configure");
    }

    if (namespaces) {
      shellState.getAccumuloClient().namespaceOperations()
          .removeIterator(OptUtil.getNamespaceOpt(cl, shellState), name, scopes);
    } else if (tables) {
      shellState.getAccumuloClient().tableOperations()
          .removeIterator(OptUtil.getTableOpt(cl, shellState), name, scopes);
    } else {
      throw new IllegalArgumentException("No table or namespace specified");
    }
    return 0;
  }

  @Override
  public String description() {
    return "deletes a table-specific or namespace-specific iterator";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    nameOpt = new Option("n", "name", true, "iterator to delete");
    nameOpt.setArgName("itername");
    nameOpt.setRequired(true);

    allScopeOpt = new Option("all", "all-scopes", false, "remove from all scopes");
    mincScopeOpt = new Option(IteratorScope.minc.name(), "minor-compaction", false,
        "remove from minor compaction scope");
    majcScopeOpt = new Option(IteratorScope.majc.name(), "major-compaction", false,
        "remove from major compaction scope");
    scanScopeOpt =
        new Option(IteratorScope.scan.name(), "scan-time", false, "remove from scan scope");

    OptionGroup grp = new OptionGroup();
    grp.addOption(OptUtil.tableOpt("table to delete the iterator from"));
    grp.addOption(OptUtil.namespaceOpt("namespace to delete the iterator from"));
    o.addOptionGroup(grp);
    o.addOption(nameOpt);

    o.addOption(allScopeOpt);
    o.addOption(mincScopeOpt);
    o.addOption(majcScopeOpt);
    o.addOption(scanScopeOpt);

    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }
}

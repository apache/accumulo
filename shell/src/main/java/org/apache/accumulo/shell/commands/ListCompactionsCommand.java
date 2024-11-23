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

import static org.apache.accumulo.shell.commands.ListScansCommand.getServerOptValue;
import static org.apache.accumulo.shell.commands.ListScansCommand.rgRegexPredicate;
import static org.apache.accumulo.shell.commands.ListScansCommand.serverRegexPredicate;

import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ListCompactionsCommand extends Command {

  private Option serverOpt, tserverOption, rgOpt, disablePaginationOpt, filterOption;

  @Override
  public String description() {
    return "lists what compactions are currently running in accumulo. See the"
        + " accumulo.core.client.admin.ActiveCompaction javadoc for more information"
        + " about columns.";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {

    String filterText = null;

    final InstanceOperations instanceOps = shellState.getAccumuloClient().instanceOperations();

    final boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());

    Stream<String> activeCompactionStream;

    String serverValue = getServerOptValue(cl, serverOpt, tserverOption);
    if (serverValue != null || cl.hasOption(rgOpt)) {
      final var serverPredicate = serverRegexPredicate(serverValue);
      final var rgPredicate = rgRegexPredicate(cl.getOptionValue(rgOpt));
      activeCompactionStream =
          ActiveCompactionHelper.activeCompactions(instanceOps, rgPredicate, serverPredicate);
    } else {
      activeCompactionStream = ActiveCompactionHelper.activeCompactions(instanceOps);
    }

    if (cl.hasOption(filterOption.getOpt())) {
      filterText = ".*" + cl.getOptionValue(filterOption.getOpt()) + ".*";
    }

    if (filterText != null) {
      activeCompactionStream =
          activeCompactionStream.filter(Pattern.compile(filterText).asMatchPredicate());
    }

    activeCompactionStream = ActiveCompactionHelper.appendHeader(activeCompactionStream);

    shellState.printLines(activeCompactionStream.iterator(), paginate);

    return 0;
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();
    filterOption = new Option("f", "filter", true, "show only compactions that match the regex");
    opts.addOption(filterOption);

    serverOpt = new Option("s", "server", true,
        "tablet/compactor server regex to list compactions for. Regex will match against strings like <host>:<port>");
    serverOpt.setArgName("tablet/compactor server regex");
    opts.addOption(serverOpt);

    // Leaving here for backwards compatibility, same as serverOpt
    tserverOption = new Option("ts", "tabletServer", true,
        "tablet/compactor server regex to list compactions for");
    tserverOption.setArgName("tablet server");
    opts.addOption(tserverOption);

    rgOpt = new Option("rg", "resourceGroup", true,
        "tablet/compactor server resource group regex to list compactions for");
    rgOpt.setArgName("resource group");
    opts.addOption(rgOpt);

    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    opts.addOption(disablePaginationOpt);

    return opts;
  }

}

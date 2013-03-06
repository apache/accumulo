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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ListCompactionsCommand extends Command {

  private Option tserverOption, disablePaginationOpt;

  @Override
  public String description() {
    return "lists what compactions are currently running in accumulo. See the accumulo.core.client.admin.ActiveCompaciton javadoc for more information about columns.";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {

    List<String> tservers;

    final InstanceOperations instanceOps = shellState.getConnector().instanceOperations();

    final boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());

    if (cl.hasOption(tserverOption.getOpt())) {
      tservers = new ArrayList<String>();
      tservers.add(cl.getOptionValue(tserverOption.getOpt()));
    } else {
      tservers = instanceOps.getTabletServers();
    }

    shellState.printLines(new ActiveCompactionIterator(tservers, instanceOps), paginate);

    return 0;
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();

    tserverOption = new Option("ts", "tabletServer", true, "tablet server to list compactions for");
    tserverOption.setArgName("tablet server");
    opts.addOption(tserverOption);

    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    opts.addOption(disablePaginationOpt);

    return opts;
  }

}

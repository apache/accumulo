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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 *
 */
public class TabletServerStatusCommand extends Command {

  private Option tserverOption, disablePaginationOpt, allOption;

  @Override
  public String description() {
    return "get tablet servers status";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    List<Map<String,String>> tservers;

    final InstanceOperations instanceOps = shellState.getConnector().instanceOperations();

    final boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());

    if (cl.hasOption(tserverOption.getOpt())) {
      tservers = new ArrayList<>();
      for (Map<String,String> ts : instanceOps.getTabletServerStatus()) {
        if (ts.get("server").equals(cl.getOptionValue(tserverOption.getOpt()))) {
          tservers.add(ts);
        }
      }
    } else if (cl.hasOption(allOption.getOpt())) {
      tservers = instanceOps.getTabletServerStatus();
    } else {
      throw new MissingOptionException("Missing options");
    }

    shellState.printLines(new TabletServerStatusIterator(tservers, shellState), paginate);

    return 0;
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();

    tserverOption = new Option("ts", "tabletServer", true, "tablet server to find status");
    tserverOption.setArgName("tablet server");
    opts.addOption(tserverOption);

    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    opts.addOption(disablePaginationOpt);

    allOption = new Option("a", "all", false, "list tablet servers status");
    opts.addOption(allOption);

    return opts;
  }

}

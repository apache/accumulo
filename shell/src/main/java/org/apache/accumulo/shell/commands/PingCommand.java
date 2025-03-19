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
import java.util.List;

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class PingCommand extends Command {

  private Option serverOption, tserverOption, disablePaginationOpt;

  @Override
  public String description() {
    return "ping compactors, scan servers, or tablet servers";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {

    final List<String> servers = new ArrayList<>();

    final InstanceOperations instanceOps = shellState.getAccumuloClient().instanceOperations();

    final boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());

    if (cl.hasOption(tserverOption.getOpt())) {
      servers.add(cl.getOptionValue(tserverOption.getOpt()));
    } else if (cl.hasOption(serverOption.getOpt())) {
      servers.add(cl.getOptionValue(serverOption.getOpt()));
    } else {
      instanceOps.getServers(ServerId.Type.COMPACTOR)
          .forEach(s -> servers.add(s.toHostPortString()));
      instanceOps.getServers(ServerId.Type.SCAN_SERVER)
          .forEach(s -> servers.add(s.toHostPortString()));
      instanceOps.getServers(ServerId.Type.TABLET_SERVER)
          .forEach(s -> servers.add(s.toHostPortString()));
    }

    shellState.printLines(new PingIterator(servers, instanceOps), paginate);

    return 0;
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();

    serverOption =
        new Option("s", "server", true, "compactor, scan server, or tablet server address to ping");
    serverOption.setArgName("server address");
    opts.addOption(serverOption);

    // Leaving here for backwards compatibility
    tserverOption = new Option("ts", "tabletServer", true,
        "compactor, scan server, or tablet server address to ping");
    tserverOption.setArgName("tablet server");
    opts.addOption(tserverOption);

    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    opts.addOption(disablePaginationOpt);

    return opts;
  }

}

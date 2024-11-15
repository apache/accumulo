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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ListScansCommand extends Command {

  private Option serverOpt, rgOpt, disablePaginationOpt;

  @Override
  public String description() {
    return "lists what scans are currently running in accumulo. See the"
        + " accumulo.core.client.admin.ActiveScan javadoc for more information about columns.";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {

    final InstanceOperations instanceOps = shellState.getAccumuloClient().instanceOperations();
    final boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());
    final Set<ServerId> servers = new HashSet<>();

    if (cl.hasOption(serverOpt) || cl.hasOption(rgOpt)) {
      final var serverPredicate = serverRegexPredicate(cl, serverOpt);
      final var rgPredicate = rgRegexPredicate(cl, rgOpt);
      servers
          .addAll(instanceOps.getServers(ServerId.Type.SCAN_SERVER, rgPredicate, serverPredicate));
      servers.addAll(
          instanceOps.getServers(ServerId.Type.TABLET_SERVER, rgPredicate, serverPredicate));
    } else {
      servers.addAll(instanceOps.getServers(ServerId.Type.SCAN_SERVER));
      servers.addAll(instanceOps.getServers(ServerId.Type.TABLET_SERVER));
    }

    shellState.printLines(new ActiveScanIterator(servers, instanceOps), paginate);

    return 0;
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();

    serverOpt = new Option("s", "server", true, "tablet/scan server regex to list scans for");
    serverOpt.setArgName("tablet/scan server regex");
    opts.addOption(serverOpt);

    rgOpt = new Option("rg", "resourceGroup", true,
        "tablet/scan server resource group regex to list scans for");
    rgOpt.setArgName("resource group");
    opts.addOption(rgOpt);

    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    opts.addOption(disablePaginationOpt);

    return opts;
  }

  static BiPredicate<String,Integer> serverRegexPredicate(CommandLine cl, Option serverOpt) {
    return Optional.ofNullable(cl.getOptionValue(serverOpt))
        .map(regex -> (BiPredicate<String,
            Integer>) (h, p) -> Pattern.compile(regex).matcher(h + ":" + p).matches())
        .orElse((h, p) -> true);
  }

  static Predicate<String> rgRegexPredicate(CommandLine cl, Option rgOpt) {
    return Optional.ofNullable(cl.getOptionValue(rgOpt))
        .map(regex -> Pattern.compile(regex).asMatchPredicate()).orElse(rg -> true);
  }

}

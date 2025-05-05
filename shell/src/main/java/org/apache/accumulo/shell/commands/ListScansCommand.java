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
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.ScanType;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.util.DurationFormat;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ListScansCommand extends Command {

  private Option serverOpt, tserverOption, rgOpt, disablePaginationOpt;

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
    final List<ServerId> servers = new ArrayList<>();

    String serverValue = getServerOptValue(cl, serverOpt, tserverOption);
    if (serverValue != null || cl.hasOption(rgOpt)) {
      final var serverPredicate = serverRegexPredicate(serverValue);
      final var rgPredicate = rgRegexPredicate(cl.getOptionValue(rgOpt));
      servers
          .addAll(instanceOps.getServers(ServerId.Type.SCAN_SERVER, rgPredicate, serverPredicate));
      servers.addAll(
          instanceOps.getServers(ServerId.Type.TABLET_SERVER, rgPredicate, serverPredicate));
    } else {
      servers.addAll(instanceOps.getServers(ServerId.Type.SCAN_SERVER));
      servers.addAll(instanceOps.getServers(ServerId.Type.TABLET_SERVER));
    }

    Stream<String> activeScans = getActiveScans(instanceOps, servers);
    activeScans = appendHeader(activeScans);
    shellState.printLines(activeScans.iterator(), paginate);

    return 0;
  }

  private Stream<String> getActiveScans(InstanceOperations instanceOps, List<ServerId> servers) {
    List<List<ServerId>> partServerIds = Lists.partition(servers, 100);
    return partServerIds.stream().flatMap(ids -> {
      try {
        return instanceOps.getActiveScans(ids).stream().map(as -> {
          var dur = new DurationFormat(as.getAge(), "");
          var dur2 = new DurationFormat(as.getLastContactTime(), "");
          var server = as.getServerId();
          return (String.format(
              "%21s |%21s |%21s |%9s |%9s |%7s |%6s |%8s |%8s |%10s |%20s |%10s |%20s |%10s | %s",
              server.getResourceGroup(), server.toHostPortString(), as.getClient(), dur, dur2,
              as.getState(), as.getType(), as.getUser(), as.getTable(), as.getColumns(),
              as.getAuthorizations(), (as.getType() == ScanType.SINGLE ? as.getTablet() : "N/A"),
              as.getScanid(), as.getSsiList(), as.getSsio()));
        });
      } catch (AccumuloException | AccumuloSecurityException e) {
        return Stream.of("ERROR " + e.getMessage());
      }
    });
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();

    serverOpt = new Option("s", "server", true,
        "tablet/scan server regex to list scans for. Regex will match against strings like <host>:<port>");
    serverOpt.setArgName("tablet/scan server regex");
    opts.addOption(serverOpt);

    // Leaving here for backwards compatibility, same as serverOpt
    tserverOption = new Option("ts", "tabletServer", true, "tablet/scan server to list scans for");
    tserverOption.setArgName("tablet server");
    opts.addOption(tserverOption);

    rgOpt = new Option("rg", "resourceGroup", true,
        "tablet/scan server resource group regex to list scans for");
    rgOpt.setArgName("resource group");
    opts.addOption(rgOpt);

    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    opts.addOption(disablePaginationOpt);

    return opts;
  }

  static String getServerOptValue(CommandLine cl, Option serverOpt, Option tserverOption) {
    Preconditions.checkArgument(!(cl.hasOption(serverOpt) && cl.hasOption(tserverOption)),
        "serverOpt and tserverOption may not be both set at the same time.");
    return cl.hasOption(serverOpt) ? cl.getOptionValue(serverOpt)
        : cl.getOptionValue(tserverOption);
  }

  static BiPredicate<String,Integer> serverRegexPredicate(String serverRegex) {
    return Optional.ofNullable(serverRegex).map(regex -> Pattern.compile(regex).asMatchPredicate())
        .map(matcherPredicate -> (BiPredicate<String,
            Integer>) (h, p) -> matcherPredicate.test(h + ":" + p))
        .orElse((h, p) -> true);
  }

  static Predicate<String> rgRegexPredicate(String rgRegex) {
    return Optional.ofNullable(rgRegex).map(regex -> Pattern.compile(regex).asMatchPredicate())
        .orElse(rg -> true);
  }

  private static Stream<String> appendHeader(Stream<String> stream) {
    Stream<String> header = Stream.of(String.format(
        " %-21s| %-21s| %-21s| %-9s| %-9s| %-7s| %-6s|"
            + " %-8s| %-8s| %-10s| %-20s| %-10s| %-10s | %-20s | %s",
        "GROUP", "SERVER", "CLIENT", "AGE", "LAST", "STATE", "TYPE", "USER", "TABLE", "COLUMNS",
        "AUTHORIZATIONS", "TABLET", "SCAN ID", "ITERATORS", "ITERATOR OPTIONS"));
    return Stream.concat(header, stream);
  }
}

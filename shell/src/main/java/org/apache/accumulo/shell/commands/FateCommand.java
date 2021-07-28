/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.shell.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.KeeperException;

/**
 * Manage FATE transactions
 */
public class FateCommand extends Command {

  private Option statusOption;
  private Option disablePaginationOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, ParseException, KeeperException,
      InterruptedException, IOException {

    String[] args = cl.getArgs();

    if (args.length <= 0) {
      throw new ParseException("Must provide a command to execute");
    }
    String cmd = args[0];
    // Only get the Transaction IDs passed in from the command line.
    List<String> txids = new ArrayList<>(cl.getArgList().subList(1, args.length));

    if ("fail".equals(cmd)) {
      if (txids.isEmpty()) {
        throw new ParseException("Must provide transaction ID");
      }

      shellState.getAccumuloClient().instanceOperations().fateFail(txids);
    } else if ("delete".equals(cmd)) {
      if (txids.isEmpty()) {
        throw new ParseException("Must provide transaction ID");
      }

      shellState.getAccumuloClient().instanceOperations().fateDelete(txids);
    } else if ("list".equals(cmd) || "print".equals(cmd)) {
      // Parse TStatus filters for print display
      EnumSet<TStatus> filterStatus = null;
      if (cl.hasOption(statusOption.getOpt())) {
        filterStatus = EnumSet.noneOf(TStatus.class);
        String[] tstat = cl.getOptionValues(statusOption.getOpt());
        for (String element : tstat) {
          try {
            filterStatus.add(TStatus.valueOf(element));
          } catch (IllegalArgumentException iae) {
            shellState.getWriter().printf("Invalid transaction status name: %s%n", element);
            return 1;
          }
        }
      }

      String buffer =
          shellState.getAccumuloClient().instanceOperations().fatePrint(txids, filterStatus);
      shellState.printLines(Collections.singletonList(buffer).iterator(),
          !cl.hasOption(disablePaginationOpt.getOpt()));
    } else if ("dump".equals(cmd)) {
      shellState.getWriter()
          .println(shellState.getAccumuloClient().instanceOperations().fateDump(txids));
    } else {
      throw new ParseException("Invalid command option");
    }

    return 0;
  }

  @Override
  public String description() {
    return "manage FATE transactions";
  }

  @Override
  public String usage() {
    return getName() + " fail <txid>... | delete <txid>... | print [<txid>...] | dump [<txid>...]";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    statusOption = new Option("t", "status-type", true,
        "filter 'print' on the transaction status type(s) {NEW, IN_PROGRESS,"
            + " FAILED_IN_PROGRESS, FAILED, SUCCESSFUL}");
    statusOption.setArgs(Option.UNLIMITED_VALUES);
    statusOption.setOptionalArg(false);
    o.addOption(statusOption);
    disablePaginationOpt =
        new Option("np", "no-pagination", false, "disables pagination of output");
    o.addOption(disablePaginationOpt);
    return o;
  }

  @Override
  public int numArgs() {
    // Arg length varies between 1 to n
    return -1;
  }
}

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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.fate.FateTxId;
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

  private Option secretOption;
  private Option statusOption;
  private Option disablePaginationOpt;

  private long parseTxid(String s) {
    if (FateTxId.isFormatedTid(s)) {
      return FateTxId.fromString(s);
    } else {
      return Long.parseLong(s, 16);
    }
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, ParseException, KeeperException,
      InterruptedException, IOException {

    String[] args = cl.getArgs();
    if (args.length <= 0) {
      throw new ParseException("Must provide a command to execute");
    }
    String cmd = args[0];
    String secret = cl.getOptionValue(secretOption.getOpt());

    if ("fail".equals(cmd)) {
      if (args.length <= 1) {
        throw new ParseException("Must provide transaction ID");
      }

      shellState.getAccumuloClient().instanceOperations().fateFail(cl.getArgList(), secret);
    } else if ("delete".equals(cmd)) {
      if (args.length <= 1) {
        throw new ParseException("Must provide transaction ID");
      }

      shellState.getAccumuloClient().instanceOperations().fateDelete(cl.getArgList(), secret);
    } else if ("list".equals(cmd) || "print".equals(cmd)) {
      // Parse transaction ID filters for print display
      Set<Long> filterTxid = null;
      if (args.length >= 2) {
        filterTxid = new HashSet<>(args.length);
        for (int i = 1; i < args.length; i++) {
          try {
            Long val = parseTxid(args[i]);
            filterTxid.add(val);
          } catch (NumberFormatException nfe) {
            // Failed to parse, will exit instead of displaying everything since the intention was
            // to potentially filter some data
            shellState.getWriter().printf("Invalid transaction ID format: %s%n", args[i]);
            return 1;
          }
        }
      }

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

      String buffer = shellState.getAccumuloClient().instanceOperations().fatePrint(cl.getArgList(),
          filterTxid, filterStatus, secret);
      shellState.printLines(Collections.singletonList(buffer).iterator(),
          !cl.hasOption(disablePaginationOpt.getOpt()));
    } else if ("dump".equals(cmd)) {
      shellState.getWriter().println(
          shellState.getAccumuloClient().instanceOperations().fateDump(cl.getArgList(), secret));
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
    secretOption = new Option("s", "secret", true, "specify the instance secret to use");
    secretOption.setOptionalArg(false);
    o.addOption(secretOption);
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

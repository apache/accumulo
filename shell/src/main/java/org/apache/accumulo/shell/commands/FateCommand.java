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
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TransactionStatus;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ManagerClient;
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
          throws ParseException, KeeperException, InterruptedException, IOException, AccumuloException, AccumuloSecurityException {
    String[] args = cl.getArgs();

    if (args.length <= 0) {
      throw new ParseException("Must provide a command to execute");
    }
    String cmd = args[0];

    // Only get the Transaction IDs passed in from the command line.
    List<String> txids = new ArrayList<>(cl.getArgList().subList(1, args.length));
      if ("cancel-submitted".equals(cmd)) {
          validateArgs(txids);
          cancelSubmittedTxs(shellState, args);
      } else if ("fail".equals(cmd)) {
      validateArgs(txids);
      shellState.getAccumuloClient().instanceOperations().fateFail(txids);
    } else if ("delete".equals(cmd)) {
      validateArgs(txids);
      shellState.getAccumuloClient().instanceOperations().fateDelete(txids);
    } else if ("list".equals(cmd) || "print".equals(cmd)) {
          // Parse TStatus filters for print display
          List<String> filterStatus = new ArrayList<>();
          if (cl.hasOption(statusOption.getOpt())) {
              filterStatus = Arrays.asList(cl.getOptionValues(statusOption.getOpt()));
          }

          StringBuilder sb = new StringBuilder(8096);
          Formatter fmt = new Formatter(sb);
          List<TransactionStatus> txStatuses =
                  shellState.getAccumuloClient().instanceOperations().fateStatus(txids, filterStatus);

          for (TransactionStatus txStatus : txStatuses) {
              fmt.format(
                      "txid: %s  status: %-18s  op: %-15s  locked: %-15s locking: %-15s top: %-15s created: %s%n",
                      txStatus.getTxid(), txStatus.getStatus(), txStatus.getDebug(), txStatus.getHeldLocks(),
                      txStatus.getWaitingLocks(), txStatus.getTop(), txStatus.getTimeCreatedFormatted());
          }
          fmt.format(" %s transactions", txStatuses.size());

          shellState.printLines(Collections.singletonList(sb.toString()).iterator(),
                  !cl.hasOption(disablePaginationOpt.getOpt()));
      } else if ("dump".equals(cmd)) {
          List<TransactionStatus> txStatuses =
                  shellState.getAccumuloClient().instanceOperations().fateStatus(txids, null);

          if (txStatuses.isEmpty())
              shellState.getWriter().println(" No transactions to dump");

          for (var tx : txStatuses) {
              shellState.getWriter().println(tx.getStackInfo());
          }
      } else {
          throw new ParseException("Invalid command option");
      }
      return 0;
  }

  private void cancelSubmittedTxs(final Shell shellState, List<String> txids)
      throws AccumuloException, AccumuloSecurityException {
      ClientContext context = shellState.getContext();
      for (String txid : txids) {
          Long txid = Long.parseLong(args[i]);
          shellState.getWriter().flush();
          String line = shellState.getReader().readLine("Cancel FaTE Tx " + txid + " (yes|no)? ");
          boolean cancelTx =
                  line != null && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"));
          if (cancelTx) {
              boolean cancelled = ManagerClient.cancelFateOperation(context, txid);
              if (cancelled) {
                  shellState.getWriter()
                          .println("FaTE transaction " + txid + " was cancelled or already completed.");
              } else {
                  shellState.getWriter()
                          .println("FaTE transaction " + txid + " was not cancelled, status may have changed.");
              }
          } else {
              shellState.getWriter().println("Not cancelling FaTE transaction " + txid);
          }
      }
  }

  private void validateArgs(List<String> txids) throws ParseException {
    if (txids.size() <= 0) {
      throw new ParseException("Must provide transaction ID");
    }
  }

  @Override
  public String description() {
    return "manage FATE transactions";
  }

  @Override
  public String usage() {
    return getName()
        + "cancel-submitted <txid> | fail <txid>... | delete <txid>... | print [<txid>...] | dump [<txid>...]";
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

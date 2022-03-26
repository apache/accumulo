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
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.KeeperException;

/**
 * Manage FATE transactions
 */
public class FateCommand extends Command {

  private Option cancel;
  private Option delete;
  private Option dump;
  private Option fail;
  private Option list;
  private Option print;
  private Option statusOption;
  private Option disablePaginationOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws ParseException, KeeperException, InterruptedException, IOException, AccumuloException,
      AccumuloSecurityException {

    if (cl.hasOption(cancel.getOpt())) {
      String[] txids = cl.getOptionValues(cancel.getOpt());
      validateArgs(txids);
      cancelSubmittedTxs(shellState, txids);
    } else if (cl.hasOption(fail.getOpt())) {
      String[] txids = cl.getOptionValues(fail.getOpt());
      validateArgs(txids);
      failTx(shellState, txids);
    } else if (cl.hasOption(delete.getOpt())) {
      String[] txids = cl.getOptionValues(delete.getOpt());
      validateArgs(txids);
      deleteTx(shellState, txids);
    } else if (cl.hasOption(list.getOpt())) {
      printTx(shellState, cl.getOptionValues(list.getOpt()), cl,
          cl.hasOption(statusOption.getOpt()));
    } else if (cl.hasOption(print.getOpt())) {
      printTx(shellState, cl.getOptionValues(print.getOpt()), cl,
          cl.hasOption(statusOption.getOpt()));
    } else if (cl.hasOption(dump.getOpt())) {
      dumpTx(shellState, cl.getOptionValues(dump.getOpt()));
    } else {
      throw new ParseException("Invalid command option");
    }
    return 0;
  }

  public void failTx(Shell shellState, String[] args) throws AccumuloException {
    shellState.getAccumuloClient().instanceOperations().fateFail(Arrays.asList(args));
  }

  protected void deleteTx(Shell shellState, String[] args) throws AccumuloException {
    shellState.getAccumuloClient().instanceOperations().fateDelete(Arrays.asList(args));
  }

  protected void dumpTx(Shell shellState, String[] args) throws AccumuloException {

    if (args == null) {
      args = new String[] {};
    }

    List<TransactionStatus> txStatuses =
        shellState.getAccumuloClient().instanceOperations().fateStatus(Arrays.asList(args), null);

    if (txStatuses.isEmpty())
      shellState.getWriter().println(" No transactions to dump");

    for (var tx : txStatuses) {
      shellState.getWriter().println(tx.getStackInfo());
    }
  }

  protected void printTx(Shell shellState, String[] args, CommandLine cl, boolean printStatus)
      throws IOException, AccumuloException {
    // Parse TStatus filters for print display
    List<String> filterStatus = new ArrayList<>();
    if (cl.hasOption(statusOption.getOpt())) {
      filterStatus = Arrays.asList(cl.getOptionValues(statusOption.getOpt()));
    }

    StringBuilder sb = new StringBuilder(8096);
    Formatter fmt = new Formatter(sb);

    if (args == null) {
      args = new String[] {};
    }

    List<TransactionStatus> txStatuses = shellState.getAccumuloClient().instanceOperations()
        .fateStatus(Arrays.asList(args), filterStatus);

    for (TransactionStatus txStatus : txStatuses) {
      fmt.format(
          "txid: %s  status: %-18s  op: %-15s  locked: %-15s locking: %-15s top: %-15s created: %s%n",
          txStatus.getTxid(), txStatus.getStatus(), txStatus.getDebug(), txStatus.getHeldLocks(),
          txStatus.getWaitingLocks(), txStatus.getTop(), txStatus.getTimeCreatedFormatted());
    }
    fmt.format(" %s transactions", txStatuses.size());

    shellState.printLines(Collections.singletonList(sb.toString()).iterator(),
        !cl.hasOption(disablePaginationOpt.getOpt()));
  }

  protected void cancelSubmittedTxs(final Shell shellState, String[] args)
      throws AccumuloException, AccumuloSecurityException {
    ClientContext context = shellState.getContext();
    
    for (String arg : args) {
      long txid = Long.parseLong(arg, 16);
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

  private void validateArgs(String[] args) throws ParseException {
    if (args.length < 1) {
      throw new ParseException("Must provide transaction ID");
    }
  }

  @Override
  public String description() {
    return "manage FATE transactions";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    OptionGroup commands = new OptionGroup();
    cancel =
        new Option("cancel", "cancel-submitted", true, "cancel new or submitted FaTE transactions");
    cancel.setArgName("txid");
    cancel.setArgs(Option.UNLIMITED_VALUES);
    cancel.setOptionalArg(false);

    fail = new Option("fail", "fail", true,
        "Transition FaTE transaction status to FAILED_IN_PROGRESS (requires Manager to be down)");
    fail.setArgName("txid");
    fail.setArgs(Option.UNLIMITED_VALUES);
    fail.setOptionalArg(false);

    delete = new Option("delete", "delete", true,
        "delete locks associated with FaTE transactions (requires Manager to be down)");
    delete.setArgName("txid");
    delete.setArgs(Option.UNLIMITED_VALUES);
    delete.setOptionalArg(false);

    list = new Option("list", "list", true, "print FaTE transaction information");
    list.setArgName("txid");
    list.setArgs(Option.UNLIMITED_VALUES);
    list.setOptionalArg(true);

    print = new Option("print", "print", true, "print FaTE transaction information");
    print.setArgName("txid");
    print.setArgs(Option.UNLIMITED_VALUES);
    print.setOptionalArg(true);

    dump = new Option("dump", "dump", true, "dump FaTE transaction information details");
    dump.setArgName("txid");
    dump.setArgs(Option.UNLIMITED_VALUES);
    dump.setOptionalArg(true);

    commands.addOption(cancel);
    commands.addOption(fail);
    commands.addOption(delete);
    commands.addOption(list);
    commands.addOption(print);
    commands.addOption(dump);
    o.addOptionGroup(commands);

    statusOption = new Option("t", "status-type", true,
        "filter 'print' on the transaction status type(s) {NEW, SUBMITTED, IN_PROGRESS,"
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

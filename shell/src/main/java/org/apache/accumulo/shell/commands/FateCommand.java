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

import static org.apache.accumulo.fate.FateTxIdUtil.parseHexLongFromString;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.FateTransaction;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.FateTransactionImpl;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
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

  protected String getZKRoot(ClientContext context) {
    return context.getZooKeeperRoot();
  }

  synchronized ZooReaderWriter getZooReaderWriter(ClientContext context, String secret) {
    if (secret == null) {
      secret = SiteConfiguration.auto().get(Property.INSTANCE_SECRET);
    }
    return context.getZooReader().asWriter(secret);
  }

  protected ZooStore<FateCommand> getZooStore(String fateZkPath, ZooReaderWriter zrw)
      throws KeeperException, InterruptedException {
    return new ZooStore<>(fateZkPath, zrw);
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws ParseException, KeeperException, InterruptedException, IOException, AccumuloException,
      AccumuloSecurityException {
    boolean failedCommand = false;

    if (cl.hasOption(cancel.getOpt())) {
      String[] txids = cl.getOptionValues(cancel.getOpt());
      validateArgs(txids);
      failedCommand = cancelSubmittedTxs(shellState, txids);
    } else if (cl.hasOption(fail.getOpt())) {
      String[] txids = cl.getOptionValues(fail.getOpt());
      validateArgs(txids);
      failedCommand = failTx(shellState, List.of(txids));
    } else if (cl.hasOption(delete.getOpt())) {
      String[] txids = cl.getOptionValues(delete.getOpt());
      validateArgs(txids);
      failedCommand = deleteTx(shellState, txids);
    } else if (cl.hasOption(list.getOpt())) {
      printTx(shellState, cl.getOptionValues(list.getOpt()), cl);
    } else if (cl.hasOption(print.getOpt())) {
      printTx(shellState, cl.getOptionValues(print.getOpt()), cl);
    } else if (cl.hasOption(dump.getOpt())) {
      String output = dumpTx(shellState, cl.getOptionValues(dump.getOpt()));
      System.out.println(output);
    } else {
      throw new ParseException("Invalid command option");
    }

    return failedCommand ? 1 : 0;
  }

  protected boolean deleteTx(Shell shellState, String[] txids) throws AccumuloException {
    var allRunningTx = shellState.getAccumuloClient().instanceOperations().getFateTransactions();
    Set<String> txSet = new HashSet<>(Arrays.asList(txids));
    // TODO delete txIds passed in
    return true;
  }

  protected String dumpTx(Shell shellState, String[] txids) throws AccumuloException {
    return dumpTx(shellState.getAccumuloClient().instanceOperations().getFateTransactions(), txids);
  }

  // visible for testing
  protected String dumpTx(Set<FateTransaction> allRunningTx, String[] txids)
      throws AccumuloException {
    if (txids == null) {
      txids = new String[] {};
    }

    Set<String> userProvidedTx = new HashSet<>(Arrays.asList(txids));
    Set<FateTransactionImpl> txToDump = getTransactions(allRunningTx, userProvidedTx, List.of());

    if (txToDump.isEmpty())
      return " No transactions to dump";

    StringBuilder sb = new StringBuilder();
    for (FateTransaction tx : txToDump) {
      sb.append(tx.getStackInfo());
    }
    return sb.toString();
  }

  protected void printTx(Shell shellState, String[] txids, CommandLine cl)
      throws IOException, AccumuloException {
    // Parse TStatus filters for print display
    final List<String> filterStatus;
    if (cl.hasOption(statusOption.getOpt())) {
      filterStatus = Arrays.asList(cl.getOptionValues(statusOption.getOpt()));
    } else {
      filterStatus = Collections.emptyList();
    }

    if (txids == null) {
      txids = new String[] {};
    }

    Set<String> userProvidedIds = new HashSet<>(Arrays.asList(txids));
    var allRunningTx = shellState.getAccumuloClient().instanceOperations().getFateTransactions();

    var transactionsToPrint = getTransactions(allRunningTx, userProvidedIds, filterStatus);

    StringBuilder sb = new StringBuilder(8096);
    Formatter fmt = new Formatter(sb);
    for (FateTransaction txStatus : transactionsToPrint) {
      fmt.format(
          "txid: %s  status: %-18s  op: %-15s  locked: %-15s locking: %-15s top: %-15s created: %s%n",
          txStatus.getId(), txStatus.getStatus(), txStatus.getDebug(), txStatus.getHeldLocks(),
          txStatus.getWaitingLocks(), txStatus.getTop(), txStatus.getTimeCreatedFormatted());
    }
    fmt.format(" %s transactions", transactionsToPrint.size());

    shellState.printLines(Collections.singletonList(sb.toString()).iterator(),
        !cl.hasOption(disablePaginationOpt.getOpt()));
  }

  // visible for testing
  protected Set<FateTransactionImpl> getTransactions(Set<FateTransaction> allRunningTx,
      Set<String> userProvidedIds, List<String> filterStatus) {
    Set<Long> userProvidedHexIds = new TreeSet<>();
    userProvidedIds.forEach(userTxId -> userProvidedHexIds.add(parseHexLongFromString(userTxId)));
    // cast to allow for sorted order
    Stream<FateTransactionImpl> stream = allRunningTx.stream().map(tx -> (FateTransactionImpl) tx);

    // if user options are empty, return everything
    if (filterStatus.isEmpty() && userProvidedHexIds.isEmpty()) {
      return stream.collect(Collectors.toSet());
    }

    if (filterStatus.isEmpty()) {
      return stream.filter(tx -> userProvidedHexIds.contains(tx.getId().canonical()))
          .collect(Collectors.toSet());
    }
    if (userProvidedHexIds.isEmpty()) {
      return stream.filter(tx -> filterStatus.contains(tx.getStatus().toString()))
          .collect(Collectors.toSet());
    }

    // gather all the running TXs that the user asked for
    return stream.filter(tx -> userProvidedHexIds.contains(tx.getId().canonical()))
        .filter(tx -> filterStatus.contains(tx.getStatus().toString())).collect(Collectors.toSet());
  }

  protected boolean cancelSubmittedTxs(final Shell shellState, String[] args)
      throws AccumuloException, AccumuloSecurityException {
    ClientContext context = shellState.getContext();
    for (int i = 1; i < args.length; i++) {
      long txid = Long.parseLong(args[i], 16);
      shellState.getWriter().flush();
      String line = shellState.getReader().readLine("Cancel FaTE Tx " + txid + " (yes|no)? ");
      boolean cancelTx =
          line != null && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"));
      if (cancelTx) {
        boolean cancelled = cancelFateOperation(context, txid, shellState);
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
    return true;
  }

  private boolean cancelFateOperation(ClientContext context, long txid, Shell shellState)
      throws AccumuloException {
    FateService.Client client = null;
    try {
      client = ThriftClientTypes.FATE.getConnectionWithRetry(context);
      return client.cancelFateOperation(TraceUtil.traceInfo(), context.rpcCreds(), txid);
    } catch (Exception e) {
      shellState.getWriter()
          .println("ManagerClient request failed, retrying. Cause: " + e.getMessage());
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        ThriftUtil.close(client, context);
    }
  }

  public boolean failTx(Shell shellState, List<String> txids) throws AccumuloException {
    shellState.getAccumuloClient().instanceOperations().getFateTransactions().stream()
        .filter(fateTransaction -> txids.contains(fateTransaction.getId().canonical().toString()))
        .forEach(FateTransaction::fail);
    return true;
  }

  private void validateArgs(String[] txids) throws ParseException {
    if (txids.length < 1) {
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

    list = new Option("list", "list", true,
        "print FaTE transaction information. Filter on id(s) with FATE[id] or id list ");
    list.setArgName("txid");
    list.setArgs(Option.UNLIMITED_VALUES);
    list.setOptionalArg(true);

    print = new Option("print", "print", true,
        "print FaTE transaction information. Filter on id(s) with FATE[id] or id list ");
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

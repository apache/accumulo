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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.tables.TableMap;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.FateTxId;
import org.apache.accumulo.fate.ReadOnlyRepo;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.commands.fateCommand.FateSummaryReport;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Manage FATE transactions
 */
public class FateCommand extends Command {

  private final static Logger LOG = LoggerFactory.getLogger(FateCommand.class);

  // this class serializes references to interfaces with the concrete class name
  private static class InterfaceSerializer<T> implements JsonSerializer<T> {
    @Override
    public JsonElement serialize(T link, Type type, JsonSerializationContext context) {
      JsonElement je = context.serialize(link, link.getClass());
      JsonObject jo = new JsonObject();
      jo.add(link.getClass().getName(), je);
      return jo;
    }
  }

  // the purpose of this class is to be serialized as JSon for display
  public static class ByteArrayContainer {
    public String asUtf8;
    public String asBase64;

    ByteArrayContainer(byte[] ba) {
      asUtf8 = new String(ba, UTF_8);
      asBase64 = Base64.getUrlEncoder().encodeToString(ba);
    }
  }

  // serialize byte arrays in human and machine readable ways
  private static class ByteArraySerializer implements JsonSerializer<byte[]> {
    @Override
    public JsonElement serialize(byte[] link, Type type, JsonSerializationContext context) {
      return context.serialize(new ByteArrayContainer(link));
    }
  }

  // the purpose of this class is to be serialized as JSon for display
  public static class FateStack {
    String txIdString;
    List<ReadOnlyRepo<FateCommand>> stack;

    FateStack(Long txid, List<ReadOnlyRepo<FateCommand>> stack) {
      this.txIdString = FastFormat.toHexString(txid);
      this.stack = stack;
    }
  }

  private Option cancel;
  private Option delete;
  private Option dump;
  private Option fail;
  private Option list;
  private Option print;
  private Option summary;
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
    ClientContext context = shellState.getContext();
    boolean failedCommand = false;

    AdminUtil<FateCommand> admin = new AdminUtil<>(false);

    String zkRoot = getZKRoot(context);
    String fatePath = zkRoot + Constants.ZFATE;
    var managerLockPath = ServiceLock.path(zkRoot + Constants.ZMANAGER_LOCK);
    var tableLocksPath = ServiceLock.path(zkRoot + Constants.ZTABLE_LOCKS);
    ZooReaderWriter zk = getZooReaderWriter(context, cl.getOptionValue(secretOption.getOpt()));
    ZooStore<FateCommand> zs = getZooStore(fatePath, zk);

    if (cl.hasOption(cancel.getOpt())) {
      String[] txids = cl.getOptionValues(cancel.getOpt());
      validateArgs(txids);
      failedCommand = cancelSubmittedTxs(shellState, txids);
    } else if (cl.hasOption(fail.getOpt())) {
      String[] txids = cl.getOptionValues(fail.getOpt());
      validateArgs(txids);
      failedCommand = failTx(admin, zs, zk, managerLockPath, txids);
    } else if (cl.hasOption(delete.getOpt())) {
      String[] txids = cl.getOptionValues(delete.getOpt());
      validateArgs(txids);
      failedCommand = deleteTx(admin, zs, zk, managerLockPath, txids);
    } else if (cl.hasOption(list.getOpt())) {
      printTx(shellState, admin, zs, zk, tableLocksPath, cl.getOptionValues(list.getOpt()), cl);
    } else if (cl.hasOption(print.getOpt())) {
      printTx(shellState, admin, zs, zk, tableLocksPath, cl.getOptionValues(print.getOpt()), cl);
    } else if (cl.hasOption(summary.getOpt())) {
      summarizeTx(shellState, admin, zs, zk, tableLocksPath, cl.getOptionValues(summary.getOpt()),
          cl);
    } else if (cl.hasOption(dump.getOpt())) {
      String output = dumpTx(zs, cl.getOptionValues(dump.getOpt()));
      System.out.println(output);
    } else {
      throw new ParseException("Invalid command option");
    }

    return failedCommand ? 1 : 0;
  }

  String dumpTx(ZooStore<FateCommand> zs, String[] args) {
    List<Long> txids;
    if (args.length == 1) {
      txids = zs.list();
    } else {
      txids = new ArrayList<>();
      for (int i = 1; i < args.length; i++) {
        txids.add(parseTxid(args[i]));
      }
    }

    Gson gson = new GsonBuilder()
        .registerTypeAdapter(ReadOnlyRepo.class, new InterfaceSerializer<>())
        .registerTypeAdapter(Repo.class, new InterfaceSerializer<>())
        .registerTypeAdapter(byte[].class, new ByteArraySerializer()).setPrettyPrinting().create();

    List<FateStack> txStacks = new ArrayList<>();
    for (Long txid : txids) {
      List<ReadOnlyRepo<FateCommand>> repoStack = zs.getStack(txid);
      txStacks.add(new FateStack(txid, repoStack));
    }

    return gson.toJson(txStacks);
  }

  protected void printTx(Shell shellState, AdminUtil<FateCommand> admin, ZooStore<FateCommand> zs,
      ZooReaderWriter zk, ServiceLock.ServiceLockPath tableLocksPath, String[] args, CommandLine cl)
      throws InterruptedException, KeeperException, IOException {
    // Parse transaction ID filters for print display
    Set<Long> filterTxid = new HashSet<>();
    if (args != null && args.length >= 1) {
      for (int i = 0; i < args.length; i++) {
        if (!args[i].isEmpty()) {
          Long val = parseTxid(args[i]);
          filterTxid.add(val);
        }
      }
    }

    // Parse TStatus filters for print display
    EnumSet<TStatus> statusFilter = getCmdLineStatusFilters(cl);

    StringBuilder buf = new StringBuilder(8096);
    Formatter fmt = new Formatter(buf);
    admin.print(zs, zk, tableLocksPath, fmt, filterTxid, statusFilter);
    shellState.printLines(Collections.singletonList(buf.toString()).iterator(),
        !cl.hasOption(disablePaginationOpt.getOpt()));
  }

  protected void summarizeTx(Shell shellState, AdminUtil<FateCommand> admin,
      ZooStore<FateCommand> zs, ZooReaderWriter zk, ServiceLockPath tableLocksPath, String[] args,
      CommandLine cl) throws InterruptedException, AccumuloException, AccumuloSecurityException,
      KeeperException, IOException {

    var transactions = admin.getStatus(zs, zk, tableLocksPath, null, null);

    // build id map - relies on unique ids for tables and namespaces
    // used to look up the names of either table or namespace by id.
    Map<TableId,String> tidToNameMap = new TableMap(shellState.getContext()).getIdtoNameMap();
    Map<String,String> idsToNameMap = new HashMap<>(tidToNameMap.size() * 2);
    tidToNameMap.forEach((tid, name) -> idsToNameMap.put(tid.canonical(), "t:" + name));
    shellState.getContext().namespaceOperations().namespaceIdMap().forEach((name, nsid) -> {
      String prev = idsToNameMap.put(nsid, "ns:" + name);
      if (prev != null) {
        LOG.warn("duplicate id found for table / namespace id. table name: {}, namespace name: {}",
            prev, name);
      }
    });

    EnumSet<TStatus> statusFilter = getCmdLineStatusFilters(cl);

    FateSummaryReport report = new FateSummaryReport(idsToNameMap, statusFilter);

    // gather statistics
    transactions.getTransactions().forEach(report::gatherTxnStatus);
    if (Arrays.asList(cl.getArgs()).contains("json")) {
      shellState.printLines(Collections.singletonList(report.toJson()).iterator(),
          !cl.hasOption(disablePaginationOpt.getOpt()));
    } else {
      // print the formatted report by lines to allow pagination
      shellState.printLines(report.formatLines().iterator(),
          !cl.hasOption(disablePaginationOpt.getOpt()));
    }
  }

  protected boolean deleteTx(AdminUtil<FateCommand> admin, ZooStore<FateCommand> zs,
      ZooReaderWriter zk, ServiceLockPath zLockManagerPath, String[] args)
      throws InterruptedException, KeeperException {
    for (int i = 1; i < args.length; i++) {
      if (admin.prepDelete(zs, zk, zLockManagerPath, args[i])) {
        admin.deleteLocks(zk, zLockManagerPath, args[i]);
      } else {
        System.out.printf("Could not delete transaction: %s%n", args[i]);
        return false;
      }
    }
    return true;
  }

  private void validateArgs(String[] args) throws ParseException {
    if (args.length < 1) {
      throw new ParseException("Must provide transaction ID");
    }
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

  private static boolean cancelFateOperation(ClientContext context, long txid,
      final Shell shellState) throws AccumuloException, AccumuloSecurityException {
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

  public boolean failTx(AdminUtil<FateCommand> admin, ZooStore<FateCommand> zs, ZooReaderWriter zk,
      ServiceLockPath managerLockPath, String[] args) {
    boolean success = true;
    for (int i = 1; i < args.length; i++) {
      if (!admin.prepFail(zs, zk, managerLockPath, args[i])) {
        System.out.printf("Could not fail transaction: %s%n", args[i]);
        return !success;
      }
    }
    return success;
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

    summary =
        new Option("summary", "summary", true, "print a summary of FaTE transaction information");
    summary.setArgName("--json");
    summary.setOptionalArg(true);

    dump = new Option("dump", "dump", true, "dump FaTE transaction information details");
    dump.setArgName("txid");
    dump.setArgs(Option.UNLIMITED_VALUES);
    dump.setOptionalArg(true);

    commands.addOption(cancel);
    commands.addOption(fail);
    commands.addOption(delete);
    commands.addOption(list);
    commands.addOption(print);
    commands.addOption(summary);
    commands.addOption(dump);
    o.addOptionGroup(commands);

    secretOption = new Option("s", "secret", true, "specify the instance secret to use");
    secretOption.setOptionalArg(false);
    o.addOption(secretOption);
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

  /**
   * If provided on the command line, get the TStatus values provided.
   *
   * @param cl
   *          the command line
   * @return a set of status filters, or an empty set if none provides
   */
  private EnumSet<TStatus> getCmdLineStatusFilters(CommandLine cl) {
    EnumSet<TStatus> statusFilter = null;
    if (cl.hasOption(statusOption.getOpt())) {
      statusFilter = EnumSet.noneOf(TStatus.class);
      String[] tstat = cl.getOptionValues(statusOption.getOpt());
      for (String element : tstat) {
        statusFilter.add(TStatus.valueOf(element));
      }
    }
    return statusFilter;
  }

}

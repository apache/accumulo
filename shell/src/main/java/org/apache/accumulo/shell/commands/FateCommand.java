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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.zookeeper.ServiceLock.ServiceLockPath;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.FateTxId;
import org.apache.accumulo.fate.ReadOnlyRepo;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.KeeperException;

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
    String txid;
    List<ReadOnlyRepo<FateCommand>> stack;

    FateStack(Long txid, List<ReadOnlyRepo<FateCommand>> stack) {
      this.txid = String.format("%016x", txid);
      this.stack = stack;
    }
  }

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
      throws ParseException, KeeperException, InterruptedException, IOException, AccumuloException {
    ClientContext context = shellState.getContext();
    var siteConfig = SiteConfiguration.auto();
    String[] args = cl.getArgs();
    if (args.length <= 0) {
      throw new ParseException("Must provide a command to execute");
    }
    String cmd = args[0];
    boolean failedCommand = false;

    AdminUtil<FateCommand> admin = new AdminUtil<>(false);

    String fatePath = context.getZooKeeperRoot() + Constants.ZFATE;
    var managerLockPath = ServiceLock.path(context.getZooKeeperRoot() + Constants.ZMANAGER_LOCK);
    var tableLocksPath = ServiceLock.path(context.getZooKeeperRoot() + Constants.ZTABLE_LOCKS);
    ZooReaderWriter zk =
        getZooReaderWriter(context, siteConfig, cl.getOptionValue(secretOption.getOpt()));
    ZooStore<FateCommand> zs = new ZooStore<>(fatePath, zk);

    if ("fail".equals(cmd)) {
      validateArgs(args);
      failedCommand = failTx(admin, zs, zk, managerLockPath, args);
    } else if ("delete".equals(cmd)) {
      validateArgs(args);
      failedCommand = deleteTx(admin, zs, zk, managerLockPath, args);
    } else if ("list".equals(cmd) || "print".equals(cmd)) {
      printTx(shellState, admin, zs, zk, tableLocksPath, args, cl,
          cl.hasOption(statusOption.getOpt()));
    } else if ("dump".equals(cmd)) {
      String output = dumpTx(zs, args);
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

  private void printTx(Shell shellState, AdminUtil<FateCommand> admin, ZooStore<FateCommand> zs,
      ZooReaderWriter zk, ServiceLock.ServiceLockPath tableLocksPath, String[] args, CommandLine cl,
      boolean printStatus) throws InterruptedException, KeeperException, IOException {
    // Parse transaction ID filters for print display
    Set<Long> filterTxid = null;
    if (args.length >= 2) {
      filterTxid = new HashSet<>(args.length);
      for (int i = 1; i < args.length; i++) {
        Long val = parseTxid(args[i]);
        filterTxid.add(val);
      }
    }

    // Parse TStatus filters for print display
    EnumSet<TStatus> filterStatus = null;
    if (printStatus) {
      filterStatus = EnumSet.noneOf(TStatus.class);
      String[] tstat = cl.getOptionValues(statusOption.getOpt());
      for (String element : tstat) {
        filterStatus.add(TStatus.valueOf(element));
      }
    }

    StringBuilder buf = new StringBuilder(8096);
    Formatter fmt = new Formatter(buf);
    admin.print(zs, zk, tableLocksPath, fmt, filterTxid, filterStatus);
    shellState.printLines(Collections.singletonList(buf.toString()).iterator(),
        !cl.hasOption(disablePaginationOpt.getOpt()));
  }

  private boolean deleteTx(AdminUtil<FateCommand> admin, ZooStore<FateCommand> zs,
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
    if (args.length <= 1) {
      throw new ParseException("Must provide transaction ID");
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

  synchronized ZooReaderWriter getZooReaderWriter(ClientContext context,
      SiteConfiguration siteConfig, String secret) {

    if (secret == null) {
      secret = siteConfig.get(Property.INSTANCE_SECRET);
    }

    return context.getZooReader().asWriter(secret);
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

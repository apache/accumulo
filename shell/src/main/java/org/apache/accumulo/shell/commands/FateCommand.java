/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.shell.commands;

import static java.nio.charset.StandardCharsets.UTF_8;

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
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ReadOnlyRepo;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
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
 *
 */
public class FateCommand extends Command {

  private static final String SCHEME = "digest";

  private static final String USER = "accumulo";

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

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws ParseException, KeeperException, InterruptedException,
      IOException {
    Instance instance = shellState.getInstance();
    String[] args = cl.getArgs();
    if (args.length <= 0) {
      throw new ParseException("Must provide a command to execute");
    }
    String cmd = args[0];
    boolean failedCommand = false;

    AdminUtil<FateCommand> admin = new AdminUtil<>(false);

    String path = ZooUtil.getRoot(instance) + Constants.ZFATE;
    String masterPath = ZooUtil.getRoot(instance) + Constants.ZMASTER_LOCK;
    IZooReaderWriter zk = getZooReaderWriter(shellState.getInstance(), cl.getOptionValue(secretOption.getOpt()));
    ZooStore<FateCommand> zs = new ZooStore<>(path, zk);

    if ("fail".equals(cmd)) {
      if (args.length <= 1) {
        throw new ParseException("Must provide transaction ID");
      }
      for (int i = 1; i < args.length; i++) {
        if (!admin.prepFail(zs, zk, masterPath, args[i])) {
          System.out.printf("Could not fail transaction: %s%n", args[i]);
          failedCommand = true;
        }
      }
    } else if ("delete".equals(cmd)) {
      if (args.length <= 1) {
        throw new ParseException("Must provide transaction ID");
      }
      for (int i = 1; i < args.length; i++) {
        if (admin.prepDelete(zs, zk, masterPath, args[i])) {
          admin.deleteLocks(zs, zk, ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS, args[i]);
        } else {
          System.out.printf("Could not delete transaction: %s%n", args[i]);
          failedCommand = true;
        }
      }
    } else if ("list".equals(cmd) || "print".equals(cmd)) {
      // Parse transaction ID filters for print display
      Set<Long> filterTxid = null;
      if (args.length >= 2) {
        filterTxid = new HashSet<>(args.length);
        for (int i = 1; i < args.length; i++) {
          try {
            Long val = Long.parseLong(args[i], 16);
            filterTxid.add(val);
          } catch (NumberFormatException nfe) {
            // Failed to parse, will exit instead of displaying everything since the intention was to potentially filter some data
            System.out.printf("Invalid transaction ID format: %s%n", args[i]);
            return 1;
          }
        }
      }

      // Parse TStatus filters for print display
      EnumSet<TStatus> filterStatus = null;
      if (cl.hasOption(statusOption.getOpt())) {
        filterStatus = EnumSet.noneOf(TStatus.class);
        String[] tstat = cl.getOptionValues(statusOption.getOpt());
        for (int i = 0; i < tstat.length; i++) {
          try {
            filterStatus.add(TStatus.valueOf(tstat[i]));
          } catch (IllegalArgumentException iae) {
            System.out.printf("Invalid transaction status name: %s%n", tstat[i]);
            return 1;
          }
        }
      }

      StringBuilder buf = new StringBuilder(8096);
      Formatter fmt = new Formatter(buf);
      admin.print(zs, zk, ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS, fmt, filterTxid, filterStatus);
      shellState.printLines(Collections.singletonList(buf.toString()).iterator(), !cl.hasOption(disablePaginationOpt.getOpt()));
    } else if ("dump".equals(cmd)) {
      List<Long> txids;

      if (args.length == 1) {
        txids = zs.list();
      } else {
        txids = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
          txids.add(Long.parseLong(args[i], 16));
        }
      }

      Gson gson = new GsonBuilder().registerTypeAdapter(ReadOnlyRepo.class, new InterfaceSerializer<>())
          .registerTypeAdapter(Repo.class, new InterfaceSerializer<>()).registerTypeAdapter(byte[].class, new ByteArraySerializer()).setPrettyPrinting()
          .create();

      List<FateStack> txStacks = new ArrayList<>();

      for (Long txid : txids) {
        List<ReadOnlyRepo<FateCommand>> repoStack = zs.getStack(txid);
        txStacks.add(new FateStack(txid, repoStack));
      }

      System.out.println(gson.toJson(txStacks));
    } else {
      throw new ParseException("Invalid command option");
    }

    return failedCommand ? 1 : 0;
  }

  protected synchronized IZooReaderWriter getZooReaderWriter(Instance instance, String secret) {

    if (secret == null) {
      AccumuloConfiguration conf = SiteConfiguration.getInstance();
      secret = conf.get(Property.INSTANCE_SECRET);
    }

    return new ZooReaderWriter(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), SCHEME, (USER + ":" + secret).getBytes());
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
        "filter 'print' on the transaction status type(s) {NEW, IN_PROGRESS, FAILED_IN_PROGRESS, FAILED, SUCCESSFUL}");
    statusOption.setArgs(Option.UNLIMITED_VALUES);
    statusOption.setOptionalArg(false);
    o.addOption(statusOption);
    disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
    o.addOption(disablePaginationOpt);
    return o;
  }

  @Override
  public int numArgs() {
    // Arg length varies between 1 to n
    return -1;
  }
}

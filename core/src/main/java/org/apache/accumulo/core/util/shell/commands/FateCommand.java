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
package org.apache.accumulo.core.util.shell.commands;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.KeeperException;

/**
 * Manage FATE transactions
 *
 */
public class FateCommand extends Command {

  private static final String SCHEME = "digest";

  private static final String USER = "accumulo";

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

    AdminUtil<FateCommand> admin = new AdminUtil<FateCommand>(false);

    String path = ZooUtil.getRoot(instance) + Constants.ZFATE;
    String masterPath = ZooUtil.getRoot(instance) + Constants.ZMASTER_LOCK;
    IZooReaderWriter zk = getZooReaderWriter(shellState.getInstance(), cl.getOptionValue(secretOption.getOpt()));
    ZooStore<FateCommand> zs = new ZooStore<FateCommand>(path, zk);

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
        filterTxid = new HashSet<Long>(args.length);
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
    } else {
      throw new ParseException("Invalid command option");
    }

    return failedCommand ? 1 : 0;
  }

  protected synchronized IZooReaderWriter getZooReaderWriter(Instance instance, String secret) {

    if (secret == null) {
      AccumuloConfiguration conf = SiteConfiguration.getInstance(DefaultConfiguration.getInstance());
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
    return getName() + " fail <txid>... | delete <txid>... | print [<txid>...]";
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

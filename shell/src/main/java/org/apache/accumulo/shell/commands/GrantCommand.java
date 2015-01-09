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

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class GrantCommand extends TableOperation {
  {
    disableUnflaggedTableOptions();
  }

  private Option systemOpt, userOpt;
  private String user;
  private String[] permission;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    user = cl.hasOption(userOpt.getOpt()) ? cl.getOptionValue(userOpt.getOpt()) : shellState.getConnector().whoami();

    permission = cl.getArgs()[0].split("\\.", 2);
    if (cl.hasOption(systemOpt.getOpt()) && permission[0].equalsIgnoreCase("System")) {
      try {
        shellState.getConnector().securityOperations().grantSystemPermission(user, SystemPermission.valueOf(permission[1]));
        Shell.log.debug("Granted " + user + " the " + permission[1] + " permission");
      } catch (IllegalArgumentException e) {
        throw new BadArgumentException("No such system permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
      }
    } else if (permission[0].equalsIgnoreCase("Table")) {
      super.execute(fullCommand, cl, shellState);
    } else if (permission[0].equalsIgnoreCase("Namespace")) {
      if (cl.hasOption(optNamespace.getOpt())) {
        try {
          shellState.getConnector().securityOperations()
              .grantNamespacePermission(user, cl.getOptionValue(optNamespace.getOpt()), NamespacePermission.valueOf(permission[1]));
        } catch (IllegalArgumentException e) {
          throw new BadArgumentException("No such namespace permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
        }
      } else {
        throw new BadArgumentException("No namespace specified to apply permission to", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
      }
    } else {
      throw new BadArgumentException("Unrecognized permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
    }
    return 0;
  }

  @Override
  protected void doTableOp(final Shell shellState, final String tableName) throws Exception {
    try {
      shellState.getConnector().securityOperations().grantTablePermission(user, tableName, TablePermission.valueOf(permission[1]));
      Shell.log.debug("Granted " + user + " the " + permission[1] + " permission on table " + tableName);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("No such table permission", e);
    }
  }

  @Override
  public String description() {
    return "grants system, table, or namespace permissions for a user";
  }

  @Override
  public String usage() {
    return getName() + " <permission>";
  }

  @Override
  public void registerCompletion(final Token root, final Map<Command.CompletionSet,Set<String>> completionSet) {
    final Token cmd = new Token(getName());
    cmd.addSubcommand(new Token(TablePermission.printableValues()));
    cmd.addSubcommand(new Token(SystemPermission.printableValues()));
    cmd.addSubcommand(new Token(NamespacePermission.printableValues()));
    root.addSubcommand(cmd);
  }

  @Override
  public Options getOptions() {
    super.getOptions();
    final Options o = new Options();

    final OptionGroup group = new OptionGroup();

    systemOpt = new Option("s", "system", false, "grant a system permission");

    optNamespace = new Option(ShellOptions.namespaceOption, "namespace", true, "name of a namespace to operate on");
    optNamespace.setArgName("namespace");

    group.addOption(systemOpt);
    group.addOption(optTableName);
    group.addOption(optTablePattern);
    group.addOption(optNamespace);

    o.addOptionGroup(group);
    userOpt = new Option(ShellOptions.userOption, "user", true, "user to operate on");
    userOpt.setArgName("username");
    userOpt.setRequired(true);
    o.addOption(userOpt);

    return o;
  }

  @Override
  public int numArgs() {
    return 1;
  }
}

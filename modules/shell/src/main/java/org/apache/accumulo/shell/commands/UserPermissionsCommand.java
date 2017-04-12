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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class UserPermissionsCommand extends Command {
  private Option userOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
    final String user = cl.getOptionValue(userOpt.getOpt(), shellState.getConnector().whoami());

    String delim = "";
    shellState.getReader().print("System permissions: ");
    for (SystemPermission p : SystemPermission.values()) {
      if (p != null && shellState.getConnector().securityOperations().hasSystemPermission(user, p)) {
        shellState.getReader().print(delim + "System." + p.name());
        delim = ", ";
      }
    }
    shellState.getReader().println();

    boolean runOnce = true;
    for (String n : shellState.getConnector().namespaceOperations().list()) {
      delim = "";
      for (NamespacePermission p : NamespacePermission.values()) {
        if (p != null && shellState.getConnector().securityOperations().hasNamespacePermission(user, n, p)) {
          if (runOnce) {
            shellState.getReader().print("\nNamespace permissions (" + n + "): ");
            runOnce = false;
          }
          shellState.getReader().print(delim + "Namespace." + p.name());
          delim = ", ";
        }
      }
      runOnce = true;
    }
    shellState.getReader().println();

    runOnce = true;
    for (String t : shellState.getConnector().tableOperations().list()) {
      delim = "";
      for (TablePermission p : TablePermission.values()) {
        if (shellState.getConnector().securityOperations().hasTablePermission(user, t, p) && p != null) {
          if (runOnce) {
            shellState.getReader().print("\nTable permissions (" + t + "): ");
            runOnce = false;
          }
          shellState.getReader().print(delim + "Table." + p.name());
          delim = ", ";
        }

      }
      runOnce = true;
    }
    shellState.getReader().println();

    return 0;
  }

  @Override
  public String description() {
    return "displays a user's system, table, and namespace permissions";
  }

  @Override
  public Options getOptions() {
    Options o = new Options();
    userOpt = new Option(ShellOptions.userOption, "user", true, "user to operate on");
    userOpt.setArgName("user");
    o.addOption(userOpt);
    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }
}

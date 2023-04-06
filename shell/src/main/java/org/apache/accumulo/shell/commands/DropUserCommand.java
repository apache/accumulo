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

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class DropUserCommand extends Command {
  private Option forceOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException {
    final String user = cl.getArgs()[0];
    if (shellState.getAccumuloClient().whoami().equals(user)) {
      throw new BadArgumentException("You cannot delete yourself", fullCommand,
          fullCommand.indexOf(user));
    }
    doDropUser(shellState, user, cl.hasOption(forceOpt.getOpt()));
    return 0;
  }

  private void doDropUser(final Shell shellState, final String user, final boolean force)
      throws AccumuloException, AccumuloSecurityException {
    boolean operate = true;

    if (!force) {
      shellState.getWriter().flush();
      String line = shellState.getReader().readLine(getName() + " { " + user + " } (yes|no)? ");
      operate = line != null && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"));
    }
    if (operate) {
      shellState.getAccumuloClient().securityOperations().dropLocalUser(user);
      Shell.log.debug("Deleted user {}", user);
    }

  }

  @Override
  public String description() {
    return "deletes a user";
  }

  @Override
  public String usage() {
    return getName() + " <username>";
  }

  @Override
  public void registerCompletion(final Token root,
      final Map<Command.CompletionSet,Set<String>> completionSet) {
    registerCompletionForUsers(root, completionSet);
  }

  @Override
  public int numArgs() {
    return 1;
  }

  @Override
  public Options getOptions() {
    forceOpt = new Option("f", "force", false, "force deletion without prompting");
    final Options opts = super.getOptions();
    opts.addOption(forceOpt);
    return opts;
  }
}

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class DeleteAuthsCommand extends Command {
  private Option userOpt;
  private Option scanOptAuths;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException {
    final AccumuloClient accumuloClient = shellState.getAccumuloClient();
    final String user = cl.getOptionValue(userOpt.getOpt(), accumuloClient.whoami());
    final String scanOpts = cl.getOptionValue(scanOptAuths.getOpt());

    final Authorizations auths = accumuloClient.securityOperations().getUserAuthorizations(user);
    final StringBuilder userAuths = new StringBuilder();
    final String[] toBeRemovedAuths = scanOpts.split(",");
    final Set<String> toBeRemovedSet = new HashSet<>();
    Collections.addAll(toBeRemovedSet, toBeRemovedAuths);
    final String[] existingAuths = auths.toString().split(",");
    for (String auth : existingAuths) {
      if (!toBeRemovedSet.contains(auth)) {
        userAuths.append(auth);
        userAuths.append(",");
      }
    }
    if (userAuths.length() > 0) {
      accumuloClient.securityOperations().changeUserAuthorizations(user,
          ScanCommand.parseAuthorizations(userAuths.substring(0, userAuths.length() - 1)));
    } else {
      accumuloClient.securityOperations().changeUserAuthorizations(user, new Authorizations());
    }

    Shell.log.debug("Changed record-level authorizations for user {}", user);
    return 0;
  }

  @Override
  public String description() {
    return "remove authorizations from the maximum scan authorizations for a user";
  }

  @Override
  public void registerCompletion(final Token root,
      final Map<Command.CompletionSet,Set<String>> completionSet) {
    registerCompletionForUsers(root, completionSet);
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    final OptionGroup setOrClear = new OptionGroup();
    scanOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations to remove");
    scanOptAuths.setArgName("comma-separated-authorizations");
    setOrClear.addOption(scanOptAuths);
    setOrClear.setRequired(true);
    o.addOptionGroup(setOrClear);
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

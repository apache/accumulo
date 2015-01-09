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

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class SetAuthsCommand extends Command {
  private Option userOpt;
  private Option scanOptAuths;
  private Option clearOptAuths;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException {
    final String user = cl.getOptionValue(userOpt.getOpt(), shellState.getConnector().whoami());
    final String scanOpts = cl.hasOption(clearOptAuths.getOpt()) ? null : cl.getOptionValue(scanOptAuths.getOpt());
    shellState.getConnector().securityOperations().changeUserAuthorizations(user, ScanCommand.parseAuthorizations(scanOpts));
    Shell.log.debug("Changed record-level authorizations for user " + user);
    return 0;
  }

  @Override
  public String description() {
    return "sets the maximum scan authorizations for a user";
  }

  @Override
  public void registerCompletion(final Token root, final Map<Command.CompletionSet,Set<String>> completionSet) {
    registerCompletionForUsers(root, completionSet);
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    final OptionGroup setOrClear = new OptionGroup();
    scanOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations to set");
    scanOptAuths.setArgName("comma-separated-authorizations");
    setOrClear.addOption(scanOptAuths);
    clearOptAuths = new Option("c", "clear-authorizations", false, "clear the scan authorizations");
    setOrClear.addOption(clearOptAuths);
    setOrClear.setRequired(true);
    o.addOptionGroup(setOrClear);
    userOpt = new Option(Shell.userOption, "user", true, "user to operate on");
    userOpt.setArgName("user");
    o.addOption(userOpt);
    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }
}

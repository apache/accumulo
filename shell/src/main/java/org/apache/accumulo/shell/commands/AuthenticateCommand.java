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
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;

public class AuthenticateCommand extends Command {
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
    final String user = cl.getArgs()[0];
    final String p = shellState.readMaskedLine("Enter current password for '" + user + "': ", '*');
    if (p == null) {
      shellState.getReader().println();
      return 0;
    } // user canceled
    final byte[] password = p.getBytes(UTF_8);
    final boolean valid = shellState.getConnector().securityOperations().authenticateUser(user, new PasswordToken(password));
    shellState.getReader().println((valid ? "V" : "Not v") + "alid");
    return 0;
  }

  @Override
  public String description() {
    return "verifies a user's credentials";
  }

  @Override
  public String usage() {
    return getName() + " <username>";
  }

  @Override
  public void registerCompletion(final Token root, final Map<Command.CompletionSet,Set<String>> completionSet) {
    registerCompletionForUsers(root, completionSet);
  }

  @Override
  public int numArgs() {
    return 1;
  }
}

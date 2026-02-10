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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class PasswdCommand extends Command {
  private Option userOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, IOException {
    final String currentUser = shellState.getAccumuloClient().whoami();
    final String user = cl.getOptionValue(userOpt.getOpt(), currentUser);

    String password = null;
    String passwordConfirm = null;
    String oldPassword = null;

    oldPassword =
        shellState.readMaskedLine("Enter current password for '" + currentUser + "': ", '*');
    if (oldPassword == null) {
      shellState.getWriter().println();
      return 0;
    } // user canceled

    if (!shellState.getAccumuloClient().securityOperations().authenticateUser(currentUser,
        new PasswordToken(oldPassword))) {
      throw new AccumuloSecurityException(user, SecurityErrorCode.BAD_CREDENTIALS);
    }

    password = shellState.readMaskedLine("Enter new password for '" + user + "': ", '*');
    if (password == null) {
      shellState.getWriter().println();
      return 0;
    } // user canceled
    passwordConfirm =
        shellState.readMaskedLine("Please confirm new password for '" + user + "': ", '*');
    if (passwordConfirm == null) {
      shellState.getWriter().println();
      return 0;
    } // user canceled

    if (!password.equals(passwordConfirm)) {
      throw new IllegalArgumentException("Passwords do not match");
    }
    byte[] pass = password.getBytes(UTF_8);
    shellState.getAccumuloClient().securityOperations().changeLocalUserPassword(user,
        new PasswordToken(pass));
    // update the current credentials if the password changed was for
    // the current user
    if (shellState.getAccumuloClient().whoami().equals(user)) {
      shellState.updateUser(user, new PasswordToken(pass));
    }
    Shell.log.debug("Changed password for user {}", user);
    return 0;
  }

  @Override
  public String description() {
    return "changes a user's password";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
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

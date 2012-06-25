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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CreateUserCommand extends Command {
  private Option scanOptAuths;
  
  static Authorizations parseAuthorizations(String field) {
    if (field == null || field.isEmpty())
      return Constants.NO_AUTHS;
    return new Authorizations(field.split(","));
  }
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, TableNotFoundException, AccumuloSecurityException,
      TableExistsException, IOException {
    String user = cl.getArgs()[0];
    String password = null;
    String passwordConfirm = null;
    
    password = shellState.readMaskedLine("Enter new password for '" + user + "': ", '*');
    if (password == null) {
      shellState.getReader().printNewline();
      return 0;
    } // user canceled
    passwordConfirm = shellState.readMaskedLine("Please confirm new password for '" + user + "': ", '*');
    if (passwordConfirm == null) {
      shellState.getReader().printNewline();
      return 0;
    } // user canceled
    
    if (!password.equals(passwordConfirm))
      throw new IllegalArgumentException("Passwords do not match");
    
    Authorizations authorizations = parseAuthorizations(cl.hasOption(scanOptAuths.getOpt()) ? cl.getOptionValue(scanOptAuths.getOpt()) : "");
    shellState.getConnector().securityOperations().createUser(user, password.getBytes(), authorizations);
    Shell.log.debug("Created user " + user + " with" + (authorizations.isEmpty() ? " no" : "") + " initial scan authorizations"
        + (!authorizations.isEmpty() ? " " + authorizations : ""));
    return 0;
  }
  
  @Override
  public String usage() {
    return getName() + " <username>";
  }
  
  @Override
  public String description() {
    return "creates a new user";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    scanOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations");
    scanOptAuths.setArgName("comma-separated-authorizations");
    o.addOption(scanOptAuths);
    return o;
  }
  
  @Override
  public int numArgs() {
    return 1;
  }
}

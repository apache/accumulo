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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class GetAuthsCommand extends Command {
  private Option userOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, IOException {
    final String user =
        cl.getOptionValue(userOpt.getOpt(), shellState.getAccumuloClient().whoami());
    // Sort authorizations
    Authorizations auths =
        shellState.getAccumuloClient().securityOperations().getUserAuthorizations(user);
    List<String> set = sortAuthorizations(auths);
    shellState.getWriter().println(String.join(",", set));
    return 0;
  }

  protected List<String> sortAuthorizations(Authorizations auths) {
    List<String> list = new ArrayList<>();
    for (byte[] auth : auths) {
      list.add(new String(auth));
    }
    list.sort(String.CASE_INSENSITIVE_ORDER);
    return list;
  }

  @Override
  public String description() {
    return "displays the maximum scan authorizations for a user";
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

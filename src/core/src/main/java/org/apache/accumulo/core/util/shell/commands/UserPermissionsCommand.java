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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class UserPermissionsCommand extends Command {
  private Option userOpt;
  private static int runOnce = 0;
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
    String user = cl.getOptionValue(userOpt.getOpt(), shellState.getConnector().whoami());
    
    String delim = "";
    shellState.getReader().printString("System permissions: ");
    for (SystemPermission p : SystemPermission.values()) {
      if (shellState.getConnector().securityOperations().hasSystemPermission(user, p) & p != null) {
        shellState.getReader().printString(delim + "System." + p.name());
        delim = ", ";
      }
    }
    shellState.getReader().printNewline();
    
    for (String t : shellState.getConnector().tableOperations().list()) {
      delim = "";
      for (TablePermission p : TablePermission.values()) {
        if (shellState.getConnector().securityOperations().hasTablePermission(user, t, p) && p != null) {
          if (runOnce == 0) {
            shellState.getReader().printString("\nTable permissions (" + t + "): ");
            runOnce++;
          }
          shellState.getReader().printString(delim + "Table." + p.name());
          delim = ", ";
        }
        
      }
      runOnce = 0;
    }
    shellState.getReader().printNewline();
    return 0;
  }
  
  @Override
  public String description() {
    return "displays a user's system and table permissions";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
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

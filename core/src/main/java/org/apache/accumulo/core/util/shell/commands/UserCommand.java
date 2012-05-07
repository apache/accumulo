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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;

public class UserCommand extends Command {
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
    // save old credentials and connection in case of failure
    String user = cl.getArgs()[0];
    byte[] pass;
    
    // We can't let the wrapping try around the execute method deal
    // with the exceptions because we have to do something if one
    // of these methods fails
    String p = shellState.getReader().readLine("Enter password for user " + user + ": ", '*');
    if (p == null) {
      shellState.getReader().printNewline();
      return 0;
    } // user canceled
    pass = p.getBytes();
    shellState.updateUser(new AuthInfo(user, ByteBuffer.wrap(pass), shellState.getConnector().getInstance().getInstanceID()));
    return 0;
  }
  
  @Override
  public String description() {
    return "switches to the specified user";
  }
  
  @Override
  public void registerCompletion(Token root, Map<Command.CompletionSet,Set<String>> special) {
    registerCompletionForUsers(root, special);
  }
  
  @Override
  public String usage() {
    return getName() + " <username>";
  }
  
  @Override
  public int numArgs() {
    return 1;
  }
}

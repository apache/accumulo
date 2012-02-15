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

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

public class MasterStateCommand extends Command {
  
  @Override
  public String description() {
    return "DEPRECATED: use the command line utility instead";
  }
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    shellState.getReader().printString("This command is no longer supported in the shell, please use \n");
    shellState.getReader().printString("  accumulo accumulo.server.master.state.SetGoalState [NORMAL|SAFE_MODE|CLEAN_STOP]\n");
    return -1;
  }
  
  @Override
  public String usage() {
    return "masterstate is deprecated, use the command line utility instead";
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
  
}

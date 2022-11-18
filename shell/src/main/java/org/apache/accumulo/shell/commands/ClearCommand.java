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

import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp.Capability;

public class ClearCommand extends Command {
  @Override
  public String description() {
    return "clears the screen";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException {

    if (Terminal.TYPE_DUMB.equalsIgnoreCase(shellState.getTerminal().getType())) {
      throw new IOException("Terminal does not support ANSI commands");
    }

    shellState.getTerminal().puts(Capability.clear_screen);
    shellState.getTerminal().puts(Capability.cursor_address, 0, 1);
    shellState.getTerminal().flush();

    return 0;
  }

  @Override
  public int numArgs() {
    return 0;
  }
}

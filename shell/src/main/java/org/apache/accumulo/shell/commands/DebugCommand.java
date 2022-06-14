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

import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

/**
 * @deprecated since 2.0; this command shouldn't be used; users should configure debug logging with
 *             their log configuration file instead
 */
@Deprecated(since = "2.0.0")
public class DebugCommand extends Command {
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) {
    shellState.printException(new IllegalArgumentException("The debug command is deprecated; "
        + "configure debug logging through your log configuration file instead."));
    return 1;
  }

  @Override
  public String description() {
    return "Deprecated since 2.0";
  }

  @Override
  public String usage() {
    return getName() + " [ on | off ] # this is now deprecated and does nothing";
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }
}

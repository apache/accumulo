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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

public class ImportDirectoryCommand extends Command {

  @Override
  public String description() {
    return "bulk imports an entire directory of data files to the current"
        + " table. The boolean argument determines if accumulo sets the time. "
        + "Passing 3 arguments will use the old bulk import.  The new bulk import only takes 2 "
        + "arguments: <directory> true|false";
  }

  @SuppressWarnings("deprecation")
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    shellState.checkTableState();

    String[] args = cl.getArgs();
    String dir = args[0];
    boolean setTime;

    // new bulk import only takes 2 args
    if (args.length == 2) {
      setTime = Boolean.parseBoolean(cl.getArgs()[1]);
      shellState.getAccumuloClient().tableOperations().importDirectory(dir)
          .to(shellState.getTableName()).tableTime(setTime).load();
    } else if (args.length == 3) {
      // warn using deprecated bulk import
      Shell.log.warn(
          "Deprecated since 2.0.0. New bulk import technique does not take a failure directory "
              + "as an argument.");
      String failureDir = args[1];
      setTime = Boolean.parseBoolean(cl.getArgs()[2]);
      shellState.getAccumuloClient().tableOperations().importDirectory(shellState.getTableName(),
          dir, failureDir, setTime);
      return 0;
    } else {
      shellState.printException(
          new IllegalArgumentException(String.format("Expected 2 or 3 arguments. There %s %d.",
              args.length == 1 ? "was" : "were", args.length)));
      printHelp(shellState);
    }

    return 0;
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }

  @Override
  public String usage() {
    return getName() + " <directory> [failureDirectory] true|false";
  }

}

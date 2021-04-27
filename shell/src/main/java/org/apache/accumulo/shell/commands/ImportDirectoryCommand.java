/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class ImportDirectoryCommand extends Command {

  @Override
  public String description() {
    return "bulk imports an entire directory of data files into an existing table."
        + " The table is either passed with the -t tablename opt, or into to the current"
        + " table if the -t option is not provided. The boolean argument determines if accumulo"
        + " sets the time. If the -i ignore option is supplied then no exception will be thrown"
        + " when attempting to import files from an empty source directory. An info log message"
        + " will be displayed indicating the source directory is empty, but no error is thrown.\n"
        + " Passing 3 arguments will use the old bulk import. The new bulk import only takes"
        + " 2 arguments:  <directory> true|false";
  }

  @SuppressWarnings("deprecation")
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {

    final String tableName = OptUtil.getTableOpt(cl, shellState);
    final boolean ignore = OptUtil.getIgnoreEmptyDirOpt(cl, shellState);

    String[] args = cl.getArgs();
    boolean setTime;
    String dir = args.length > 0 ? args[0] : "";
    int status = 0;

    switch (args.length) {
      case 2: {
        // new bulk import only takes 2 args
        setTime = Boolean.parseBoolean(cl.getArgs()[1]);
        shellState.getAccumuloClient().tableOperations().importDirectory(dir, ignore).to(tableName)
            .tableTime(setTime).load();
        break;
      }
      case 3: {
        // warn using deprecated bulk import
        Shell.log.warn(
            "Deprecated since 2.0.0. New bulk import technique does not take a failure directory "
                + "as an argument.");
        String failureDir = args[1];
        setTime = Boolean.parseBoolean(cl.getArgs()[2]);
        shellState.getAccumuloClient().tableOperations().importDirectory(tableName, dir, failureDir,
            setTime);
        break;
      }
      default: {
        shellState.printException(
            new IllegalArgumentException(String.format("Expected 2 or 3 arguments. There %s %d.",
                args.length == 1 ? "was" : "were", args.length)));
        printHelp(shellState);
        status = 1;
        break;
      }
    }

    return status;
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }

  @Override
  public String usage() {
    return getName() + " <directory> [failureDirectory] true|false";
  }

  @Override
  public Options getOptions() {
    final Options opts = super.getOptions();
    opts.addOption(OptUtil.tableOpt("name of the table to import files into"));
    opts.addOption(OptUtil.ignoreEmptyDirOpt("ignore empty bulk import source directory"));
    return opts;
  }

  @Override
  public void registerCompletion(final Token root,
      final Map<CompletionSet,Set<String>> completionSet) {
    registerCompletionForTables(root, completionSet);
  }

}

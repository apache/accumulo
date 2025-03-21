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
        + " Bulk import only takes 2 arguments:  <directory> true|false";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {

    final String tableName = OptUtil.getTableOpt(cl, shellState);
    final boolean ignore = OptUtil.getIgnoreEmptyDirOpt(cl, shellState);

    String[] args = cl.getArgs();
    String dir = args[0];
    boolean setTime = Boolean.parseBoolean(cl.getArgs()[1]);
    int status = 0;
    shellState.getAccumuloClient().tableOperations().importDirectory(dir).to(tableName)
        .tableTime(setTime).ignoreEmptyDir(ignore).load();
    return status;
  }

  @Override
  public int numArgs() {
    return 2;
  }

  @Override
  public String usage() {
    return getName() + " <directory> true|false";
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

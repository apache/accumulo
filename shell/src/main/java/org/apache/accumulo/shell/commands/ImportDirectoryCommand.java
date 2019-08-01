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
import org.apache.commons.cli.Options;

public class ImportDirectoryCommand extends Command {

  @Override
  public String description() {
    return "bulk imports an entire directory of data files into an existing table."
        + " The table is either passed with the -t tablename opt, or into to the current"
        + " table if the -t option is not provided. The boolean argument determines if accumulo sets the time.";
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {

    final String tableName = OptUtil.getTableOpt(cl, shellState);

    String dir = cl.getArgs()[0];
    String failureDir = cl.getArgs()[1];

    final boolean setTime = Boolean.parseBoolean(cl.getArgs()[2]);

    shellState.getConnector().tableOperations().importDirectory(tableName, dir, failureDir,
        setTime);

    return 0;
  }

  @Override
  public int numArgs() {
    // arg count for args not handled with Options
    return 3;
  }

  @Override
  public String usage() {
    return getName() + "[-t tablename] <directory> <failureDirectory> true|false";
  }

  @Override
  public Options getOptions() {
    final Options opts = super.getOptions();
    opts.addOption(OptUtil.tableOpt("name of the table to import files into"));
    return opts;
  }

}

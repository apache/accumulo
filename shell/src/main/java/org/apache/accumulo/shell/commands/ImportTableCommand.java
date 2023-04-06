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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ImportConfiguration;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ImportTableCommand extends Command {
  private Option keepMappingsOption;
  private Option keepOfflineOption;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException {

    Set<String> importDirs = Arrays.stream(cl.getArgs()).skip(1).collect(Collectors.toSet());
    String tableName = cl.getArgs()[0];
    boolean keepMappings = cl.hasOption(keepMappingsOption.getOpt());
    boolean keepOffline = cl.hasOption(keepOfflineOption.getOpt());

    var ic = ImportConfiguration.builder().setKeepMappings(keepMappings).setKeepOffline(keepOffline)
        .build();
    shellState.getAccumuloClient().tableOperations().importTable(tableName, importDirs, ic);
    return 0;
  }

  @Override
  public String usage() {
    return getName() + " <table name> <import dir>{ <import dir>}";
  }

  @Override
  public String description() {
    return "imports a table";
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }

  @Override
  public Options getOptions() {
    Options opts = new Options();
    keepMappingsOption =
        new Option("m", "mapping", false, "keep the mapping file after importing.");
    opts.addOption(keepMappingsOption);
    keepOfflineOption =
        new Option("o", "offline", false, "do not bring the table online after importing.");
    opts.addOption(keepOfflineOption);
    return opts;
  }
}

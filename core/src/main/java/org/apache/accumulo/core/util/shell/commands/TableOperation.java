/**
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

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public abstract class TableOperation extends Command {
  
  protected Option optTablePattern, optTableName;
  private boolean force = true;
  private boolean useCommandLine = true;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    // populate the tableSet set with the tables you want to operate on
    SortedSet<String> tableSet = new TreeSet<String>();
    if (cl.hasOption(optTablePattern.getOpt())) {
      for (String table : shellState.getConnector().tableOperations().list())
        if (table.matches(cl.getOptionValue(optTablePattern.getOpt())))
          tableSet.add(table);
    } else if (cl.hasOption(optTableName.getOpt())) {
      tableSet.add(cl.getOptionValue(optTableName.getOpt()));
    } else if (useCommandLine && cl.getArgs().length > 0) {
      for (String tableName : cl.getArgs())
        tableSet.add(tableName);
    } else {
      shellState.checkTableState();
      tableSet.add(shellState.getTableName());
    }
    
    if (tableSet.isEmpty())
      Shell.log.warn("No tables found that match your criteria");
    
    boolean more = true;
    // flush the tables
    for (String tableName : tableSet) {
      if (!more)
        break;
      if (!shellState.getConnector().tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      boolean operate = true;
      if (!force) {
        shellState.getReader().flushConsole();
        String line = shellState.getReader().readLine(getName() + " { " + tableName + " } (yes|no)? ");
        more = line != null;
        operate = line != null && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"));
      }
      if (operate)
        doTableOp(shellState, tableName);
    }
    
    return 0;
  }
  
  protected abstract void doTableOp(Shell shellState, String tableName) throws Exception;
  
  @Override
  public String description() {
    return "makes a best effort to flush tables from memory to disk";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    
    optTablePattern = new Option("p", "pattern", true, "regex pattern of table names to operate on");
    optTablePattern.setArgName("pattern");
    
    optTableName = new Option(Shell.tableOption, "table", true, "name of a table to operate on");
    optTableName.setArgName("tableName");
    
    OptionGroup opg = new OptionGroup();
    
    opg.addOption(optTablePattern);
    opg.addOption(optTableName);
    
    o.addOptionGroup(opg);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    if (useCommandLine)
      return Shell.NO_FIXED_ARG_LENGTH_CHECK;
    else
      return 0;
  }
  
  protected void force() {
    force = true;
  }
  
  protected void noForce() {
    force = false;
  }
  
  protected void disableUnflaggedTableOptions() {
    useCommandLine = false;
  }
  
  @Override
  public String usage() {
    return getName() + " [<table>{ <table>}]";
  }
  
  @Override
  public void registerCompletion(Token root, Map<Command.CompletionSet,Set<String>> special) {
    if (useCommandLine)
      registerCompletionForTables(root, special);
  }
}

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class SetGroupsCommand extends Command {
  
  private Option tableOpt;
  
  @Override
  public String description() {
    return "sets the locality groups for a given table (for binary or commas, use Java API)";
  }
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    
    String tableName;
    
    if (cl.hasOption(tableOpt.getOpt())) {
      tableName = cl.getOptionValue(tableOpt.getOpt());
      if (!shellState.getConnector().tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
    }
    
    else {
      shellState.checkTableState();
      tableName = shellState.getTableName();
    }
    
    HashMap<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
    
    for (String arg : cl.getArgs()) {
      String sa[] = arg.split("=", 2);
      if (sa.length < 2)
        throw new IllegalArgumentException("Missing '='");
      String group = sa[0];
      HashSet<Text> colFams = new HashSet<Text>();
      
      for (String family : sa[1].split(",")) {
        colFams.add(new Text(family));
      }
      
      groups.put(group, colFams);
    }
    
    shellState.getConnector().tableOperations().setLocalityGroups(tableName, groups);
    
    return 0;
  }
  
  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }
  
  @Override
  public String usage() {
    return getName() + " <group>=<col fam>{,<col fam>}{ <group>=<col fam>{,<col fam>}}";
  }
  
  @Override
  public Options getOptions() {
    Options opts = new Options();
    
    tableOpt = new Option(Shell.tableOption, "table", true, "get locality groups for specified table");
    tableOpt.setArgName("table");
    tableOpt.setRequired(false);
    opts.addOption(tableOpt);
    return opts;
  }
  
}
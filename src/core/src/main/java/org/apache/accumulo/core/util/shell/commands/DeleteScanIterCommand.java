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

import java.util.Map;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class DeleteScanIterCommand extends Command {
  private Option tableOpt, nameOpt, allOpt;
  
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
    
    if (cl.hasOption(allOpt.getOpt())) {
      Map<String,Map<String,String>> tableIterators = shellState.scanIteratorOptions.remove(tableName);
      if (tableIterators == null)
        Shell.log.info("No scan iterators set on table " + tableName);
      else
        Shell.log.info("Removed the following scan iterators from table " + tableName + ":" + tableIterators.keySet());
    } else if (cl.hasOption(nameOpt.getOpt())) {
      String name = cl.getOptionValue(nameOpt.getOpt());
      Map<String,Map<String,String>> tableIterators = shellState.scanIteratorOptions.get(tableName);
      if (tableIterators != null) {
        Map<String,String> options = tableIterators.remove(name);
        if (options == null)
          Shell.log.info("No iterator named " + name + " found for table " + tableName);
        else
          Shell.log.info("Removed scan iterator " + name + " from table " + tableName);
      } else {
        Shell.log.info("No iterator named " + name + " found for table " + tableName);
      }
    } else {
      throw new IllegalArgumentException("Must specify one of " + nameOpt.getArgName() + " or " + allOpt.getArgName());
    }
    
    return 0;
  }
  
  @Override
  public String description() {
    return "deletes a table-specific scan iterator so it is no longer used during this shell session";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    
    tableOpt = new Option(Shell.tableOption, "table", true, "tableName");
    tableOpt.setArgName("table");
    
    nameOpt = new Option("n", "name", true, "iterator to delete");
    nameOpt.setArgName("itername");
    
    allOpt = new Option("a", "all", false, "delete all for tableName");
    allOpt.setArgName("all");
    
    o.addOption(tableOpt);
    o.addOption(nameOpt);
    o.addOption(allOpt);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
}
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

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class TablesCommand extends Command {
  private Option tableIdOption;
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
    if (cl.hasOption(tableIdOption.getOpt())) {
      Map<String,String> tableIds = shellState.getConnector().tableOperations().tableIdMap();
      for (String tableName : shellState.getConnector().tableOperations().list())
        shellState.getReader().printString(String.format("%-15s => %10s\n", tableName, tableIds.get(tableName)));
    } else {
      for (String table : shellState.getConnector().tableOperations().list())
        shellState.getReader().printString(table + "\n");
    }
    return 0;
  }
  
  @Override
  public String description() {
    return "displays a list of all existing tables";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    tableIdOption = new Option("l", "list-ids", false, "display internal table ids along with the table name");
    o.addOption(tableIdOption);
    return o;
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
}

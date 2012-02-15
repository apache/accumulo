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

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class DeleteRowsCommand extends Command {
  private Option optStartRow, optEndRow, tableOpt, forceOpt;
  
  @Override
  public int execute(String fullCommand, CommandLine cl, final Shell shellState) throws Exception {
    String tableName;
    if (cl.hasOption(tableOpt.getOpt())) {
      tableName = cl.getOptionValue(tableOpt.getOpt());
      if (!shellState.getConnector().tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
    } else {
      shellState.checkTableState();
      tableName = shellState.getTableName();
    }
    Text startRow = null;
    if (cl.hasOption(optStartRow.getOpt()))
      startRow = new Text(cl.getOptionValue(optStartRow.getOpt()));
    Text endRow = null;
    if (cl.hasOption(optEndRow.getOpt()))
      endRow = new Text(cl.getOptionValue(optEndRow.getOpt()));
    if (!cl.hasOption(forceOpt.getOpt()) && (startRow == null || endRow == null)) {
      shellState.getReader().printString("Not deleting unbounded range. Specify both ends, or use --force\n");
      return 1;
    }
    shellState.getConnector().tableOperations().deleteRows(tableName, startRow, endRow);
    return 0;
  }
  
  @Override
  public String description() {
    return "delete a range of rows in a table.  Note that rows matching the start row ARE NOT deleted, but rows matching the end row ARE deleted.";
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    tableOpt = new Option(Shell.tableOption, "tableName", true, "table to delete row range");
    tableOpt.setArgName("table");
    tableOpt.setRequired(false);
    optStartRow = new Option("b", "begin-row", true, "begin row");
    optEndRow = new Option("e", "end-row", true, "end row");
    forceOpt = new Option("f", "force", false, "delete data even if start or end are not specified");
    o.addOption(optStartRow);
    o.addOption(optEndRow);
    o.addOption(tableOpt);
    o.addOption(forceOpt);
    return o;
  }
}
